"""Decode one qlog segment into InfluxDB points."""
import base64
import json
import re

import zstandard
import capnp
from influxdb_client import Point, WritePrecision

from .config import ROOT
from .engage import UNKNOWN_CAUSE, EngagementTracker, alert_cause
from .faults import FaultTracker

capnp.remove_import_hook()
_log = capnp.load(str(ROOT / "schema" / "log.capnp"), imports=[str(ROOT / "schema")])

MPH = 2.23694
MILE = 1609.344

# mode -> numeric code, used for colouring the map and state timeline
MODES = {"manual": 0, "acc_only": 1, "mads": 2, "steer_stock_acc": 3, "full": 4}

# openpilot alerts that mean something is wrong with the system: its soft/immediate-disable events
# (openpilot/selfdrive/selfdrived/events.py), minus the ones the driver or situation causes, plus steering warnings
OP_ALERTS = {
  "bigModelFailed", "cameraMalfunction", "cameraFrameRate", "locationdTemporaryError", "locationdPermanentError",
  "paramsdTemporaryError", "paramsdPermanentError", "steerTempUnavailable", "sensorDataInvalid", "excessiveActuation",
  "overheat", "calibrationInvalid", "commIssue", "commIssueAvgFreq", "selfdrivedLagging", "processNotRunning",
  "radarFault", "radarTempUnavailable", "modeldLagging", "posenetInvalid", "lowMemory", "accFaulted",
  "controlsMismatch", "canError", "canBusMissing", "steerUnavailable", "relayMalfunction", "vehicleSensorsInvalid",
  "steerSaturated", "steerTempUnavailableSilent",
}
# which driving model is running (sunnypilot forks with a Chestnut accelerator run a "big" model on it, falling back to
# the device's own "small" model): big while the Chestnut reports a live temperature, loading while it starts up
MODEL_CODES = {"small": 0, "loading": 1, "big": 2}
CHESTNUT_STALE_NS = 3e9  # chestnutState is 1 Hz in qlogs

# visibility: lane-line confidence only means something while moving
VISION_MOVING = 5.0  # m/s
LOW_LANE_PROB = 0.5

# steering tracking / following: only while moving
TRACK_MOVING = 5.0  # m/s

# position in lane: below this the model has no line at all, so there is nothing to place the car against
NO_LINE_PROB = 0.1
LANE_WIDTH = (2.0, 5.0)  # m: a plausible lane width; anything else is a misread
LANE_WIDTH_START = 3.0   # m, until the segment's first clear sight of both lines


class LanePosition:
  """Where the car sits in its lane, from drivingModelData's lane lines (sideways distance from the car, right = +).

  Every sample is kept; `quality` says how far to trust it: 2 = both lines clear, 1 = one clear (the other placed
  the lane's last measured width away), 0 = neither clear (their best guess). Raw line positions and confidences are
  stored as well, so the rules can change at query time."""

  def __init__(self):
    self.width = LANE_WIDTH_START

  def fields(self, dm) -> dict:
    ll = dm.laneLineMeta
    f = {"left_y": round(ll.leftY, 3), "right_y": round(ll.rightY, 3), "left_prob": round(ll.leftProb, 3),
         "right_prob": round(ll.rightProb, 3), "lane_change": int(str(dm.meta.laneChangeState) != "off")}
    left, right = ll.leftProb > LOW_LANE_PROB, ll.rightProb > LOW_LANE_PROB
    if left and right:
      if LANE_WIDTH[0] < ll.rightY - ll.leftY < LANE_WIDTH[1]:
        self.width = ll.rightY - ll.leftY
      centre, quality = (ll.leftY + ll.rightY) / 2, 2
    elif left:
      centre, quality = ll.leftY + self.width / 2, 1
    elif right:
      centre, quality = ll.rightY - self.width / 2, 1
    elif max(ll.leftProb, ll.rightProb) >= NO_LINE_PROB:
      centre, quality = (ll.leftY + ll.rightY) / 2, 0
    else:
      return f
    f.update(offset=round(-centre, 3), width=round(self.width, 3), quality=quality)  # offset: + = right of centre
    return f

# driver monitoring: the driver's state as one code per moment, worse = higher (the timeline row shows each window's worst)
# Warnings: "distracted" ones (the camera policy) say Pay Attention, "unresponsive" ones (the wheel-touch policy) say Touch
# Steering Wheel; at the same level the two are equally bad
ATTENTION = {"attentive": 0, "wheel touch": 1, "no face": 2, "distracted": 3,
             "distracted 1": 4, "unresponsive 1": 5, "distracted 2": 6, "unresponsive 2": 7, "distracted 3": 8, "unresponsive 3": 9}
DM_LEVELS = {"none": 0, "one": 1, "two": 2, "three": 3}
FORWARD_GEARS = ("drive", "low", "sport", "manumatic", "brake", "eco")
# which lateral controller openpilot is steering with (controlsState), 0 while it isn't steering
LAT_CONTROLLERS = {"torqueState": 1, "angleState": 2, "pidState": 3, "curvatureState": 4, "debugState": 5}
# Adventure Pilot's Rivian angle harness (carParams flag) steers by angle inside the car port, so openpilot's controller
# still says torque: there, steering with zero applied torque (carOutput) is angle. Torque crosses zero now and then, so
# it takes this many zero samples in a row (qlogs log carOutput at 10 Hz)
RIVIAN_ANGLE_HARNESS = 2
ANGLE_ZERO_RUN = 3

# a boot's first segment: the car is often rolling before openpilot is up, and every process reports errors meanwhile
STARTUP_S = 45


def classify(lat: bool, lon: bool, acc: bool, mads_active: bool, op_enabled: bool, op_long: bool) -> str:
  if lat and (lon or (acc and op_long)):
    return "full"             # op steering + op gas/brake (incl. momentary gas override)
  if lat and acc:
    return "steer_stock_acc"  # op steering + the car's own ACC
  if lat and mads_active and not op_enabled:
    return "mads"             # steering only, driver on the pedals
  if lat:
    return "mads"
  if acc:
    return "acc_only"
  return "manual"


def read_events(path):
  data = zstandard.ZstdDecompressor().stream_reader(open(path, "rb")).read()
  return _log.Event.read_multiple_bytes(data)


def extract_segment(path, tags: dict, fallback_start_ns: int | None) -> tuple[list[Point], dict, dict]:
  msgs = list(read_events(path))
  if not msgs:
    return [], {}, {}
  info = software_info(msgs)
  # every segment starts with the boot's initData, stamped when logging started: the segment itself starts after it
  seg_t0 = next((m.logMonoTime for m in msgs if m.which() != "initData"), msgs[0].logMonoTime)

  # mono -> wall clock: prefer a GPS fix in this segment, else the API's segment start time
  offset = None
  for m in msgs:
    if m.which() == "gpsLocationExternal":
      g = m.gpsLocationExternal
      if g.hasFix and g.unixTimestampMillis > 0:
        offset = g.unixTimestampMillis * 1_000_000 - m.logMonoTime
        break
  if offset is None:
    if fallback_start_ns is None:
      return [], {}, info
    offset = fallback_start_ns - seg_t0

  points: list[Point] = []
  vision_sum = {"lane_sum": 0.0, "lane_n": 0, "low_n": 0}
  lane_pos = LanePosition()
  steer_sum = {"err_sq_sum": 0.0, "n": 0, "sat_n": 0}
  lead_sum = {"gap_sum": 0.0, "n": 0}
  # driver monitoring while openpilot steered and the car moved: seconds, seconds distracted, warnings (0 -> any level)
  dm_sum = {"steered_s": 0.0, "distracted_s": 0.0, "warnings": 0, "max_level": 0}
  dm_prev_t = dm_prev_level = None
  long_active = False
  disables: list[tuple[int, str, str]] = []  # recent openpilot events that disengage: (t, name, soft|immediate|user)
  cur = dict(rot_std=None, exposure=None, op_enabled=False, mads_active=False, mads_state="disabled", acc=False, v=0.0, angle=0.0,
             torque=0.0, eps=0.0, lat_ctrl=0, in_gear=False, pressed=False, gas=False, brake=False, lat=None, lon=None, gps_t=0)
  cp = next((m.carParams for m in msgs if m.which() == "carParams"), None)
  op_long = cp.openpilotLongitudinalControl if cp else False
  angle_harness = bool(cp and cp.brand == "rivian" and cp.flags & RIVIAN_ANGLE_HARNESS)
  zero_torque_run = 0
  # a segment's carControls start before its first controlsState: start from what that one (and, on the angle harness,
  # the first carOutputs) says, or every segment would open with a blip of manual, then torque, then angle
  first_cs = next((m.controlsState for m in msgs if m.which() == "controlsState"), None)
  if first_cs is not None:
    lcs = first_cs.lateralControlState
    kind = lcs.which()
    cur["lat_ctrl"] = LAT_CONTROLLERS[kind] if kind in LAT_CONTROLLERS and getattr(lcs, kind).active else 0
  if angle_harness and cur["lat_ctrl"] == LAT_CONTROLLERS["torqueState"]:
    outs = [m.carOutput.actuatorsOutput.torque for m in msgs if m.which() == "carOutput"][:ANGLE_ZERO_RUN]
    if len(outs) == ANGLE_ZERO_RUN and not any(outs):
      zero_torque_run = ANGLE_ZERO_RUN
      cur["lat_ctrl"] = LAT_CONTROLLERS["angleState"]
  if info and cp:
    info["brand"] = cp.brand  # decides whether the rlogs' CAN can be decoded (Rivian only)
  prev_t = None
  last_mode = "manual"
  run_start = None
  prev_events: dict[str, set[str]] = {"onroadEvents": set(), "onroadEventsSP": set()}
  summary: dict[str, list[float]] = {}  # mode -> [seconds, metres]
  faults = FaultTracker(tags, offset)
  lat_active = next((m.carControl.latActive for m in msgs if m.which() == "carControl"), False)  # as lat_ctrl above
  prev_tx_blocked = None
  driving = False
  # last chestnutState with a live Chestnut. Segments are decoded separately, so if the first report comes within the
  # stale window, the Chestnut was already running when this segment started
  chestnut_t = next((m.logMonoTime for m in msgs if m.which() == "chestnutState" and m.chestnutState.tempC > 0), None)
  if chestnut_t is not None and chestnut_t - seg_t0 < CHESTNUT_STALE_NS:
    chestnut_t = seg_t0
  else:
    chestnut_t = None
  big_loading = False
  loading_until = 0  # the Chestnut's first report comes a moment after the loading alert clears
  model_secs = {"small": 0.0, "big": 0.0}
  # big-model loads: [loading alert start (None if it began in an earlier segment), end]; and when bigModelFailed fired
  loads: list[list] = []
  load_start = None
  seen_events = False
  failed_t = []
  # startup, for a segment that begins a drive: first openpilot state, first driving-model output, first live Chestnut
  first_op = first_model = first_chestnut = None
  first_engageable = None  # first moment openpilot could be engaged (any segment: it can wait on the car)
  startup_end = msgs[0].logMonoTime + STARTUP_S * 1e9 if info else 0  # initData's time = when this boot started logging
  engagements = EngagementTracker(lambda meas, t: pt(meas, t), faults, "qlog")

  def qlog_cause(t: int) -> str:
    return alert_cause(disables, t) or ("brake" if cur["brake"] else UNKNOWN_CAUSE)

  def pt(measurement: str, t_mono: int) -> Point:
    p = Point(measurement).time(t_mono + offset, WritePrecision.NS)
    for k, v in tags.items():
      p = p.tag(k, v)
    return p

  def interval(start: int, end: int, mode: str) -> Point:
    """One stretch of a single mode (split at segment boundaries)."""
    return pt("mode_interval", start).tag("mode", mode).field("end_ns", end + offset).field("code", MODES[mode])

  for m in msgs:
    w = m.which()
    t = m.logMonoTime
    if w == "selfdriveState":
      first_op = first_op or t
      if m.selfdriveState.engageable:
        first_engageable = first_engageable or t
      cur["op_enabled"] = m.selfdriveState.enabled
      engagements.update(t, m.selfdriveState.enabled, cur["v"], lambda: qlog_cause(t))
    elif w == "controlsState":
      lcs = m.controlsState.lateralControlState
      kind = lcs.which()
      cur["lat_ctrl"] = LAT_CONTROLLERS[kind] if kind in LAT_CONTROLLERS and getattr(lcs, kind).active else 0
      if cur["lat_ctrl"] == LAT_CONTROLLERS["torqueState"] and zero_torque_run >= ANGLE_ZERO_RUN:
        cur["lat_ctrl"] = LAT_CONTROLLERS["angleState"]
      ts = lcs.torqueState if kind == "torqueState" else None
      if ts is not None and ts.active and cur["v"] > TRACK_MOVING:
        points.append(pt("steering", t).field("desired_lat_accel", round(ts.desiredLateralAccel, 3))
                      .field("actual_lat_accel", round(ts.actualLateralAccel, 3)).field("saturated", int(ts.saturated)))
        steer_sum["err_sq_sum"] += (ts.desiredLateralAccel - ts.actualLateralAccel) ** 2
        steer_sum["n"] += 1
        steer_sum["sat_n"] += ts.saturated
    elif w == "carOutput" and angle_harness:
      zero_torque_run = zero_torque_run + 1 if lat_active and m.carOutput.actuatorsOutput.torque == 0 else 0
    elif w == "radarState":
      lead = m.radarState.leadOne
      if lead.present and cur["v"] > TRACK_MOVING:
        gap = lead.dRel / cur["v"]
        points.append(pt("lead", t).field("distance_m", round(lead.dRel, 1)).field("rel_speed", round(lead.vRel, 2))
                      .field("gap_s", round(gap, 2)).field("following", int(long_active)))
        if long_active:
          lead_sum["gap_sum"] += gap
          lead_sum["n"] += 1
    elif w == "selfdriveStateSP":
      mads = m.selfdriveStateSP.mads
      cur["mads_active"] = mads.active
      cur["mads_state"] = str(mads.state)
    elif w == "carState":
      cs = m.carState
      cur.update(acc=cs.cruiseState.enabled, v=cs.vEgo, angle=cs.steeringAngleDeg, torque=cs.steeringTorque,
                 eps=cs.steeringTorqueEps, pressed=cs.steeringPressed, gas=cs.gasPressed, brake=cs.brakePressed)
      # park/reverse and startup produce harmless faults; only count them while driving forward
      gear = str(cs.gearShifter)
      driving = gear in FORWARD_GEARS
      cur["in_gear"] = driving or gear == "reverse"  # vs park / neutral, where the driver camera's verdict means little
      faults.update("op_steer_fault", t, driving and (cs.steerFaultTemporary or cs.steerFaultPermanent), mode=last_mode)
      faults.update("can_error", t, driving and not cs.canValid, mode=last_mode)
    elif w == "pandaStates" and len(m.pandaStates):
      # the panda drops commands that break its safety rules; blocked while openpilot is steering = steering lost.
      # (blocks while parked/disengaged are normal: startup, shutdown, gear changes)
      tx = m.pandaStates[0].safetyTxBlocked
      new = tx - prev_tx_blocked if prev_tx_blocked is not None and tx >= prev_tx_blocked else 0
      prev_tx_blocked = tx
      faults.update("steering_blocked", t, new > 0 and lat_active, frames=new, mode=last_mode)
    elif w == "drivingModelData":
      dm = m.drivingModelData
      dm_big = dm.big  # set by openpilot's modeld; sunnypilot's modeld_v2 leaves it False
      first_model = first_model or t
      if dm_big:
        chestnut_t = t
        first_chestnut = first_chestnut or t
      lane = (dm.laneLineMeta.leftProb + dm.laneLineMeta.rightProb) / 2
      p = (pt("vision", t).field("lane_left", round(dm.laneLineMeta.leftProb, 3))
           .field("lane_right", round(dm.laneLineMeta.rightProb, 3))
           .field("frame_drop_pct", round(dm.frameDropPerc, 2))
           # what the model wants to do with speed (the device tints its path with it in experimental mode)
           .field("model_accel", round(dm.action.desiredAcceleration, 3)))
      if cur["rot_std"] is not None:
        p = p.field("motion_std", round(cur["rot_std"], 4))
      if cur["exposure"] is not None:
        p = p.field("exposure_pct", round(cur["exposure"], 2))
      if cur["v"] > VISION_MOVING:  # lane lines hide behind traffic at stops, so only while moving
        p = p.field("lane_moving", round(lane, 3))
      points.append(p)
      lp = pt("lane", t).tag("source", "qlog")
      for k, v in lane_pos.fields(dm).items():
        lp = lp.field(k, v)
      points.append(lp)
      if cur["v"] > VISION_MOVING:
        vision_sum["lane_sum"] += lane
        vision_sum["lane_n"] += 1
        vision_sum["low_n"] += lane < LOW_LANE_PROB
    elif w == "driverMonitoringState":
      d = m.driverMonitoringState
      vis = d.visionPolicyState
      wheel = str(d.activePolicy) == "wheeltouch"  # the camera can't judge (e.g. not calibrated yet): hands on the wheel instead
      level = DM_LEVELS.get(str(d.alertLevel), 0)
      distracted = vis.isDistracted and not wheel
      state = (f"{'unresponsive' if wheel else 'distracted'} {level}" if level else "wheel touch" if wheel else "no face" if not vis.faceDetected
               else "distracted" if distracted else "attentive")
      awareness = d.wheeltouchPolicyState.awarenessPercent if wheel else vis.awarenessPercent
      points.append(pt("driver_monitor", t).field("awareness_pct", awareness).field("alert_level", level)
                    .field("distracted", int(distracted)).field("face", int(vis.faceDetected)).field("wheel_touch", int(wheel))
                    .field("in_gear", int(cur["in_gear"])).field("code", ATTENTION[state]))
      if dm_prev_t is not None and lat_active and cur["v"] > TRACK_MOVING:
        dt = min((t - dm_prev_t) / 1e9, 1.0)
        dm_sum["steered_s"] += dt
        dm_sum["distracted_s"] += dt * distracted
      if dm_prev_level == 0 and level > 0:
        dm_sum["warnings"] += 1
      dm_sum["max_level"] = max(dm_sum["max_level"], level)
      dm_prev_t, dm_prev_level = t, level
    elif w == "cameraOdometry":
      cur["rot_std"] = sum(x * x for x in m.cameraOdometry.rotStd) ** 0.5  # vision-motion uncertainty
    elif w == "narrowRoadCameraState":
      cur["exposure"] = m.narrowRoadCameraState.exposureValPercent
    elif w == "thumbnail":
      # ~3.5 KB road-camera JPEG once a minute; base64 so a Grafana table can show it
      points.append(pt("thumbnail", t).field("jpeg", base64.b64encode(bytes(m.thumbnail.thumbnail)).decode()))
    elif w == "chestnutState":
      if m.chestnutState.tempC > 0:
        chestnut_t = t
        first_chestnut = first_chestnut or t
    elif w == "gpsLocationExternal":
      g = m.gpsLocationExternal
      if g.hasFix:
        cur.update(lat=g.latitude, lon=g.longitude, gps_t=t)
    elif w in ("onroadEvents", "onroadEventsSP"):
      evs = m.onroadEvents if w == "onroadEvents" else m.onroadEventsSP.events
      names = {str(e.name) for e in evs}
      disables[:] = [d for d in disables if t - d[0] < 2e9]
      for e in evs:
        if e.softDisable or e.immediateDisable or e.userDisable:
          disables.append((t, str(e.name), "soft" if e.softDisable else "immediate" if e.immediateDisable else "user"))
      for name in names - prev_events[w]:
        # tag whether openpilot was engaged, so the dashboard can grey out events during manual driving
        points.append(pt("events", t).tag("event", name).tag("engaged", str(int(last_mode != "manual")))
                      .field("count", 1).field("mode", last_mode))
      prev_events[w] = names
      if w == "onroadEvents":
        loading_now = "bigModelLoading" in names
        if loading_now and not big_loading:
          load_start = t if seen_events else None
        if big_loading and not loading_now:
          loading_until = t + CHESTNUT_STALE_NS
          loads.append([load_start, t])
        if "bigModelFailed" in names:
          failed_t.append(t)
        big_loading = loading_now
        seen_events = True
      if driving and t > startup_end:
        for name in names & OP_ALERTS:
          faults.update("op_alert", t, True, key=name, mode=last_mode)
    elif w == "carControl":
      cc = m.carControl
      lat_active = cc.latActive
      long_active = cc.longActive
      mode =classify(cc.latActive, cc.longActive, cur["acc"], cur["mads_active"], cur["op_enabled"], op_long)
      if mode != last_mode or run_start is None:
        if run_start is not None:
          points.append(interval(run_start, t, last_mode))
        run_start = t
      last_mode = mode
      dt = min((t - prev_t) / 1e9, 1.0) if prev_t is not None else 0.0
      prev_t = t
      model = ("big" if chestnut_t is not None and t - chestnut_t < CHESTNUT_STALE_NS else
               "loading" if big_loading or t < loading_until else "small")
      if model != "loading":
        model_secs[model] += dt
      s = summary.setdefault(mode, [0.0, 0.0])
      s[0] += dt
      s[1] += cur["v"] * dt
      p = (pt("drive", t)
           .field("mode", mode).field("mode_code", MODES[mode])
           .field("speed_mph", round(cur["v"] * MPH, 2))
           .field("steer_angle", round(cur["angle"], 2))
           .field("driver_torque", round(cur["torque"], 3))
           .field("eps_torque", round(cur["eps"], 3))
           .field("op_torque", round(cc.actuators.torque, 4))
           .field("op_accel", round(cc.actuators.accel, 3))
           .field("model_code", MODEL_CODES[model])
           .field("lat_active", int(cc.latActive)).field("long_active", int(cc.longActive))
           .field("lat_ctrl", cur["lat_ctrl"])
           .field("acc_on", int(cur["acc"])).field("op_enabled", int(cur["op_enabled"]))
           .field("mads_state", cur["mads_state"])
           .field("steering_pressed", int(cur["pressed"]))
           .field("gas_pressed", int(cur["gas"])).field("brake_pressed", int(cur["brake"])))
      if cur["lat"] is not None and t - cur["gps_t"] < 3e9:
        p = p.field("lat", cur["lat"]).field("lon", cur["lon"])
      points.append(p)

  if run_start is not None:
    points.append(interval(run_start, prev_t, last_mode))
  points += faults.finish(prev_t)

  seg_start = seg_t0
  for mode, (secs, metres) in summary.items():
    points.append(pt("mode_summary", seg_start).tag("mode", mode)
                  .field("seconds", round(secs, 2)).field("miles", round(metres / MILE, 4)))
  # one point per big-model load, at the moment it finished (or gave up)
  boot_t0 = msgs[0].logMonoTime if msgs[0].which() == "initData" else None
  for start, end in loads:
    failed = any(end - 1e9 <= f <= end + 5e9 for f in failed_t)
    p = (pt("model_load", end).tag("model", info.get("model_big") or "big model")
         .field("ok", int(not failed)))
    if start is not None:
      p = p.field("load_s", round((end - start) / 1e9, 2))
    if boot_t0 is not None:
      p = p.field("boot_to_ready_s", round((end - boot_t0) / 1e9, 2))
    points.append(p)
  # time to ready, from the drive's log starting (car on): openpilot running, then the first driving-model output.
  # Nothing drives while the big model loads, so that output is from whichever model came up: the Chestnut's if it
  # reported within a moment of it, else the device's own (after a failed big-model load, too)
  # Only the drive's first segment: every segment repeats the initData, but only the first starts right after it
  if boot_t0 is not None and first_op is not None and seg_t0 - boot_t0 < 30e9:
    p = pt("startup", boot_t0).field("openpilot_s", round((first_op - boot_t0) / 1e9, 2))
    if first_model is not None:
      big = first_chestnut is not None and abs(first_chestnut - first_model) < CHESTNUT_STALE_NS
      p = (p.field("model_s", round((first_model - boot_t0) / 1e9, 2)).field("model", "big" if big else "device")
           .field("model_name", info.get("model_big" if big else "model_small") or "")
           .field("big_failed", int(any(f < first_model for f in failed_t))))
    points.append(p)
  # the first engageable moment in this segment, from the drive's start; the dashboard takes the drive's earliest
  if boot_t0 is not None and first_engageable is not None:
    points.append(pt("engage_ready", first_engageable).field("since_start_s", round((first_engageable - boot_t0) / 1e9, 2)))
  points += engagements.points
  if steer_sum["n"]:
    points.append(pt("steering_summary", seg_start).field("err_sq_sum", round(steer_sum["err_sq_sum"], 4))
                  .field("n", steer_sum["n"]).field("sat_n", steer_sum["sat_n"]))
  if lead_sum["n"]:
    points.append(pt("lead_summary", seg_start).field("gap_sum", round(lead_sum["gap_sum"], 2)).field("n", lead_sum["n"]))
  if vision_sum["lane_n"]:
    points.append(pt("vision_summary", seg_start).field("lane_sum", round(vision_sum["lane_sum"], 3))
                  .field("lane_n", vision_sum["lane_n"]).field("low_n", vision_sum["low_n"]))
  if dm_prev_t is not None:
    points.append(pt("dm_summary", seg_start).field("steered_s", round(dm_sum["steered_s"], 2))
                  .field("distracted_s", round(dm_sum["distracted_s"], 2)).field("warnings", dm_sum["warnings"])
                  .field("max_level", dm_sum["max_level"]))
  if any(model_secs.values()):
    points.append(pt("model_summary", seg_start).field("small_s", round(model_secs["small"], 2))
                  .field("big_s", round(model_secs["big"], 2)))
  return points, summary, info


def software_info(msgs) -> dict:
  """openpilot / AGNOS versions and selected models from the segment's initData (each segment repeats the boot's)."""
  for m in msgs:
    if m.which() == "initData":
      i = m.initData
      return {"version": i.version, "remote": i.gitRemote, "os": i.osVersion.strip(), "commit": i.gitCommit[:7], "branch": i.gitBranch,
              "commit_date": i.gitCommitDate.strip("'").split(" ")[1] if i.gitCommitDate.count(" ") >= 1 else "",
              "device": str(i.deviceType), **model_names(i)}
  return {}


def model_names(init) -> dict:
  """Selected driving models from the params snapshot in initData (sunnypilot's model manager; blank elsewhere)."""
  params = {e.key: bytes(e.value) for e in init.params.entries}
  out = {}
  for key, param in (("model_small", "ModelManager_ActiveBundle"), ("model_big", "ModelManager_ActiveBundleChestnut")):
    try:
      name = json.loads(params[param])["displayName"]
      out[key] = re.sub(r"\s*\([^)]*\d{4}\)$", "", name)  # "WMI V12 (January 13, 2026)" -> "WMI V12"
    except (KeyError, ValueError, TypeError):
      out[key] = ""
  return out
