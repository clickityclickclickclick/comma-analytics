"""Collision alerts: openpilot's forward collision warning ("BRAKE! Risk of Collision"), and the car's own FCW / AEB.

Each alert is found in the qlog (every drive has one), so it keeps one time whichever log is decoded later. The
seconds around it come from the rlog when the drive was copied off the device (100 Hz), else from the qlog (radar
about 4 Hz, the rest 10 Hz), sampled every 0.1 s so the two write the same points:

  collision_alert   one point at the alert: what warned, speed, the car ahead, time to impact, your reaction
  collision_detail  every 0.1 s from BEFORE_S before to AFTER_S after it, with t_rel = seconds from the alert

A window can run into the segments either side (logMonoTime carries on across a drive's segments).
"""
from influxdb_client import Point, WritePrecision

from .extract import MPH, read_events

# onroadEvents name / carState flag -> what warned; the strongest one in an alert names it
KINDS = {"stockAeb": "car AEB", "aeb": "openpilot AEB", "stockFcw": "car FCW", "fcw": "openpilot FCW"}
RANK = list(KINDS)  # strongest first
BEFORE_S = 6.0
AFTER_S = 8.0
STEP_NS = 100_000_000
NEW_ALERT_NS = 5e9  # triggers this close together are one alert


def _triggers(m) -> list[str]:
  w = m.which()
  if w == "onroadEvents":
    return [str(e.name) for e in m.onroadEvents if str(e.name) in KINDS]
  if w == "carState":
    return [k for k, on in (("stockAeb", m.carState.stockAeb), ("stockFcw", m.carState.stockFcw)) if on]
  return []


def find_alerts(msgs) -> list[tuple[int, str]]:
  """(logMonoTime of the alert's start, strongest kind) for each alert in these messages."""
  alerts: list[list] = []
  last = None
  for m in msgs:
    trig = _triggers(m)
    if not trig:
      continue
    t = m.logMonoTime
    if last is None or t - last > NEW_ALERT_NS:
      alerts.append([t, trig[0]])
    for k in trig:
      if RANK.index(k) < RANK.index(alerts[-1][1]):
        alerts[-1][1] = k
    last = t
  return [(t, k) for t, k in alerts]


def clock_offset(msgs, fallback_start_ns: int | None) -> int | None:
  """mono -> wall clock, as extract_segment does it: a GPS fix, else the API's segment start time."""
  for m in msgs:
    if m.which() == "gpsLocationExternal":
      g = m.gpsLocationExternal
      if g.hasFix and g.unixTimestampMillis > 0:
        return g.unixTimestampMillis * 1_000_000 - m.logMonoTime
  if fallback_start_ns is None:
    return None
  seg_t0 = next((m.logMonoTime for m in msgs if m.which() != "initData"), msgs[0].logMonoTime)
  return fallback_start_ns - seg_t0


def _window(paths, start: int, stop: int) -> list:
  out = []
  for p in paths:
    out += [m for m in read_events(p) if start <= m.logMonoTime <= stop
            and m.which() in ("carState", "radarState", "selfdriveState", "carControl", "onroadEvents")]
  out.sort(key=lambda m: m.logMonoTime)
  return out


def render(anchor: int, kind: str, msgs, offset: int, tags: dict, source: str) -> list[Point]:
  """The alert's summary point (at `anchor`, the qlog's time for it) and its 0.1 s close-up from `msgs`."""
  # the alert's own start in this log (the rlog sees it a little earlier than the qlog)
  near = [m.logMonoTime for m in msgs if anchor - 1.5e9 <= m.logMonoTime <= anchor + 0.5e9
          and (_triggers(m) or (m.which() == "selfdriveState" and m.selfdriveState.alertType.split("/")[0] in KINDS))]
  t_alert = min(near, default=anchor)

  def pt(measurement: str, t: int) -> Point:
    p = Point(measurement).time(t, WritePrecision.NS)
    for k, v in tags.items():
      p = p.tag(k, v)
    return p

  cur: dict = {}
  text = ""
  alert_end = t_alert
  reaction = None
  rows = []
  grid = ((t_alert + offset - int(BEFORE_S * 1e9)) // STEP_NS + 1) * STEP_NS - offset  # mono time of the first 0.1 s mark
  stop = t_alert + int(AFTER_S * 1e9)
  for m in msgs:
    t = m.logMonoTime
    while grid < t and grid <= stop:
      if "v" in cur:
        rows.append((grid, dict(cur)))
      grid += STEP_NS
    w = m.which()
    if w == "carState":
      cs = m.carState
      if t >= t_alert and reaction is None and cs.brakePressed:
        reaction = max(0.0, (t - t_alert) / 1e9)
      cur.update(v=cs.vEgo, a=cs.aEgo, brake=cs.brakePressed, gas=cs.gasPressed)
    elif w == "radarState":
      lead = m.radarState.leadOne
      cur.update(lead=lead.present, d=lead.dRel, v_lead=lead.vLead, v_rel=lead.vRel)
    elif w == "selfdriveState":
      s = m.selfdriveState
      showing = s.alertType.split("/")[0] in KINDS
      cur["alert"] = showing
      if showing and t >= t_alert - 1e9:
        text = text or " · ".join(x for x in (s.alertText1, s.alertText2) if x)
        alert_end = t
    elif w == "carControl":
      cur.update(long=m.carControl.longActive, lat=m.carControl.latActive, accel_cmd=m.carControl.actuators.accel)
  while grid <= stop and "v" in cur:  # the log ended before the window did
    rows.append((grid, dict(cur)))
    grid += STEP_NS

  points = []
  for t, c in rows:
    p = (pt("collision_detail", t + offset).field("t_rel", round((t - t_alert) / 1e9, 2))
         .field("speed_mph", round(c["v"] * MPH, 2)).field("accel", round(c["a"], 2))
         .field("brake", int(c["brake"])).field("gas", int(c["gas"])).field("alert", int(c.get("alert", False)))
         .field("op_long", int(c.get("long", False))))
    if c.get("lead"):
      p = p.field("lead_m", round(c["d"], 1)).field("lead_mph", round(c["v_lead"] * MPH, 2))
      if c["v_rel"] < -0.1:
        p = p.field("ttc_s", round(c["d"] / -c["v_rel"], 2))
    if c.get("long"):
      p = p.field("accel_cmd", round(c["accel_cmd"], 2))
    points.append(p)

  at = next((c for t, c in rows if t >= t_alert), rows[-1][1] if rows else {})
  # who was driving: just before the alert (MADS pauses steering the moment you brake)
  before = next((c for t, c in reversed(rows) if t < t_alert - 2e8), at)
  after = [c for t, c in rows if t >= t_alert]
  s = (pt("collision_alert", anchor + offset).tag("kind", KINDS[kind]).field("text", text or "")
       .field("source", source).field("alert_s", round((alert_end - t_alert) / 1e9, 1)))
  if at:
    s = (s.field("speed_mph", round(at["v"] * MPH, 1)).field("op_long", int(before.get("long", False)))
         .field("op_steer", int(before.get("lat", False))).field("gas", int(before["gas"])).field("brake", int(before["brake"])))
    if at.get("lead"):
      s = s.field("lead_m", round(at["d"], 1)).field("lead_mph", round(at["v_lead"] * MPH, 1))
      if at["v_rel"] < -0.1:
        s = s.field("ttc_s", round(at["d"] / -at["v_rel"], 1))
  if after:
    s = s.field("peak_decel", round(max(0.0, -min(c["a"] for c in after)), 1))
    s = s.field("slowest_mph", round(min(c["v"] for c in after) * MPH, 1))
    gaps = [c["d"] for c in after if c.get("lead")]
    if gaps:
      s = s.field("closest_m", round(min(gaps), 1))
  if reaction is not None:
    s = s.field("reaction_s", round(reaction, 2))
  return [s] + points


def _trigger_msgs(path) -> list:
  """The messages with a trigger in a neighbouring segment's qlog (none if it isn't here)."""
  return [] if path is None else [m for m in read_events(path) if _triggers(m)]


def extract_alerts(seg: int, qlog_for, best_for, tags: dict, fallback_start_ns) \
    -> tuple[list[Point], list[tuple[int, int]], tuple[int, int] | None]:
  """Points for the alerts that start in segment `seg`, each one's close-up span (wall ns) for clearing old points,
  and the segment's own span (wall ns), for clearing alert points written for it before; None if it can't be placed.

  An alert whose triggers run on from the segment before belongs to that one, and triggers running on into the
  segment after count toward its strongest kind: each segment is one file, the alert isn't.

  qlog_for(seg) / best_for(seg) -> the segment's qlog / best log (rlog if copied, else qlog), or None."""
  q = qlog_for(seg)
  if q is None:
    return [], [], None
  qmsgs = list(read_events(q))
  if not qmsgs:
    return [], [], None
  offset = clock_offset(qmsgs, fallback_start_ns)
  if offset is None:
    return [], [], None
  # the segment's own span, to tell whether a close-up reaches into the one before / after
  seg_t0 = next((m.logMonoTime for m in qmsgs if m.which() != "initData"), qmsgs[0].logMonoTime)
  seg_t1 = qmsgs[-1].logMonoTime
  seg_span = (seg_t0 + offset, seg_t1 + offset)
  alerts = find_alerts(qmsgs)
  stale = []  # close-up of a copy written before for an alert that turns out to be the previous segment's
  if alerts:
    triggers = [m.logMonoTime for m in qmsgs if _triggers(m)]
    # only read a neighbour when an alert touches that edge (its triggers may run on across it)
    before = _trigger_msgs(qlog_for(seg - 1)) if seg > 0 and triggers[0] - seg_t0 <= NEW_ALERT_NS else []
    after = _trigger_msgs(qlog_for(seg + 1)) if seg_t1 - triggers[-1] <= NEW_ALERT_NS else []
    before = [m for m in before if m.logMonoTime < seg_t0]
    after = [m for m in after if m.logMonoTime > seg_t1]
    if before or after:
      joined = find_alerts(before + qmsgs + after)
      if alerts[0][0] not in {t for t, _ in joined}:  # this segment's first alert is the previous one's carrying on
        # the previous segment rewrites the close-up up to its own stop; what a copy written before had past that goes
        own = max(t for t, _ in joined if t < seg_t0)
        own_stop, copy_stop = own + int((AFTER_S + 1) * 1e9), alerts[0][0] + int((AFTER_S + 1) * 1e9)
        if copy_stop > own_stop:
          stale.append((own_stop + 1 + offset, copy_stop + offset))
      alerts = [a for a in joined if seg_t0 <= a[0] <= seg_t1]
  if not alerts:
    return [], stale, seg_span
  points, spans = [], list(stale)
  for anchor, kind in alerts:
    start, stop = anchor - int((BEFORE_S + 2) * 1e9), anchor + int((AFTER_S + 1) * 1e9)
    paths = [best_for(s) for s, needed in ((seg - 1, start < seg_t0), (seg, True), (seg + 1, stop > seg_t1)) if needed]
    sources = {str(p).endswith("rlog.zst") for p in paths if p is not None}
    msgs = _window([p for p in paths if p is not None], start, stop)
    points += render(anchor, kind, msgs, offset, tags, "rlog" if sources == {True} else "qlog")
    spans.append((start + offset, stop + offset))
  return points, spans, seg_span
