"""Errors during a drive, as intervals: one `fault` point when a fault starts (code, end_ns, duration, frames) and
a code=0 point when it ends, so the timeline can carry the state forward and tables/annotations can use the start rows.

Codes are unique and stored, so they never change; SEVERITY ranks them for the timeline's worst-fault-at-each-moment.
"""
from influxdb_client import Point, WritePrecision

# name -> (code, label, merge gap s, min duration s, min frames)
FAULTS = {
  # EPS latched its torque-overlay fault (H_CAN_EPSS_ToiFlt): it ignores openpilot's steering. rlog CAN only.
  "eps_torque_fault": (5, "EPS ignoring steering (ToiFlt)", 2.0, 0.5, 0),
  # EPS silently not applying openpilot's torque: we request it (ACM_lkaActToi) but ToiActive stays 0 with no ToiFlt,
  # for 0.3 s or more while openpilot steers in drive. rlog CAN only. Stitched across segments by sync.stitch_faults.
  "rack_ignoring_torque": (8, "rack ignoring torque", 1.0, 0.3, 0),
  # EPS reports a steering system fault / angle controller (EAC) fault. rlog CAN only.
  "eps_fault": (4, "EPS fault", 2.0, 0.0, 0),
  # panda safety blocked openpilot's commands while it was steering (safetyTxBlocked rising). qlog, so every drive.
  "steering_blocked": (3, "panda blocked steering", 1.0, 0.0, 10),
  # openpilot itself flagged a steering fault (carState.steerFaultTemporary/Permanent) while in drive
  "op_steer_fault": (2, "openpilot steering fault", 1.0, 0.3, 0),
  # CAN messages missing/invalid (carState.canValid false) while in drive
  "can_error": (1, "CAN error", 1.0, 0.0, 0),
  # openpilot raised a system-error alert (commIssue, locationd/paramsd errors, ...) while in drive, past startup.
  # One interval per alert; its name is in the `event` tag. qlog, so every drive.
  "op_alert": (6, "openpilot alert", 2.0, 0.0, 0),
  # an engagement that ended within ENGAGE_SHORT_S without the driver doing it (car dropped ACC, openpilot cancelled or
  # an alert). Written whole by ct/engage.py, not through FaultTracker.update.
  "engage_dropped": (7, "engagement didn't stick", 0.0, 0.0, 0),
}

# codes from least to most severe
SEVERITY = [6, 7, 1, 2, 3, 4, 8, 5]


class FaultTracker:
  """Feed per-frame fault flags in time order; get back merged intervals as Points."""

  def __init__(self, tags: dict, offset: int, extra: dict | None = None):
    self.tags, self.offset, self.extra = tags, offset, extra or {}
    self.open: dict[str, list] = {}  # key -> [name, start, last_seen, frames, extra fields at start]
    self.points: list[Point] = []

  def update(self, name: str, t: int, active: bool, frames: int = 1, key: str | None = None, **fields) -> None:
    """`key` splits one fault type into separate intervals, tagged `event` (e.g. one per openpilot alert)."""
    key = key or name
    cur = self.open.get(key)
    merge_ns = FAULTS[name][2] * 1e9
    if cur and t - cur[2] > merge_ns:
      self._close(key)
      cur = None
    if active:
      if cur is None:
        self.open[key] = [name, t, t, frames, fields]
      else:
        cur[2] = t
        cur[3] += frames

  def add(self, name: str, start: int, end: int, **fields) -> None:
    """One whole interval at once (start/end in log time)."""
    p = (self._pt(start, name, None).field("code", FAULTS[name][0]).field("end_ns", end + self.offset)
         .field("duration_s", round((end - start) / 1e9, 2)).field("frames", 0))
    for k, v in fields.items():
      p = p.field(k, v)
    self.points += [p, self._pt(end, name, None).field("code", 0)]

  def finish(self, t_end: int | None = None) -> list[Point]:
    """Close everything still open (at the segment end, so a fault running into the next segment isn't cut short)."""
    for key in list(self.open):
      if t_end is not None and t_end - self.open[key][2] < 0.5e9:
        self.open[key][2] = t_end
      self._close(key)
    return self.points

  def _close(self, key: str) -> None:
    name, start, end, frames, fields = self.open.pop(key)
    event = key if key != name else None
    code, _, _, min_s, min_frames = FAULTS[name]
    dur = (end - start) / 1e9
    if dur < min_s or frames < min_frames:
      return
    p = (self._pt(start, name, event).field("code", code).field("end_ns", end + self.offset)
         .field("duration_s", round(dur, 2)).field("frames", frames))
    for k, v in fields.items():
      p = p.field(k, v)
    self.points += [p, self._pt(end, name, event).field("code", 0)]

  def _pt(self, t: int, name: str, event: str | None) -> Point:
    p = Point("fault").time(t + self.offset, WritePrecision.NS).tag("fault", name)
    if event:
      p = p.tag("event", event)
    for k, v in self.tags.items():
      p = p.tag(k, v)
    return p
