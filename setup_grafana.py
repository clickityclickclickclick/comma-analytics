#!/usr/bin/env python3
"""Setup (safe to re-run; the container runs it on every start): InfluxDB bucket + read-only token for Grafana,
the Grafana data source, and the dashboard (keeping any layout changes made in Grafana)."""
import datetime as dt
import html
import json
import re
import sys
import time

import requests
from influxdb_client import InfluxDBClient, Permission, PermissionResource

from ct import dashboard_v2
from ct.config import ENV, secret
from ct.engage import ENGAGE_SHORT_S
from ct.faults import FAULTS, SEVERITY

BUCKET = ENV["INFLUX_BUCKET"]
DS_UID = "comma-influx"
DASH_UID = "comma-telemetry"
ZOOM_URL = "/d/" + DASH_UID + "/comma-drives?var-route=${route}&${driver:queryparam}&from=${route_from}&to=${route_to}"
DS = {"type": "influxdb", "uid": DS_UID}

MODE_COLORS = {  # mode -> (colour, label, code)
  "full": ("green", "Full (steer + accel/brake)", 4),
  "steer_stock_acc": ("purple", "Steer + stock ACC", 3),
  "mads": ("blue", "MADS (steer only)", 2),
  "acc_only": ("orange", "ACC only", 1),
  "manual": ("#8e8e8e", "Manual", 0),
}

TZ = f'import "timezone"\noption location = timezone.location(name: "{ENV["TZ_NAME"]}")\n'
# fixed left-axis width for the drive graphs, so their time axes line up with the mode timeline,
# whose left margin is its longest row label ("accelerator (override)"). Each graph has two left axes of AXIS_WIDTH/2
# and no axis titles (they add width) or right axis, so the right edges match too. Measured with a headless browser:
# the timeline's plot starts 12 px further right than graphs with 132.
AXIS_WIDTH = 144
DOWNSAMPLE = ('  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\n'
              '  |> keep(columns: ["_time", "_value", "_field"])')
DAY_LABEL = ('days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]\n'
             'pad = (n) => if n < 10 then "0" + string(v: n) else string(v: n)\n'
             'dayLabel = (t) => days[date.weekDay(t: t)] + " " + pad(n: date.month(t: t)) + "/" + pad(n: date.monthDay(t: t))\n')
# fault code -> colour (worst first); labels come from ct.faults
FAULT_COLOURS = {5: "#C4162A", 8: "#A50E2D", 4: "#E02F44", 3: "#FF7F00", 2: "#FFB357", 1: "#A16D6D", 7: "#D683CE", 6: "#C0A16B"}
# the timeline row plots severity rank (1 = least), not the stored code
FAULT_RANK = ("rank = (c) => " + " ".join(f"if c == {code} then {i} else" for i, code in enumerate(SEVERITY, 1)) + " 0\n")
# Flux dict: fault name -> readable label
FAULT_LABELS = ('import "dict"\nlabels = dict.fromList(pairs: [' +
                ", ".join(f'{{key: "{k}", value: "{v[1]}"}}' for k, v in FAULTS.items()) + '])\n')
# openpilot alerts carry their name in the `event` tag: "openpilot alert: commIssue"
FAULT_LABEL = 'dict.get(dict: labels, key: r.fault, default: r.fault) + (if exists r.event then ": " + r.event else "")'
# units: data is stored in miles / mph; metric dashboards convert at query time
METRIC = ENV.get("UNITS", "imperial") == "metric"
DIST = "km" if METRIC else "mi"
DIST_WORD = "Kilometres" if METRIC else "Miles"
MI_TO_DIST = 1.609344 if METRIC else 1.0
SPEED_UNIT = "velocitykmh" if METRIC else "velocitymph"
TO_DIST = f'  |> map(fn: (r) => ({{r with _value: r._value * {MI_TO_DIST}}}))\n' if METRIC else ""
# distance since the drive started (column d, in DIST), from the 10 Hz speed; looks back a day so a zoomed-in view
# still counts from the start. Gaps (missing segments) count at most 1 s, like the mode summaries.
ODOMETER_PIPE = (f'from(bucket: "{BUCKET}")\n'
                 '  |> range(start: date.sub(d: 1d, from: v.timeRangeStart), stop: v.timeRangeStop)\n'
                 '  |> filter(fn: (r) => r._measurement == "drive" and r.route == "${route}" and r._field == "speed_mph")\n'
                 '  |> elapsed(unit: 1ms)\n'
                 f'  |> map(fn: (r) => ({{_time: r._time, d: r._value * {MI_TO_DIST} * float(v: if r.elapsed > 1000 then 1000 else r.elapsed) / 3600000.0}}))\n'
                 '  |> cumulativeSum(columns: ["d"])\n')
# the day before the range only feeds the running total: drop it, or a zoomed-in view returns tens of thousands of rows
ODOMETER = 'import "date"\n' + ODOMETER_PIPE + '  |> filter(fn: (r) => r._time >= v.timeRangeStart)\n'
# distance as a tooltip-only series on the drive graphs (not drawn, no axis, so the time axes still line up)
DISTANCE_TOOLTIP = {"matcher": {"id": "byName", "options": "distance"},
                    "properties": [{"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}},
                                   {"id": "unit", "value": DIST}, {"id": "decimals", "value": 2},
                                   {"id": "displayName", "value": "distance into drive"}]}
# route map background: OpenStreetMap data, OpenFreeMap's muted Positron style (OpenMapTiles schema; free, no API key)
# drawn by Grafana's MapLibre basemap, so the mode colours stand out. Not CARTO (needs a key now) or VersaTiles'
# shortbread styles (only their labels draw here); "liberty" is OpenFreeMap's colourful one
MAP_BASE = {"type": "maplibre", "name": "Map", "opacity": 1.0, "config": {"url": "https://tiles.openfreemap.org/styles/positron"}}
# radar distance to the car ahead is in metres; feet on imperial dashboards
# driver monitoring states (ct.extract.ATTENTION code -> label, colour), for the timeline row
# The warnings carry the device's alert text and the colour comma's UI (mici) gives that alert level: 2 orange, 3 red;
# level 1 is a black banner there, white (its text) here so it shows on the dark dashboard
ATTENTION_COLOURS = {0: ("attentive", "transparent"), 1: ("wheel touch (camera not used)", "#5A6E8C"), 2: ("no face seen", "#8e8e8e"),
                     3: ("distracted", "#FADE2A"),
                     4: ("1: Pay Attention", "#FFFFFF"), 5: ("1: Touch Steering Wheel: No Face Detected", "#FFFFFF"),
                     6: ("2: Pay Attention (Driver Distracted)", "#FF7300"), 7: ("2: Touch Steering Wheel (Driver Unresponsive)", "#FF7300"),
                     8: ("3: DISENGAGE IMMEDIATELY (Driver Distracted)", "#FF0015"),
                     9: ("3: DISENGAGE IMMEDIATELY (Driver Unresponsive)", "#FF0015")}
LEAD_DIST_FACTOR = 1.0 if METRIC else 3.28084
LEAD_DIST_UNIT = "lengthm" if METRIC else "lengthft"
OVERVIEW_DAYS = 30  # the overview panels show the last this many days, whatever the time picker says
OVERVIEW_PANELS = {1, 2, 3, 4, 33, 42}
CSS_PANEL = 39  # the top bar's CSS (below); always where the code puts it, whatever the live layout says
EVENT_LOG = 40  # the drive's events, one row each; scrolls to the shared crosshair's time (FOLLOW_JS)
BAR = '[data-testid="data-testid dashboard controls"]'
ZOOM_LINK = 'a[data-testid="data-testid Dashboard link"][href*="/d/' + DASH_UID + '/"]'
VAR = '[data-testid="data-testid template variable"]'
DRIVE_VAR = VAR + ':has([data-testid="data-testid Dashboard template variables submenu Label Drive"])'
TOP_BAR_CSS = ("<style>"
               f"{BAR} {{ display: flex !important; flex-wrap: wrap; align-items: center; column-gap: 0; padding-top: 8px !important }}"
               # Grafana puts 8px under every control: 4px keeps the lines closer together
               f"{BAR} > * {{ margin-bottom: 4px !important }}"
               # first line: Drive, then "Zoom to selected drive", then the time picker + refresh on the right
               f"{BAR} > {DRIVE_VAR} {{ order: 1 }}"
               f'{BAR} > [class*="dashboard-links-controls"] {{ order: 2 }}'
               f"{BAR} > :first-child {{ order: 3; margin: 0 0 4px auto !important; display: flex; gap: 8px }}"  # same bottom margin as the rest
               f"{BAR} > :first-child > * {{ margin: 0 !important }}"
               f'{BAR}::before {{ content: ""; flex-basis: 100%; order: 4 }}'  # line break
               # then the other dropdowns, then the annotation toggles
               f"{BAR} > {VAR} {{ order: 5 }}"
               f'{BAR}::after {{ content: ""; flex-basis: 100%; order: 6 }}'
               f'{BAR} > [class*="dashboard-data-layer-controls"] {{ order: 7 }}'
               f"{ZOOM_LINK} {{ background: #3D71D9; border-color: #3D71D9; color: #fff; font-weight: 500 }}"
               f"{ZOOM_LINK}:hover {{ background: #2F5FC4 }}"
               # this panel's own headerless row takes no space. Not display: none: with no width, Grafana stops drawing
               # the panel, which takes this CSS away, which brings the row back, and so on, many times a second
               f'[data-testid^="data-testid dashboard-row-wrapper"]:has([data-viz-panel-key="panel-{CSS_PANEL}"])'
               ' { position: absolute; width: 100%; visibility: hidden; pointer-events: none }'
               "</style>")
# the event log follows the shared crosshair: on each hover (Grafana's data-hover event, which carries the time under the
# cursor) scroll the log to the last event at or before it, level with the cursor, and highlight that row. Runs from the
# top bar's panel (always drawn); an <img> error handler because Grafana doesn't run <script> in text panels. No "$" in
# it: Grafana would read it as a variable
FOLLOW_JS = """(function () {
  if (window.ctFollow) return;
  window.ctFollow = 1;
  var key = 'panel-%d', DATA = '\\x24data', style = document.createElement('style'), mouseY = 0, scene = null, sub = null;
  document.head.appendChild(style);
  document.addEventListener('mousemove', function (e) { mouseY = e.clientY; }, true);
  function find(o) {
    if (o.state && o.state.key === key) return o;
    var r = null;
    if (o.forEachChild) o.forEachChild(function (c) { r = r || find(c); });
    return r;
  }
  function follow(e) {
    var t = e.payload && e.payload.point && e.payload.point.time, panel = t != null && find(scene);
    var data = panel && panel.state[DATA] && panel.state[DATA].state.data, frame = data && data.series[0];
    var grid = document.querySelector('[data-viz-panel-key=' + key + '] [role=grid]');
    if (!frame || !grid) return;
    var times = frame.fields[0].values, lo = 0, hi = times.length - 1, i = -1;
    while (lo <= hi) { var mid = (lo + hi) >> 1; if (times[mid] <= t) { i = mid; lo = mid + 1; } else hi = mid - 1; }
    var head = grid.querySelector('[aria-rowindex=\\'1\\'] > *'), cell = grid.querySelector('[aria-rowindex=\\'2\\'] > *');
    var headH = head ? head.offsetHeight : 0, rowH = cell ? cell.offsetHeight : 36, box = grid.getBoundingClientRect();
    var y = Math.min(Math.max(mouseY, box.top + headH + rowH / 2), box.bottom - rowH / 2);
    grid.scrollTop = box.top + headH + Math.max(i, 0) * rowH + rowH / 2 - y;
    style.textContent = i < 0 ? '' : '[data-viz-panel-key=' + key + '] [aria-rowindex=\\'' + (i + 2) + '\\'] > * '
      + '{ background: rgba(255, 152, 48, 0.3) !important; font-weight: 500 }';
  }
  setInterval(function () {
    var s = window.__grafanaSceneContext;
    if (s === scene) return;
    if (sub) sub.unsubscribe();
    scene = s;
    sub = s && s.subscribeToEvent({ type: 'data-hover' }, follow);
  }, 1000);
})()""" % EVENT_LOG
DRIVER_FILTER = ('  |> filter(fn: (r) => contains(value: r.driver, set: ${driver:json}))\n'
                 # the drives that ran the models picked at the top (model_routes)
                 '  |> filter(fn: (r) => contains(value: r.route, set: ${model_routes:json}))\n')


def zero_filled(measurement: str, fields: list[str], zero: list[str] = (), convert: str = "") -> str:
  """This drive's `fields` from a measurement that only exists some of the time, downsampled; the `zero` ones (default:
  all) read 0 in every window the drive has data in but the measurement doesn't (rather than a gap)."""
  match = " or ".join(f'r._field == "{f}"' for f in fields)
  return ('data = ' + src(measurement, f' and r.route == "${{route}}" and ({match})').lstrip() + convert + DOWNSAMPLE + '\n'
          'moving = ' + src("drive", ' and r.route == "${route}" and r._field == "speed_mph"').lstrip() +
          '  |> aggregateWindow(every: v.windowPeriod, fn: count, createEmpty: false)\n'
          '  |> map(fn: (r) => ({_time: r._time, _value: 0.0}))\n'
          'union(tables: [data, ' + ", ".join(f'moving |> set(key: "_field", value: "{f}")' for f in zero or fields) + '])\n'
          '  |> group(columns: ["_field", "_time"])\n  |> sum()\n'
          '  |> group(columns: ["_field"])\n  |> sort(columns: ["_time"])')


# drive tables: start time compactly, and the route as a link that opens that drive
START_COLUMN = {"matcher": {"id": "byName", "options": "start"},
                "properties": [{"id": "displayName", "value": "Start"}, {"id": "unit", "value": "time:MM/DD h:mm A"},
                               {"id": "custom.width", "value": 115}]}
# shown as its counter (000000a1); the link (and hover) keep the full route id
ROUTE_LINK = {"matcher": {"id": "byName", "options": "route"},
              "properties": [{"id": "displayName", "value": "Route"}, {"id": "custom.width", "value": 95},
                             {"id": "mappings", "value": [{"type": "regex", "options": {"pattern": "^([0-9a-f]+)--.*$", "result": {"text": "$1"}}}]},
                             {"id": "links", "value": [{"title": "Show this drive",
                               "url": "/d/" + DASH_UID + "/comma-drives?var-route=${__value.raw}&${driver:queryparam}&from=${__data.fields.from_ms}&to=${__data.fields.to_ms}"}]}]}


def fixed_width(width: int, *names: str) -> list[dict]:
  return [{"matcher": {"id": "byName", "options": n}, "properties": [{"id": "custom.width", "value": width}]} for n in names]


def src(measurement: str, extra: str = "") -> str:
  return (f'from(bucket: "{BUCKET}")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n'
          f'  |> filter(fn: (r) => r._measurement == "{measurement}"{extra})\n')


def target(query: str, ref: str = "A") -> dict:
  return {"refId": ref, "datasource": DS, "query": query}


def mode_overrides(by: str) -> list[dict]:
  """Colour + label per mode. `by` = 'name' (field named after mode) or a label lookup."""
  return [{"matcher": {"id": "byName", "options": m},
           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}, {"id": "displayName", "value": lbl}]}
          for m, (c, lbl, _) in MODE_COLORS.items()]


def mode_text_mappings() -> list[dict]:
  return [{"type": "value", "options": {m: {"text": lbl, "color": c, "index": i}
                                        for i, (m, (c, lbl, _)) in enumerate(MODE_COLORS.items())}},
          {"type": "value", "options": {"0": {"text": "off", "color": "transparent"}, "1": {"text": "on", "color": "red"}}}]


def code_thresholds() -> dict:
  steps = sorted(((code, c) for c, _, code in MODE_COLORS.values()))
  return {"mode": "absolute", "steps": [{"color": c, "value": None if i == 0 else code} for i, (code, c) in enumerate(steps)]}


def fault_mappings() -> list[dict]:
  return [{"type": "value", "options": {"0": {"text": " ", "color": "transparent"},
                                        **{str(SEVERITY.index(code) + 1): {"text": label, "color": FAULT_COLOURS[code]}
                                           for code, label, *_ in FAULTS.values()}}}]


def panel(pid, title, ptype, x, y, w, h, targets, **kw) -> dict:
  p = {"id": pid, "title": title, "type": ptype, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
       "targets": targets, "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
  for k, v in kw.items():
    p[k] = v
  if ptype == "table":  # long column names wrap rather than get cut off
    p["fieldConfig"].setdefault("defaults", {}).setdefault("custom", {})["wrapHeaderText"] = True
  return p


MODES_HELP = ("**Modes**\n\n"
              "- **Full**: openpilot steers and controls accel/brake\n"
              "- **Steer + stock ACC**: openpilot steers, the car's own cruise does the speed\n"
              "- **MADS**: openpilot steers only, you do the pedals\n"
              "- **ACC only**: the car's cruise, no steering\n"
              "- **Manual**: you drive")
# the (i) tooltip on each panel (Grafana renders it as Markdown); panels that set their own keep it
DESCRIPTIONS = {
  11: "Whether the last pull from comma's cloud worked.\n\n"
      "ERROR usually means the comma token expired or comma's API couldn't be reached; details in `docker compose logs sync`.",
  12: "When the sync last checked comma's cloud for new drives (hourly, and on every restart).",
  13: "Start of the newest drive in the database.",
  1: f"{DIST_WORD} driven each day (local days), split by mode.\n\n" + MODES_HELP +
     "\n\nDistances come from the car's wheel speed, which reads about 2–3% above comma's GPS figure.",
  2: f"Share of {DIST_WORD.lower()} by mode over the last {OVERVIEW_DAYS} days.\n\n" + MODES_HELP,
  3: f"{DIST_WORD} per driver over the last {OVERVIEW_DAYS} days, split by mode.\n\n"
     "Driver names come from Home Assistant if it's set up, otherwise every drive gets `DRIVER_NAME`.",
  4: f"Every drive in the last {OVERVIEW_DAYS} days. Click a route to open it below.\n\n"
     "- **Distance, Minutes**: comma's figures\n"
     "- **Engaged**: share of the distance openpilot was steering\n"
     "- **Errors**: steering/system errors (see the Errors table)\n"
     "- **FCW**: collision alerts (see Collision alerts on the drive)\n"
     "- **Distracted, DM Warnings**: the driver-monitoring camera, while openpilot steered\n"
     "- **Regen**: needs the device copy (rlogs)\n"
     "- **Lane Conf.**: how sure the driving model was that it saw the lane lines",
  21: "Errors found in the drives, newest first. Click a route to see that moment, ±1 minute.\n\n"
      "- EPS ignoring steering (ToiFlt), the rack silently ignoring openpilot's torque, and EPS faults "
      "(device-copied drives)\n"
      "- the panda blocking openpilot's steering commands\n"
      "- openpilot steering faults\n"
      "- CAN errors\n"
      "- openpilot system alerts\n\n"
      "Only while in drive; startup noise is filtered out.",
  5: "The selected drive over time, one row each:\n\n"
     "- **model**: which driving model ran (the device's own small model, or the big one on a Chestnut or Jetlink host)\n"
     "- **mode**: the openpilot mode\n"
     "- **driver attention**: from the driver-monitoring camera (distracted, no face seen), and its alerts as the "
     "device shows them: Pay Attention or Touch Steering Wheel, level 1 (white), 2 (orange) and 3 (red)\n"
     "- **lane confidence**: how sure the driving model was that it saw the lane lines while moving (red = barely, "
     "green = clearly; blank when stopped; low on unpainted roads too, not just in fog)\n"
     "- **steering control**: how openpilot was steering, by torque or by angle (blank while it wasn't). On a Rivian "
     "with Adventure Pilot's angle harness, angle is when openpilot steered with no torque applied\n"
     "- **model intent**: what the driving model wants to do with speed (red = brake, yellow = hold, green = speed up), "
     "the colours the device tints its path with in experimental mode. Not what the car did: it only acts on it while openpilot "
     "controls speed\n"
     "- **steering / accelerator / brake (override)**: when you overrode them\n"
     "- **braking (CAN)**: the braking source (device-copied drives)\n"
     "- **errors**: the worst error at each moment\n"
     "- **events**: openpilot events (grey = while not engaged)\n\n"
     "Dashed lines with ticks on the time axis are distance markers.",
  6: "GPS track of the selected drive, coloured by mode: green Full, purple Steer + stock ACC, blue MADS, orange ACC only, "
     "grey Manual.\n\nHover a point for its speed.",
  7: "Speed and steering-wheel angle (positive = left).\n\n"
     "Background shading is the mode; red bands are errors. Hover for the distance into the drive.",
  8: "Torque on the steering:\n\n"
     "- what the driver applied\n"
     "- what openpilot asked for (-1..1 of its limit)\n"
     "- whether the steering rack was applying openpilot's torque (1) or not (0): Rivian, device-copied drives only. "
     "At 0 while openpilot is steering in torque mode, the rack is ignoring it\n"
     "- what the steering motor (EPS) reports, on cars that report it\n\n"
     "Driver and EPS torque are in the car's own units. Red bands are errors. Hover for the distance into the drive.",
  9: "Every openpilot event on the selected drive: how many times it started and when it first did.\n\n"
     "Includes routine ones (overrides, lane changes, speed too low for steering) as well as alerts.",
}


# collision alerts (ct.safety). The close-up reads the selected alert's own 15 s, whatever the dashboard's time range;
# $alert is that alert's time in ms (empty on a drive without one, which matches nothing)
ALERT_WINDOW = ('a = "${alert}"\n'
                't0 = if a =~ /^[0-9]+$/ then int(v: a) * 1000000 else 0\n'
                f'from(bucket: "{BUCKET}")\n'
                '  |> range(start: time(v: t0 - 7000000000), stop: time(v: t0 + 9000000000))\n'
                '  |> filter(fn: (r) => r._measurement == "collision_detail" and r.route == "${route}")\n'
                '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
                '  |> group()\n  |> sort(columns: ["_time"])\n'
                # missing fields (no car ahead, not closing in) stay null through the conversions
                f'  |> map(fn: (r) => ({{r with speed_mph: r.speed_mph * {MI_TO_DIST}, lead_mph: r.lead_mph * {MI_TO_DIST}, '
                f'lead_m: r.lead_m * {LEAD_DIST_FACTOR}}}))\n')
ALERT_COLOUR = "#C4162A"
ALERT_LINK = "/d/" + DASH_UID + "/comma-drives?var-route=${route}&${driver:queryparam}&var-alert=${__data.fields.ms}"


def alert_band(name: str, label: str, colour: str, height: float) -> dict:
  """A 0/1 field as a shaded band behind the close-up's lines (height 1 = full, 2 = the bottom half)."""
  return {"matcher": {"id": "byName", "options": name},
          "properties": [{"id": "displayName", "value": label}, {"id": "color", "value": {"mode": "fixed", "fixedColor": colour}},
                         {"id": "custom.fillOpacity", "value": 25}, {"id": "custom.lineWidth", "value": 0},
                         {"id": "custom.lineInterpolation", "value": "stepAfter"}, {"id": "custom.axisPlacement", "value": "hidden"},
                         {"id": "min", "value": 0}, {"id": "max", "value": height}]}


def collision_panels() -> list[dict]:
  callout = panel(35, "Collision alerts on this drive", "table", 0, 45, 24, 5,
    [target(src("collision_alert", ' and r.route == "${route}"') +
            '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
            '  |> sort(columns: ["_time"])\n'
            '  |> map(fn: (r) => {\n'
            '      steer = exists r.op_steer and r.op_steer == 1\n'
            '      long = exists r.op_long and r.op_long == 1\n'
            '      ms = int(v: r._time) / 1000000\n'
            '      return {time: r._time, ms: ms, from_ms: ms - 20000, to_ms: ms + 20000, what: r.text, by: r.kind,\n'
            f'        speed: r.speed_mph * {MI_TO_DIST}, gap: r.lead_m * {LEAD_DIST_FACTOR}, lead_speed: r.lead_mph * {MI_TO_DIST},\n'
            '        ttc: r.ttc_s, reaction: r.reaction_s, peak_g: r.peak_decel / 9.81,\n'
            f'        slowest: r.slowest_mph * {MI_TO_DIST}, closest: r.closest_m * {LEAD_DIST_FACTOR},\n'
            '        openpilot: if steer and long then "steering + speed" else if steer then "steering only" else if long then "speed only" else "off",\n'
            '        detail: if r.source == "rlog" then "full log" else "qlog"}\n'
            '  })')],
    description="openpilot's forward collision warning (BRAKE! Risk of Collision) and the car's own collision warning / automatic "
                "emergency braking, at the moment it fired:\n\n"
                "- your speed, and the car ahead (gap and its speed)\n"
                "- time to impact if nothing changed\n"
                "- how soon you were on the brake\n"
                "- the hardest braking and the closest you got after it\n"
                "- **openpilot was**: what it was doing just before\n\n"
                "Click a time for its close-up (just below) or to zoom the drive to it. Red markers on the graphs show each alert.",
    transformations=[{"id": "organize", "options": {"excludeByName": {"ms": True, "from_ms": True, "to_ms": True},
                                                    "indexByName": {"time": 0, "what": 1, "by": 2, "speed": 3, "gap": 4, "lead_speed": 5,
                                                                    "ttc": 6, "reaction": 7, "peak_g": 8, "slowest": 9, "closest": 10,
                                                                    "openpilot": 11, "detail": 12}}}],
    fieldConfig={"defaults": {"noValue": "No collision alerts on this drive", "custom": {"minWidth": 50, "wrapHeaderText": True, "wrapText": True}},
                 "overrides": [
                   {"matcher": {"id": "byName", "options": "time"},
                    "properties": [{"id": "displayName", "value": "When"}, {"id": "unit", "value": "time:h:mm:ss A"},
                                   {"id": "custom.width", "value": 105},
                                   {"id": "links", "value": [
                                     {"title": "Show its close-up", "url": ALERT_LINK + "&from=${__from}&to=${__to}"},
                                     {"title": "Zoom the drive to this moment", "url": ALERT_LINK + "&from=${__data.fields.from_ms}&to=${__data.fields.to_ms}"}]}]},
                   {"matcher": {"id": "byName", "options": "what"},
                    "properties": [{"id": "displayName", "value": "Alert"}, {"id": "custom.width", "value": 150},
                                   {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": ALERT_COLOUR}}]},
                   {"matcher": {"id": "byName", "options": "by"},
                    "properties": [{"id": "displayName", "value": "From"}, {"id": "custom.width", "value": 125},
                                   {"id": "description", "value": "openpilot FCW = openpilot's warning; car FCW / car AEB = the car's own system"}]},
                   {"matcher": {"id": "byName", "options": "speed"},
                    "properties": [{"id": "displayName", "value": "Speed"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0}]},
                   {"matcher": {"id": "byName", "options": "gap"},
                    "properties": [{"id": "displayName", "value": "Gap"}, {"id": "unit", "value": LEAD_DIST_UNIT}, {"id": "decimals", "value": 0},
                                   {"id": "noValue", "value": "–"}]},
                   {"matcher": {"id": "byName", "options": "lead_speed"},
                    "properties": [{"id": "displayName", "value": "Car ahead"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0},
                                   {"id": "noValue", "value": "–"}]},
                   {"matcher": {"id": "byName", "options": "ttc"},
                    "properties": [{"id": "displayName", "value": "To impact"}, {"id": "unit", "value": "suffix: s"}, {"id": "decimals", "value": 1},
                                   {"id": "noValue", "value": "–"},
                                   {"id": "description", "value": "Gap ÷ closing speed at the alert: how long until impact if neither car changed speed"},
                                   {"id": "mappings", "value": [{"type": "range", "options": {"from": 10, "to": 1e9, "result": {"text": "–"}}}]}]},
                   {"matcher": {"id": "byName", "options": "reaction"},
                    "properties": [{"id": "displayName", "value": "Braked after"}, {"id": "unit", "value": "suffix: s"}, {"id": "decimals", "value": 2},
                                   {"id": "noValue", "value": "didn't"},
                                   {"id": "description", "value": "From the alert to your foot on the brake (0 = already braking)"}]},
                   {"matcher": {"id": "byName", "options": "peak_g"},
                    "properties": [{"id": "displayName", "value": "Peak braking"}, {"id": "unit", "value": "suffix: g"}, {"id": "decimals", "value": 2}]},
                   {"matcher": {"id": "byName", "options": "slowest"},
                    "properties": [{"id": "displayName", "value": "Slowed to"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0}]},
                   {"matcher": {"id": "byName", "options": "closest"},
                    "properties": [{"id": "displayName", "value": "Closest"}, {"id": "unit", "value": LEAD_DIST_UNIT}, {"id": "decimals", "value": 0},
                                   {"id": "noValue", "value": "–"}]},
                   {"matcher": {"id": "byName", "options": "openpilot"},
                    "properties": [{"id": "displayName", "value": "openpilot was"}]},
                   {"matcher": {"id": "byName", "options": "detail"},
                    "properties": [{"id": "displayName", "value": "From log"}, {"id": "custom.width", "value": 80},
                                   {"id": "description", "value": "full log = 100 Hz, copied off the device; qlog = cloud logs (radar ~4 Hz)"}]}]},
    options={"showHeader": True, "cellHeight": "sm"})

  chart = panel(36, "Collision alert close-up (seconds from the alert)", "trend", 0, 50, 12, 9,
    [target(ALERT_WINDOW + '  |> keep(columns: ["t_rel", "speed_mph", "lead_mph", "lead_m", "alert", "brake"])')],
    description="The selected collision alert (pick one in Collision alerts above), from 6 s before to 8 s after:\n\n"
                "- your speed and the car ahead's\n"
                "- the gap between you (right axis)\n"
                "- when the alert showed (red) and when you were on the brake (orange)\n\n"
                "Hover for the values.",
    fieldConfig={"defaults": {"noValue": "No collision alerts on this drive",
                              "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": False}},
                 "overrides": [
                   {"matcher": {"id": "byName", "options": "t_rel"},
                    "properties": [{"id": "unit", "value": "s"}, {"id": "custom.axisLabel", "value": "seconds from the alert"}]},
                   {"matcher": {"id": "byName", "options": "speed_mph"},
                    "properties": [{"id": "displayName", "value": "your speed"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "min", "value": 0},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}}]},
                   {"matcher": {"id": "byName", "options": "lead_mph"},
                    "properties": [{"id": "displayName", "value": "car ahead's speed"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "min", "value": 0},
                                   {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 6]}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "#B877D9"}}]},
                   {"matcher": {"id": "byName", "options": "lead_m"},
                    "properties": [{"id": "displayName", "value": "gap to car ahead"}, {"id": "unit", "value": LEAD_DIST_UNIT}, {"id": "min", "value": 0},
                                   {"id": "custom.axisPlacement", "value": "right"},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "#73BF69"}}]},
                   alert_band("alert", "alert showing", "#F2495C", 1), alert_band("brake", "on the brake", "#FF9830", 2)]},
    options={"xField": "t_rel", "tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}})

  # every half second, landing on the alert itself (the 0.1 s points are on the clock's grid, not the alert's)
  table = panel(37, "Collision alert, second by second", "table", 12, 50, 12, 9,
    [target('import "math"\n' + ALERT_WINDOW +
            '  |> map(fn: (r) => ({r with tenths: int(v: math.round(x: r.t_rel * 10.0))}))\n'
            '  |> filter(fn: (r) => r.tenths % 5 == 0 and r.tenths >= -50)\n'
            '  |> map(fn: (r) => ({time: r._time, sec: float(v: r.tenths) / 10.0, speed: r.speed_mph, lead_speed: r.lead_mph, gap: r.lead_m,\n'
            '      ttc: r.ttc_s, accel: r.accel, pedal: if r.brake == 1 then "brake" else if r.gas == 1 then "gas" else "",\n'
            '      alert: if r.alert == 1 then "showing" else ""}))')],
    transformations=[{"id": "organize", "options": {"indexByName": {"time": 0, "sec": 1, "speed": 2, "lead_speed": 3, "gap": 4, "ttc": 5,
                                                                    "accel": 6, "pedal": 7, "alert": 8}}}],
    description="The selected collision alert every half second:\n\n"
                "- **You, Ahead, Gap**: your speed, the car ahead's speed and the gap\n"
                "- **Impact**: time to impact if neither car changed speed (only while closing in, under 10 s)\n"
                "- **Accel**: your acceleration (negative = slowing)\n"
                "- **Pedal**: which pedal you were on\n"
                "- **Alert**: whether the alert was showing",
    fieldConfig={"defaults": {"noValue": "No collision alerts on this drive", "custom": {"minWidth": 40, "wrapHeaderText": True}},
                 "overrides": [
                   {"matcher": {"id": "byName", "options": "time"},
                    "properties": [{"id": "displayName", "value": "Time"}, {"id": "unit", "value": "time:h:mm:ss.S"}, {"id": "custom.width", "value": 80}]},
                   {"matcher": {"id": "byName", "options": "sec"},
                    "properties": [{"id": "displayName", "value": "From alert"}, {"id": "unit", "value": "s"}, {"id": "decimals", "value": 1}, {"id": "custom.width", "value": 65},
                                   {"id": "description", "value": "Seconds from the alert: negative = before it, 0 = when it fired"}]},
                   {"matcher": {"id": "byName", "options": "speed"},
                    "properties": [{"id": "displayName", "value": "You"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0}]},
                   {"matcher": {"id": "byName", "options": "lead_speed"},
                    "properties": [{"id": "displayName", "value": "Ahead"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0},
                                   {"id": "noValue", "value": "–"}]},
                   {"matcher": {"id": "byName", "options": "gap"},
                    "properties": [{"id": "displayName", "value": "Gap"}, {"id": "unit", "value": LEAD_DIST_UNIT}, {"id": "decimals", "value": 0},
                                   {"id": "noValue", "value": "–"}]},
                   {"matcher": {"id": "byName", "options": "ttc"},
                    "properties": [{"id": "displayName", "value": "Impact"}, {"id": "unit", "value": "suffix: s"}, {"id": "decimals", "value": 1},
                                   {"id": "noValue", "value": "–"}, {"id": "mappings", "value": [{"type": "range", "options": {"from": 10, "to": 1e9, "result": {"text": "–"}}}]},
                                   {"id": "custom.cellOptions", "value": {"type": "color-text"}}, {"id": "color", "value": {"mode": "thresholds"}},
                                   {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                     {"color": "#F2495C", "value": None}, {"color": "#FF9830", "value": 2}, {"color": "text", "value": 4}]}}]},
                   {"matcher": {"id": "byName", "options": "accel"},
                    "properties": [{"id": "displayName", "value": "Accel m/s²"}, {"id": "decimals", "value": 1},
                                   {"id": "custom.cellOptions", "value": {"type": "color-text"}}, {"id": "color", "value": {"mode": "thresholds"}},
                                   {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                     {"color": "#F2495C", "value": None}, {"color": "#FF9830", "value": -4}, {"color": "text", "value": -1.5}]}}]},
                   {"matcher": {"id": "byName", "options": "pedal"},
                    "properties": [{"id": "displayName", "value": "Pedal"}, {"id": "noValue", "value": " "},
                                   {"id": "mappings", "value": [{"type": "value", "options": {"brake": {"color": "#FF9830", "index": 0}}}]},
                                   {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "transparent"}}]},
                   {"matcher": {"id": "byName", "options": "alert"},
                    "properties": [{"id": "displayName", "value": "Alert"}, {"id": "noValue", "value": " "},
                                   {"id": "mappings", "value": [{"type": "value", "options": {"showing": {"color": ALERT_COLOUR, "index": 0}}}]},
                                   {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "transparent"}}]}]},
    options={"showHeader": True, "cellHeight": "sm"})
  return [callout, chart, table]


def overview_range(p: dict) -> dict:
  """The panel's queries over the last OVERVIEW_DAYS days instead of the time picker's range, so zooming in on a drive
  leaves the overview alone. (A panel time override wouldn't do: Grafana ignores it while the picker is absolute.)"""
  for t in p["targets"]:
    q = (t["query"].replace("v.timeRangeStart", f"date.sub(d: {OVERVIEW_DAYS}d, from: now())")
         .replace("v.timeRangeStop", "now()"))
    t["query"] = q if 'import "date"' in q else 'import "date"\n' + q
  return p


INTENT_STEP = 0.5  # m/s²: the event log's model intent is brake below -this, speed up above it, hold between
STEERING_LABELS = {0: ("manual", "#8e8e8e"), 1: ("torque", "#4D9DFF"), 2: ("angle", "#3ADB6D"), 3: ("PID", "#B0BEC5"),
                   4: ("curvature", "#B0BEC5"), 5: ("debug", "#B0BEC5")}
INTENT_LABELS = {-1: ("brake", "#F2495C"), 0: ("hold", "#FADE2A"), 1: ("speed up", "#73BF69")}
LANE_STEPS = (0.5, 0.8)  # the event log's lane confidence is low below the first, high from the second, medium between
LANE_LABELS = {0: ("low", "#F2495C"), 1: ("medium", "#FADE2A"), 2: ("high", "#73BF69")}


def flux_label(labels: dict, prefix: str) -> str:
  """Flux expression naming r.c from `labels` ({code: (label, colour)})."""
  return "(" + "".join(f'if r.c == {c} then "{prefix}{label}" else ' for c, (label, _) in labels.items()) + f'"{prefix}" + string(v: r.c))'


# the event log: the drive's events, and where the mode timeline's steering control, driver attention, lane confidence
# and model intent rows change (keepFirst: the state at the start of the range too), as one table in time order for FOLLOW_JS
EVENT_LOG_QUERY = (
  'changes = (tables=<-) => tables\n  |> group()\n  |> sort(columns: ["_time"])\n  |> duplicate(column: "c", as: "d")\n'
  '  |> difference(columns: ["d"], keepFirst: true)\n  |> filter(fn: (r) => not exists r.d or r.d != 0)\n'
  'events = ' + src("events", ' and r.route == "${route}" and r._field == "count"').lstrip() +
  '  |> map(fn: (r) => ({_time: r._time, event: (if r.engaged == "1" then "" else "(manual) ") + r.event}))\n'
  'steering = ' + src("drive", ' and r.route == "${route}" and r._field == "lat_ctrl"').lstrip() +
  '  |> map(fn: (r) => ({r with c: r._value}))\n  |> changes()\n'
  '  |> map(fn: (r) => ({_time: r._time, event: ' + flux_label(STEERING_LABELS, "steering: ") + '}))\n'
  'attention = ' + src("driver_monitor", ' and r.route == "${route}" and r._field == "code"').lstrip() +
  '  |> map(fn: (r) => ({r with c: r._value}))\n  |> changes()\n'
  '  |> map(fn: (r) => ({_time: r._time, event: ' +
  flux_label({c: (label, "") for c, (label, _) in ATTENTION_COLOURS.items()}, "attention: ") + '}))\n'
  'intent = ' + src("vision", ' and r.route == "${route}" and r._field == "model_accel"').lstrip() +
  '  |> aggregateWindow(every: 1s, fn: mean, createEmpty: false, timeSrc: "_start")\n'
  f'  |> map(fn: (r) => ({{r with c: if r._value < -{INTENT_STEP} then -1 else if r._value > {INTENT_STEP} then 1 else 0}}))\n'
  '  |> changes()\n'
  '  |> map(fn: (r) => ({_time: r._time, event: ' + flux_label(INTENT_LABELS, "model intent: ") +
  ' + " " + string(v: float(v: int(v: r._value * 10.0 + (if r._value < 0.0 then -0.5 else 0.5))) / 10.0)}))\n'
  # the lane confidence row's 5 s means (it's only logged while moving, so stops leave no windows)
  'lanes = ' + src("vision", ' and r.route == "${route}" and r._field == "lane_moving"').lstrip() +
  '  |> aggregateWindow(every: 5s, fn: mean, createEmpty: false, timeSrc: "_start")\n'
  f'  |> map(fn: (r) => ({{r with c: if r._value < {LANE_STEPS[0]} then 0 else if r._value < {LANE_STEPS[1]} then 1 else 2}}))\n'
  '  |> changes()\n'
  '  |> map(fn: (r) => ({_time: r._time, event: ' + flux_label(LANE_LABELS, "lane confidence: ") +
  ' + " " + string(v: int(v: r._value * 100.0 + 0.5)) + "%"}))\n'
  'union(tables: [events, steering, attention, intent, lanes])\n  |> group()\n  |> sort(columns: ["_time"])')
# the added rows in their timeline row's colours (white attention alerts stay the default text colour)
EVENT_LOG_COLOURS = [{"type": "regex", "options": {"pattern": f"^{prefix}{label}( .*)?$", "result": {"color": colour}}}
                     for prefix, labels in (("steering: ", STEERING_LABELS), ("model intent: ", INTENT_LABELS), ("lane confidence: ", LANE_LABELS))
                     for label, colour in labels.values()] + \
                    [{"type": "regex", "options": {"pattern": f"^attention: {re.escape(label)}$", "result": {"color": colour}}}
                     for label, colour in ATTENTION_COLOURS.values() if colour not in ("transparent", "#FFFFFF")]


def build_dashboard() -> dict:
  per_mode = (src("mode_summary", ' and r._field == "miles"') + DRIVER_FILTER + TO_DIST +
              '  |> group(columns: ["mode"])\n')
  panels = [
    {"id": 100, "type": "row", "title": f"Overview (last {OVERVIEW_DAYS} days + driver)", "gridPos": {"x": 0, "y": 7, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(1, f"{DIST_WORD} per day by mode", "barchart", 0, 8, 16, 7,
          # whole local days, even when the time picker starts mid-day; x axis is categorical so bars never shrink to hours
          [target('import "date"\n' + TZ + DAY_LABEL +
                  f'from(bucket: "{BUCKET}")\n'
                  '  |> range(start: date.truncate(t: v.timeRangeStart, unit: 1d), stop: v.timeRangeStop)\n'
                  '  |> filter(fn: (r) => r._measurement == "mode_summary" and r._field == "miles")\n' + DRIVER_FILTER + TO_DIST +
                  '  |> map(fn: (r) => ({r with _time: date.truncate(t: r._time, unit: 1d)}))\n'
                  '  |> group(columns: ["_time", "mode"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["_time"], columnKey: ["mode"], valueColumn: "_value")\n'
                  '  |> sort(columns: ["_time"])\n'
                  '  |> map(fn: (r) => ({r with day: dayLabel(t: r._time)}))\n'
                  '  |> drop(columns: ["_time"])')],
          fieldConfig={"defaults": {"unit": DIST, "decimals": 1, "custom": {"fillOpacity": 90, "lineWidth": 0}},
                       "overrides": mode_overrides("name")},
          options={"xField": "day", "stacking": "normal", "orientation": "vertical", "showValue": "never", "barWidth": 0.8,
                   "legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}),
    panel(2, f"{DIST_WORD} by mode", "piechart", 16, 8, 8, 14,
          # one row, a column per mode (like the bar chart), so the mode colour overrides match by name
          [target(per_mode + '  |> sum()\n  |> group()\n  |> map(fn: (r) => ({mode: r.mode, _value: r._value, row: 0}))\n'
                  '  |> pivot(rowKey: ["row"], columnKey: ["mode"], valueColumn: "_value")\n  |> drop(columns: ["row"])')],
          fieldConfig={"defaults": {"unit": DIST, "decimals": 1}, "overrides": mode_overrides("name")},
          options={"pieType": "pie", "legend": {"displayMode": "table", "placement": "bottom", "values": ["value", "percent"]},
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}),
    panel(3, f"{DIST_WORD} by driver", "barchart", 0, 15, 10, 7,
          [target(src("mode_summary", ' and r._field == "miles"') + DRIVER_FILTER + TO_DIST +
                  '  |> group(columns: ["driver", "mode"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["driver"], columnKey: ["mode"], valueColumn: "_value")')],
          fieldConfig={"defaults": {"unit": DIST, "decimals": 1}, "overrides": mode_overrides("name")},
          options={"orientation": "horizontal", "stacking": "normal", "xField": "driver", "showValue": "never",
                   "legend": {"displayMode": "list", "placement": "bottom"}}),
    # the wheel's capacitive touch sensor, from rlogs only (ct.rlog: hands_summary), a group of bars per driver
    panel(42, "Hands on wheel by driver", "barchart", 10, 15, 6, 7,
          [target('import "math"\n' + src("hands_summary") + DRIVER_FILTER +
                  '  |> group(columns: ["driver", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["driver"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> sort(columns: ["drive_s"], desc: true)\n'
                  # the hours it's based on go in the driver's label: one axis, percentages only
                  '  |> map(fn: (r) => ({driver: r.driver + " (" + string(v: math.round(x: r.drive_s / 360.0) / 10.0) + " h)",\n'
                  '      hands: if r.drive_s > 0.0 then 100.0 * r.hands_s / r.drive_s else 0.0,\n'
                  '      op: if r.drive_s > 0.0 then 100.0 * r.steer_s / r.drive_s else 0.0,\n'
                  '      op_hands: if r.steer_s > 0.0 then 100.0 * r.steer_hands_s / r.steer_s else 0.0}))')],
          description=f"Share of each driver's driving time over the last {OVERVIEW_DAYS} days with hands on the steering wheel, from the wheel's own touch "
                      "sensor.\n\n"
                      "- **h** after each name: hours driven (moving in a forward gear) on drives with full logs\n"
                      "- **Hands on**: share of those hours with hands on the wheel\n"
                      "- **openpilot steered**: share of those hours openpilot was steering (full, MADS or steer + "
                      "stock ACC)\n"
                      "- **Hands on while openpilot steered**: share of the openpilot time with hands on the wheel; "
                      "the rest was hands-off\n\n"
                      "Only drives with full logs (rlogs, copied off the comma on home Wi-Fi) have the touch sensor, so "
                      "drives without them aren't counted. This is the car's capacitive sensor, not openpilot's "
                      "\"steering pressed\", which only notices you pushing on the wheel.",
          fieldConfig={"defaults": {"unit": "percent", "decimals": 0, "min": 0, "max": 100, "noValue": "no full logs",
                                    # values are printed on the bars; the 0-100% axis only crowded the narrow panel
                                    "custom": {"fillOpacity": 90, "lineWidth": 0, "axisPlacement": "hidden"}},
                       # magenta / amber / teal: hues the mode colours don't use, so nothing reads as MADS or ACC;
                       # checked for colour-blind separation and contrast on Grafana's dark theme
                       "overrides": [{"matcher": {"id": "byName", "options": n},
                                      "properties": [{"id": "displayName", "value": label},
                                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": colour}}]}
                                     for n, label, colour in (
                                       ("hands", "Hands on", "#d55181"),
                                       ("op", "openpilot steered", "#c98500"),
                                       ("op_hands", "Hands on while openpilot steered", "#199e70"))] +
                                    # the driver axis stays: it carries the name and hours
                                    [{"matcher": {"id": "byName", "options": "driver"},
                                      "properties": [{"id": "custom.axisPlacement", "value": "auto"}]}]},
          options={"orientation": "horizontal", "xField": "driver", "stacking": "none", "showValue": "always",
                   "groupWidth": 0.8, "barWidth": 0.9, "legend": {"displayMode": "list", "placement": "bottom"},
                   "tooltip": {"mode": "multi"}}),
    # a typical day of the week: each weekday's distance over every such day since the first drive (0 when not driven)
    panel(38, f"Average {DIST_WORD.lower()} by day of the week (all drives)", "barchart", 0, 22, 24, 6,
          [target('import "array"\nimport "date"\n' + TZ + DAY_LABEL +
                  f'data = from(bucket: "{BUCKET}")\n'
                  '  |> range(start: -10y)\n'
                  '  |> filter(fn: (r) => r._measurement == "mode_summary" and r._field == "miles")\n' + DRIVER_FILTER + TO_DIST +
                  'first = data |> group() |> min(column: "_time") |> findRecord(fn: (key) => true, idx: 0)\n'
                  'start = date.truncate(t: if exists first._time then first._time else now(), unit: 1d)\n'
                  'ndays = (int(v: date.truncate(t: now(), unit: 1d)) - int(v: start)) / int(v: 1d) + 1\n'
                  'count = (wd) => ndays / 7 + (if (wd - date.weekDay(t: start) + 7) % 7 < ndays % 7 then 1 else 0)\n'
                  'sums = data\n'
                  '  |> map(fn: (r) => ({wd: date.weekDay(t: r._time), mode: r.mode, _value: r._value}))\n'
                  '  |> group(columns: ["wd", "mode"])\n  |> sum()\n  |> group()\n'
                  # every weekday gets a bar, even one never driven on
                  'zeros = array.from(rows: [0, 1, 2, 3, 4, 5, 6] |> array.map(fn: (x) => ({wd: x, mode: "manual", _value: 0.0})))\n'
                  'union(tables: [sums, zeros])\n'
                  '  |> group(columns: ["wd", "mode"])\n  |> sum()\n  |> group()\n'
                  '  |> map(fn: (r) => ({r with _value: r._value / float(v: count(wd: r.wd))}))\n'
                  '  |> pivot(rowKey: ["wd"], columnKey: ["mode"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({r with order: (r.wd + 6) % 7, day: days[r.wd], days_counted: count(wd: r.wd)}))\n'
                  '  |> sort(columns: ["order"])\n  |> drop(columns: ["wd", "order"])')],
          description=f"How far you usually drive on each day of the week, split by mode.\n\n"
                      f"- each weekday's {DIST_WORD.lower()}, averaged over every one of that weekday since the first drive\n"
                      "- days you didn't drive count as 0\n"
                      f"- uses all drives, not the overview's {OVERVIEW_DAYS} days; the driver and model filters apply",
          fieldConfig={"defaults": {"unit": DIST, "decimals": 1, "custom": {"fillOpacity": 90, "lineWidth": 0}},
                       "overrides": mode_overrides("name") + [
                         {"matcher": {"id": "byName", "options": "days_counted"},
                          "properties": [{"id": "displayName", "value": "days averaged"}, {"id": "unit", "value": "none"},
                                         {"id": "decimals", "value": 0},
                                         {"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}}]}]},
          options={"xField": "day", "stacking": "normal", "orientation": "vertical", "showValue": "never", "barWidth": 0.6,
                   "legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}),
    panel(4, "Drives — click a route to zoom in", "table", 0, 28, 24, 8,
          [target(src("route", ' and (r._field == "distance_mi" or r._field == "duration_min")') + DRIVER_FILTER +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> map(fn: (r) => ({start: r._time, route: r.route, driver: r.driver,\n'
                  f'      miles: r.distance_mi * {MI_TO_DIST}, minutes: r.duration_min,\n'
                  '      from_ms: uint(v: r._time) / uint(v: 1000000) - uint(v: 60000),\n'
                  '      to_ms: uint(v: r._time) / uint(v: 1000000) + uint(v: r.duration_min * 60000.0) + uint(v: 60000)}))\n'
                  '  |> sort(columns: ["start"], desc: true)', "A"),
           target(src("mode_summary", ' and r._field == "miles"') + DRIVER_FILTER + TO_DIST +
                  '  |> group(columns: ["route", "mode"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["mode"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => {\n'
                  '      full = if exists r.full then r.full else 0.0\n'
                  '      mads = if exists r.mads then r.mads else 0.0\n'
                  '      sacc = if exists r.steer_stock_acc then r.steer_stock_acc else 0.0\n'
                  '      acc = if exists r.acc_only then r.acc_only else 0.0\n'
                  '      man = if exists r.manual then r.manual else 0.0\n'
                  '      tot = full + mads + sacc + acc + man\n'
                  '      return {route: r.route, engaged_pct: if tot > 0.0 then 100.0 * (full + mads + sacc) / tot else 0.0}\n'
                  '  })', "B"),
           target(src("braking_summary", ' and (r._field == "regen_s" or r._field == "friction_s")') + DRIVER_FILTER +
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({route: r.route, regen_pct: if r.regen_s + r.friction_s > 0.0 then 100.0 * r.regen_s / (r.regen_s + r.friction_s) else 0.0}))',
                  "C"),
           target(src("fault", ' and r._field == "duration_s"') + DRIVER_FILTER +
                  '  |> group(columns: ["route"])\n  |> count()\n  |> group()\n  |> map(fn: (r) => ({route: r.route, errors: r._value}))', "D"),
           target(src("vision_summary", ' and (r._field == "lane_sum" or r._field == "lane_n")') + DRIVER_FILTER +
                  '  |> map(fn: (r) => ({r with _value: float(v: r._value)}))\n'
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({route: r.route, lane_pct: if r.lane_n > 0.0 then 100.0 * r.lane_sum / r.lane_n else 0.0}))', "F"),
           target(src("engagement", ' and r.event == "start" and r._field == "speed_mph"') + DRIVER_FILTER +
                  '  |> group(columns: ["route"])\n  |> count()\n  |> group()\n  |> map(fn: (r) => ({route: r.route, engagements: r._value}))', "G"),
           target(src("fault", ' and r.fault == "engage_dropped" and r._field == "duration_s"') + DRIVER_FILTER +
                  '  |> group(columns: ["route"])\n  |> count()\n  |> group()\n  |> map(fn: (r) => ({route: r.route, didnt_stick: r._value}))', "H"),
           target('import "math"\n' + src("steering_summary", ' and (r._field == "err_sq_sum" or r._field == "n")') + DRIVER_FILTER +
                  '  |> map(fn: (r) => ({r with _value: float(v: r._value)}))\n'
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({route: r.route, steer_err: if r.n > 0.0 then math.sqrt(x: r.err_sq_sum / r.n) else 0.0}))', "I"),
           target(src("lead_summary", ' and (r._field == "gap_sum" or r._field == "n")') + DRIVER_FILTER +
                  '  |> map(fn: (r) => ({r with _value: float(v: r._value)}))\n'
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({route: r.route, follow_gap: if r.n > 0.0 then r.gap_sum / r.n else 0.0}))', "J"),
           target(src("dm_summary", ' and (r._field == "steered_s" or r._field == "distracted_s" or r._field == "warnings")') + DRIVER_FILTER +
                  '  |> map(fn: (r) => ({r with _value: float(v: r._value)}))\n'
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => ({route: r.route, dm_warnings: r.warnings,\n'
                  '      distracted_pct: if r.steered_s > 0.0 then 100.0 * r.distracted_s / r.steered_s else 0.0}))', "K"),
           target(src("collision_alert", ' and r._field == "text"') + DRIVER_FILTER +
                  '  |> group(columns: ["route"])\n  |> count()\n  |> group()\n  |> map(fn: (r) => ({route: r.route, collisions: r._value}))', "L")],
          transformations=[{"id": "merge", "options": {}},
                           {"id": "organize", "options": {"excludeByName": {"from_ms": True, "to_ms": True},
                                                          "indexByName": {"start": 0, "route": 1, "driver": 2, "miles": 3, "minutes": 4,
                                                                          "engaged_pct": 5, "errors": 6, "collisions": 7, "engagements": 8,
                                                                          "didnt_stick": 9, "distracted_pct": 10, "dm_warnings": 11, "regen_pct": 12,
                                                                          "lane_pct": 13, "steer_err": 14, "follow_gap": 15}}},
                           {"id": "sortBy", "options": {"sort": [{"field": "start", "desc": True}]}}],
          # number columns get fixed narrow widths so the table fits; the rest share what's left
          fieldConfig={"defaults": {"decimals": 1},
                       "overrides": [
                         START_COLUMN, ROUTE_LINK,
                         # sized to their headers; Engaged takes what's left
                         *fixed_width(65, "errors", "didnt_stick", "regen_pct", "collisions"), *fixed_width(75, "miles", "minutes", "engagements"),
                         *fixed_width(90, "distracted_pct", "dm_warnings"),
                         *fixed_width(85, "lane_pct"), *fixed_width(90, "driver", "follow_gap"), *fixed_width(95, "steer_err"),
                         {"matcher": {"id": "byName", "options": "driver"}, "properties": [{"id": "displayName", "value": "Driver"}]},
                         {"matcher": {"id": "byName", "options": "minutes"}, "properties": [{"id": "displayName", "value": "Minutes"}]},
                         {"matcher": {"id": "byName", "options": "engaged_pct"},
                          "properties": [{"id": "unit", "value": "percent"}, {"id": "displayName", "value": "Engaged"},
                                         {"id": "description", "value": "Share of the distance openpilot steered (full, MADS or steer + stock ACC). "
                                                                         "Distance by mode is on the Overview charts."},
                                         {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic"}},
                                         {"id": "min", "value": 0}, {"id": "max", "value": 100},
                                         # one neutral colour: no % is "good" or "bad"
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}}]},
                         {"matcher": {"id": "byName", "options": "errors"},
                          "properties": [{"id": "displayName", "value": "Errors"}, {"id": "decimals", "value": 0}, {"id": "noValue", "value": "0"},
                                         {"id": "description", "value": "Steering/EPS/CAN errors and openpilot system alerts on this drive (see the Errors table)"},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                         {"id": "color", "value": {"mode": "thresholds"}},
                                         {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                           {"color": "transparent", "value": None}, {"color": "#C4162A", "value": 1}]}}]},
                         {"matcher": {"id": "byName", "options": "collisions"},
                          "properties": [{"id": "displayName", "value": "FCW"}, {"id": "decimals", "value": 0}, {"id": "noValue", "value": "0"},
                                         {"id": "description", "value": "Collision alerts on this drive: openpilot's forward collision warning "
                                                                         "(BRAKE! Risk of Collision) and the car's own FCW / emergency braking"},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                         {"id": "color", "value": {"mode": "thresholds"}},
                                         {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                           {"color": "transparent", "value": None}, {"color": ALERT_COLOUR, "value": 1}]}}]},
                         {"matcher": {"id": "byName", "options": "engagements"},
                          "properties": [{"id": "displayName", "value": "Engages"}, {"id": "decimals", "value": 0}, {"id": "noValue", "value": "0"},
                                         {"id": "description", "value": "Times openpilot was engaged on this drive"}]},
                         {"matcher": {"id": "byName", "options": "didnt_stick"},
                          "properties": [{"id": "displayName", "value": "Short"}, {"id": "decimals", "value": 0}, {"id": "noValue", "value": "0"},
                                         {"id": "description", "value": f"Engagements that ended within {ENGAGE_SHORT_S:.0f} s without you ending them (the car dropped "
                                                                         "ACC, openpilot cancelled, or an alert). Known only on drives copied off the device."},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                         {"id": "color", "value": {"mode": "thresholds"}},
                                         {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                           {"color": "transparent", "value": None}, {"color": FAULT_COLOURS[FAULTS["engage_dropped"][0]], "value": 1}]}}]},
                         {"matcher": {"id": "byName", "options": "distracted_pct"},
                          "properties": [{"id": "displayName", "value": "Distracted"}, {"id": "unit", "value": "percent"},
                                         {"id": "decimals", "value": 0}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Share of the time openpilot steered (while moving) that the driver camera judged you distracted "
                                                                         "(looking away or eyes off the road). See Driver attention for the drive."}]},
                         {"matcher": {"id": "byName", "options": "dm_warnings"},
                          "properties": [{"id": "displayName", "value": "DM Warnings"}, {"id": "decimals", "value": 0}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Driver-monitoring warnings on this drive (openpilot telling you to pay attention), any level"},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                         {"id": "color", "value": {"mode": "thresholds"}},
                                         {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                           {"color": "transparent", "value": None}, {"color": "#FF9830", "value": 1}]}}]},
                         {"matcher": {"id": "byName", "options": "steer_err"},
                          "properties": [{"id": "displayName", "value": "Steer Error"}, {"id": "unit", "value": "accMS2"},
                                         {"id": "decimals", "value": 2}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "How far the car's lateral acceleration was from what openpilot asked for while it steered "
                                                                         "(RMS, m/s²). Lower = it held the line more closely."}]},
                         {"matcher": {"id": "byName", "options": "follow_gap"},
                          "properties": [{"id": "displayName", "value": "Follow Gap"}, {"id": "unit", "value": "s"},
                                         {"id": "decimals", "value": 1}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Average time gap to the car ahead while openpilot controlled the speed."}]},
                         {"matcher": {"id": "byName", "options": "lane_pct"},
                          "properties": [{"id": "displayName", "value": "Lane Conf."}, {"id": "unit", "value": "percent"},
                                         {"id": "decimals", "value": 0}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Average lane confidence while moving: how sure the driving model was that it saw the lane lines. Fog, rain, glare, a dirty windshield, "
                                                                         "or roads without clear paint pull it down"}]},
                         {"matcher": {"id": "byName", "options": "regen_pct"},
                          "properties": [{"id": "unit", "value": "percent"}, {"id": "displayName", "value": "Regen"},
                                         {"id": "decimals", "value": 0}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Share of braking done by regen rather than the friction brakes (drives copied off the device)"}]},
                         {"matcher": {"id": "byName", "options": "miles"}, "properties": [{"id": "displayName", "value": "Distance"}, {"id": "unit", "value": DIST}]}]},
          options={"showHeader": True, "cellHeight": "sm"}),

    # the software each drive ran (it rarely changes, so it's kept out of the Drives table)
    panel(33, "Software by drive", "table", 0, 36, 24, 6,
          [target(src("route", ' and (r._field == "duration_min" or r._field == "commit_date")') + DRIVER_FILTER +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> map(fn: (r) => ({start: r._time, route: r.route, version: r.version, branch: r.branch,\n'
                  '      commit: r.commit + " (" + (if exists r.commit_date then r.commit_date else "?") + ")", os: r.os,\n'
                  '      from_ms: uint(v: r._time) / uint(v: 1000000) - uint(v: 60000),\n'
                  '      to_ms: uint(v: r._time) / uint(v: 1000000) + uint(v: r.duration_min * 60000.0) + uint(v: 60000)}))\n'
                  '  |> sort(columns: ["start"], desc: true)', "A"),
           # driving model: names from the route, time on each from model_summary (strings, so the two can be unioned)
           target('secs = ' + src("model_summary", ' and (r._field == "big_s" or r._field == "small_s")').lstrip() + DRIVER_FILTER +
                  '  |> group(columns: ["route", "_field"])\n  |> sum()\n'
                  '  |> map(fn: (r) => ({route: r.route, _field: r._field, _value: string(v: r._value)}))\n'
                  'names = ' + src("route", ' and (r._field == "model_small" or r._field == "model_big")').lstrip() + DRIVER_FILTER +
                  '  |> group(columns: ["route", "_field"])\n  |> last()\n'
                  '  |> map(fn: (r) => ({route: r.route, _field: r._field, _value: r._value}))\n'
                  'union(tables: [secs, names])\n  |> group()\n'
                  '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> map(fn: (r) => {\n'
                  '      big = if exists r.big_s then float(v: r.big_s) else 0.0\n'
                  '      small = if exists r.small_s then float(v: r.small_s) else 0.0\n'
                  '      bn = if exists r.model_big and r.model_big != "" then r.model_big else "big model"\n'
                  '      sn = if exists r.model_small and r.model_small != "" then r.model_small else "–"\n'
                  '      return {route: r.route, model: if big < 1.0 then sn else if small < 5.0 then bn + " (big)"\n'
                  '              else bn + " (big) " + string(v: int(v: 100.0 * big / (big + small))) + "%, then " + sn}\n'
                  '  })', "B")],
          transformations=[{"id": "merge", "options": {}},
                           {"id": "organize", "options": {"excludeByName": {"from_ms": True, "to_ms": True},
                                                          "indexByName": {"start": 0, "route": 1, "model": 2, "version": 3, "branch": 4,
                                                                          "commit": 5, "os": 6}}},
                           {"id": "sortBy", "options": {"sort": [{"field": "start", "desc": True}]}}],
          fieldConfig={"defaults": {},
                       "overrides": [
                         START_COLUMN, ROUTE_LINK,
                         {"matcher": {"id": "byName", "options": "model"},
                          "properties": [{"id": "displayName", "value": "Model"}, {"id": "noValue", "value": "–"},
                                         {"id": "description", "value": "Driving model. (big) = the big model on a Chestnut or Jetlink host; a % means it fell back to the device's own model partway"}]},
                         {"matcher": {"id": "byName", "options": "version"}, "properties": [{"id": "displayName", "value": "Version"}]},
                         {"matcher": {"id": "byName", "options": "branch"}, "properties": [{"id": "displayName", "value": "Branch"}]},
                         {"matcher": {"id": "byName", "options": "commit"}, "properties": [{"id": "displayName", "value": "Commit"}]},
                         {"matcher": {"id": "byName", "options": "os"}, "properties": [{"id": "displayName", "value": "AGNOS"}]}]},
          options={"showHeader": True, "cellHeight": "sm"}),

    panel(21, "Errors — click a route to see it", "table", 0, 158, 24, 4,
          [target(FAULT_LABELS + src("fault", ' and (r._field == "duration_s" or r._field == "frames" or r._field == "end_ns")') + DRIVER_FILTER +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> map(fn: (r) => ({time: r._time, route: r.route, driver: r.driver,\n'
                  '      error: ' + FAULT_LABEL + ', seconds: r.duration_s, frames: r.frames,\n'
                  '      from_ms: uint(v: r._time) / uint(v: 1000000) - uint(v: 60000), to_ms: uint(v: r.end_ns) / uint(v: 1000000) + uint(v: 60000)}))\n'
                  '  |> sort(columns: ["time"], desc: true)')],
          transformations=[{"id": "organize", "options": {"excludeByName": {"from_ms": True, "to_ms": True},
                                                          "indexByName": {"time": 0, "error": 1, "seconds": 2, "frames": 3, "route": 4, "driver": 5}}}],
          fieldConfig={"defaults": {},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "route"},
                          "properties": [{"id": "links", "value": [{"title": "Show this error (±1 min)",
                            "url": "/d/" + DASH_UID + "/comma-drives?var-route=${__value.raw}&${driver:queryparam}&from=${__data.fields.from_ms}&to=${__data.fields.to_ms}"}]}]},
                         {"matcher": {"id": "byName", "options": "error"},
                          "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                         {"id": "mappings", "value": [{"type": "value", "options": {
                                           label: {"color": FAULT_COLOURS[code]} for code, label, *_ in FAULTS.values()}},
                                           {"type": "regex", "options": {"pattern": "^(" + FAULTS["op_alert"][1] + ": .+)$",
                                                                         "result": {"text": "$1", "color": FAULT_COLOURS[FAULTS["op_alert"][0]]}}}]}]},
                         {"matcher": {"id": "byName", "options": "seconds"},
                          "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 1}]},
                         {"matcher": {"id": "byName", "options": "frames"},
                          "properties": [{"id": "description", "value": "CAN frames: blocked by the panda, or EPS status frames reporting the fault (openpilot alerts: alert messages)"}]}]},
          options={"showHeader": True, "cellHeight": "sm"}),

    # the selected drive: how long from the car coming on until openpilot could drive. One stacked bar, in the order
    # it happens: openpilot starting, the wait for the first driving-model output (coloured by which model it was:
    # nothing drives while the big one loads), then waiting on the car (gear, door, seatbelt...) until openpilot first
    # said it could engage. Any time range: the drive picks the records
    panel(22, "Time to ready (car on → can engage)", "barchart", 0, 59, 24, 4,
          [target('engage = ' + src("engage_ready", ' and r.route == "${route}" and r._field == "since_start_s"')
                  .replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y").lstrip() +
                  '  |> group()\n  |> min()\n  |> findRecord(fn: (key) => true, idx: 0)\n' +
                  src("startup", ' and r.route == "${route}" and (r._field == "openpilot_s" or r._field == "model_s" or r._field == "model"'
                              ' or r._field == "model_name" or r._field == "big_failed")')
                  .replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y") +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n  |> last(column: "openpilot_s")\n'
                  '  |> map(fn: (r) => {\n'
                  '      wait = if exists r.model_s then r.model_s - r.openpilot_s else 0.0\n'
                  '      ready = if exists r.model_s then r.model_s else r.openpilot_s\n'
                  '      car = if exists engage._value and engage._value > ready then engage._value - ready else 0.0\n'
                  '      big = exists r.model and r.model == "big"\n'
                  '      name = if exists r.model_name and r.model_name != "" then r.model_name else if big then "big model" else "device model"\n'
                  '      return {model: name + (if exists r.big_failed and r.big_failed == 1 then " (big model failed)" else ""),\n'
                  '              openpilot: r.openpilot_s, big: if big then wait else 0.0, device: if big then 0.0 else wait, car: car}\n'
                  '  })')],
          transformations=[{"id": "organize", "options": {"indexByName": {"model": 0, "openpilot": 1, "device": 2, "big": 3, "car": 4}}}],
          description="This drive, from the car coming on (the comma starting to log) until it could engage:\n\n"
                      "- **grey**: openpilot starting up\n"
                      "- **purple / orange**: waiting for the first driving-model output, from the big model on a Chestnut or Jetlink host (purple) or the "
                      "device's own (orange, also after a failed big-model load). Nothing drives while the big model loads.\n"
                      "- **yellow**: waiting on the car (in park or reverse, door open, seatbelt off...) until openpilot first said it "
                      "could be engaged",
          fieldConfig={"defaults": {"unit": "s", "decimals": 1, "min": 0, "noValue": "no startup in this drive's logs",
                                    "custom": {"fillOpacity": 85, "lineWidth": 0}},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "openpilot"},
                          "properties": [{"id": "displayName", "value": "openpilot starting"}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#8e8e8e"}}]},
                         {"matcher": {"id": "byName", "options": "big"},
                          "properties": [{"id": "displayName", "value": "big model (Chestnut/Jetlink)"}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#B877D9"}},
                                         {"id": "noValue", "value": ""}]},
                         {"matcher": {"id": "byName", "options": "device"},
                          "properties": [{"id": "displayName", "value": "device model"}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}}]},
                         {"matcher": {"id": "byName", "options": "car"},
                          "properties": [{"id": "displayName", "value": "waiting on the car"}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FADE2A"}}]}]},
          options={"xField": "model", "stacking": "normal", "orientation": "horizontal", "showValue": "always", "barWidth": 0.6,
                   "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
                   "tooltip": {"mode": "multi", "sort": "none"}}),

    {"id": 101, "type": "row", "title": "${route:text}", "gridPos": {"x": 0, "y": 42, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    # the comma route name (what comma's tools take) with a copy button; the button needs GF_PANELS_DISABLE_SANITIZE_HTML,
    # without it just the link shows. :raw for the id (plain ${route} gives the label in some places)
    panel(32, "", "text", 0, 43, 24, 2, [], transparent=True,
          options={"mode": "html", "content":
                   '<div style="display: flex; align-items: center; gap: 12px; height: 100%">'
                   f'<a href="https://connect.comma.ai/{ENV["DONGLE_ID"]}/${{route:raw}}" target="_blank" title="Open this drive in comma connect" '
                   f'style="font-family: monospace">{ENV["DONGLE_ID"]}/${{route:raw}}</a>'
                   '<button title="Copy the route name, e.g. for a bug report" style="padding: 2px 10px; border: 1px solid currentColor; '
                   'border-radius: 4px; background: transparent; color: inherit; cursor: pointer" onclick="'
                   f"const t = '{ENV['DONGLE_ID']}/${{route:raw}}', b = this, done = () => {{ b.textContent = 'Copied'; setTimeout(() => b.textContent = 'Copy', 1500) }};"
                   # the clipboard API is https/localhost only; execCommand covers plain http on the LAN
                   " if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(t).then(done);"
                   " else { const a = document.createElement('textarea'); a.value = t; document.body.appendChild(a); a.select();"
                   " document.execCommand('copy'); a.remove(); done() }"
                   '">Copy</button></div>'}),
    panel(5, "Mode timeline", "state-timeline", 0, 63, 18, 7,
          [target(src("drive", ' and r.route == "${route}" and (r._field == "mode" or r._field == "lat_ctrl" or r._field == "steering_pressed"'
                                ' or r._field == "gas_pressed" or r._field == "brake_pressed")') +
                  '  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\n  |> keep(columns: ["_time", "_value", "_field"])'),
           # events as a bar row: name while it fires (prefixed when openpilot wasn't engaged), empty otherwise
           target(src("events", ' and r.route == "${route}" and r._field == "count"') +
                  '  |> map(fn: (r) => ({r with _value: (if r.engaged == "1" then "" else "(manual) ") + r.event, _field: "events"}))\n'
                  '  |> group(columns: ["_field"])\n'
                  '  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: true)\n'
                  '  |> fill(value: "")\n  |> keep(columns: ["_time", "_value", "_field"])', "B"),
           # braking source from the rlogs' CAN (only drives copied off the device have it)
           target(src("braking", ' and r.route == "${route}" and r._field == "decel_code"') +
                  '  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)\n'
                  '  |> map(fn: (r) => ({r with _field: "braking"}))\n  |> keep(columns: ["_time", "_value", "_field"])', "C"),
           # worst fault active in each window. Faults are stored as start (code) / end (0) points: per window take the
           # peak (catches faults shorter than a window) and the state carried in from before
           target('import "date"\n' + FAULT_RANK +
                  f'f = from(bucket: "{BUCKET}")\n  |> range(start: date.sub(d: 1h, from: v.timeRangeStart), stop: v.timeRangeStop)\n'
                  '  |> filter(fn: (r) => r._measurement == "fault" and r.route == "${route}" and r._field == "code")\n'
                  '  |> group(columns: ["fault", "event"])\n'
                  '  |> map(fn: (r) => ({r with _value: rank(c: r._value)}))\n'
                  'peak = f |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)\n'
                  'state = f |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: true) |> fill(usePrevious: true) |> fill(value: 0)\n'
                  'union(tables: [peak, state, state |> timeShift(duration: v.windowPeriod)])\n'
                  '  |> filter(fn: (r) => r._time >= v.timeRangeStart and r._time <= v.timeRangeStop)\n'
                  '  |> group(columns: ["_time"])\n  |> max()\n  |> group()\n  |> sort(columns: ["_time"])\n'
                  # _field back in the group key, or Grafana gets a plain table instead of a series named "errors"
                  '  |> map(fn: (r) => ({_time: r._time, _value: r._value, _field: "errors"}))\n  |> group(columns: ["_field"])', "D"),
           # which driving model ran, named from the route's model params
           target('names = ' + src("route", ' and r.route == "${route}" and (r._field == "model_small" or r._field == "model_big")')
                  .replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y").lstrip() +
                  '  |> last()\n  |> group()\n  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                  '  |> findRecord(fn: (key) => true, idx: 0)\n'
                  'small = if exists names.model_small and names.model_small != "" then names.model_small else "device model"\n'
                  'big = if exists names.model_big and names.model_big != "" then names.model_big else "big model"\n' +
                  # from the qlog, and from the rlog where it was copied: the rlog's modelV2 knows a Jetlink big model the
                  # qlog can't see. Each window takes the higher of the two, so big from either wins and loading still shows
                  'qlog = ' + src("drive", ' and r.route == "${route}" and r._field == "model_code"').lstrip() +
                  '  |> keep(columns: ["_start", "_stop", "_time", "_value"])\n  |> group()\n  |> sort(columns: ["_time"])\n'
                  '  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\n'
                  'rlog = ' + src("model_rlog", ' and r.route == "${route}" and r._field == "model_code"').lstrip() +
                  '  |> keep(columns: ["_start", "_stop", "_time", "_value"])\n  |> group()\n  |> sort(columns: ["_time"])\n'
                  '  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\n'
                  'union(tables: [qlog, rlog])\n  |> group(columns: ["_time"])\n  |> max()\n  |> group()\n  |> sort(columns: ["_time"])\n'
                  '  |> map(fn: (r) => ({_time: r._time, _field: "model",\n'
                  '      _value: if r._value == 2 then big + " (big)" else if r._value == 1 then "loading big model" else small + " (small)"}))\n'
                  '  |> group(columns: ["_field"])', "E"),
           # lane-line confidence while moving, as a colour strip; windows of at least 5 s so it doesn't flicker, empty
           # windows (stopped, or no data) stay blank
           target('every = if int(v: v.windowPeriod) < int(v: 5s) then 5s else v.windowPeriod\n' +
                  src("vision", ' and r.route == "${route}" and r._field == "lane_moving"') +
                  '  |> aggregateWindow(every: every, fn: mean, createEmpty: true)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r._value, _field: "lane_lines"}))', "F"),
           # the driver-monitoring camera's verdict: each window's worst state, so short warnings still show
           target(src("driver_monitor", ' and r.route == "${route}" and r._field == "code"') +
                  '  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r._value, _field: "attention"}))', "G"),
           # the acceleration the driving model wants, each window's mean; windows of at least 1 s (the logs have it at
           # 2 Hz) so it doesn't stripe, windows without it stay blank
           target('every = if int(v: v.windowPeriod) < int(v: 1s) then 1s else v.windowPeriod\n' +
                  src("vision", ' and r.route == "${route}" and r._field == "model_accel"') +
                  '  |> aggregateWindow(every: every, fn: mean, createEmpty: true)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r._value, _field: "long_accel"}))', "H")],
          maxDataPoints=2000,
          # rows come back alphabetically; join them on time and order them: model, mode, attention, steering control and
          # override, accelerator, model intent, brake
          transformations=[{"id": "joinByField", "options": {"byField": "Time", "mode": "outer"}},
                           {"id": "organize", "options": {"indexByName": {"Time": 0, "model": 1, "mode": 2, "attention": 3, "lane_lines": 4,
                                                                          "lat_ctrl": 5, "steering_pressed": 6, "gas_pressed": 7, "long_accel": 8,
                                                                          "brake_pressed": 9, "braking": 10, "errors": 11, "events": 12}}}],
          fieldConfig={"defaults": {"mappings": mode_text_mappings(), "color": {"mode": "fixed", "fixedColor": "transparent"}},
                       # driver-input rows each get their own colour, distinct from the mode colours and the red event lines
                       "overrides": [{"matcher": {"id": "byName", "options": f},
                                      "properties": [{"id": "displayName", "value": label},
                                                     {"id": "mappings", "value": [{"type": "value", "options": {
                                                       "1": {"text": "on", "color": colour}, "0": {"text": "off", "color": "transparent"}}}]}]}
                                     for f, label, colour in (("steering_pressed", "steering (override)", "#FADE2A"),
                                                              ("gas_pressed", "accelerator (override)", "#5CC8C8"),
                                                              ("brake_pressed", "brake (override)", "#E03ED8"))]
                                    # torque blue / angle green, as Adventure Pilot tints the wheel on the device
                                    + [{"matcher": {"id": "byName", "options": "lat_ctrl"},
                                        "properties": [{"id": "displayName", "value": "steering control"},
                                                       {"id": "mappings", "value": [{"type": "value", "options": {
                                                         "0": {"text": " ", "color": "transparent"},
                                                         "1": {"text": "torque", "color": "#4D9DFF"},
                                                         "2": {"text": "angle", "color": "#3ADB6D"},
                                                         "3": {"text": "PID", "color": "#B0BEC5"},
                                                         "4": {"text": "curvature", "color": "#B0BEC5"},
                                                         "5": {"text": "debug", "color": "#B0BEC5"}}}]}]}]
                                    + [{"matcher": {"id": "byName", "options": "braking"},
                                        "properties": [{"id": "displayName", "value": "braking (CAN)"},
                                                       {"id": "mappings", "value": [{"type": "value", "options": {
                                                         "0": {"text": " ", "color": "transparent"},
                                                         "1": {"text": "hold (stopped)", "color": "#5A5A5A"},
                                                         "2": {"text": "regen", "color": "#B877D9"},
                                                         "3": {"text": "friction brakes", "color": "#FF780A"}}}]}]}]
                                    + [{"matcher": {"id": "byName", "options": "errors"},
                                        "properties": [{"id": "displayName", "value": "errors"}, {"id": "mappings", "value": fault_mappings()}]}]
                                    + [{"matcher": {"id": "byName", "options": "attention"},
                                        "properties": [{"id": "displayName", "value": "driver attention"},
                                                       {"id": "mappings", "value": [{"type": "value", "options": {
                                                         str(code): {"text": label, "color": colour} for code, (label, colour) in ATTENTION_COLOURS.items()}}]}]}]
                                    + [{"matcher": {"id": "byName", "options": "lane_lines"},
                                        "properties": [{"id": "displayName", "value": "lane confidence"}, {"id": "mappings", "value": []},
                                                       {"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 0},
                                                       {"id": "min", "value": 0}, {"id": "max", "value": 1},
                                                       {"id": "color", "value": {"mode": "continuous-RdYlGr"}}]}]
                                    # red (braking) / yellow (holding) / green (speeding up), the hues the device tints its
                                    # path with in experimental mode (60 + 35 * accel, clamped to 0..120)
                                    + [{"matcher": {"id": "byName", "options": "long_accel"},
                                        "properties": [{"id": "displayName", "value": "model intent"}, {"id": "mappings", "value": []},
                                                       {"id": "unit", "value": "accMS2"}, {"id": "decimals", "value": 1},
                                                       {"id": "min", "value": -1.7}, {"id": "max", "value": 1.7},
                                                       {"id": "color", "value": {"mode": "continuous-RdYlGr"}}]}]
                                    + [{"matcher": {"id": "byName", "options": "model"},
                                        "properties": [{"id": "displayName", "value": "model"},
                                                       {"id": "mappings", "value": [
                                                         {"type": "regex", "options": {"pattern": "^(.+ \\(big\\))$", "result": {"text": "$1", "color": "#8F3BB8"}}},
                                                         {"type": "regex", "options": {"pattern": "^(loading.*)$", "result": {"text": "$1", "color": "#F2CC0C"}}},
                                                         {"type": "regex", "options": {"pattern": "^(.+ \\(small\\))$", "result": {"text": "$1", "color": "#5A6E8C"}}}]}]}]
                                    + [{"matcher": {"id": "byName", "options": "events"},
                                        "properties": [{"id": "displayName", "value": "events"},
                                                       {"id": "mappings", "value": [
                                                         {"type": "value", "options": {"": {"text": " ", "color": "transparent"}}},
                                                         {"type": "regex", "options": {"pattern": "^\\(manual\\) (.+)$",
                                                                                       "result": {"text": "$1 (manual)", "color": "#8e8e8e"}}},
                                                         {"type": "regex", "options": {"pattern": "^(.+)$",
                                                                                       "result": {"text": "$1", "color": "red"}}}]}]}]},
          options={"showValue": "never", "mergeValues": True, "rowHeight": 0.8, "legend": {"showLegend": False}}),
    # one markers layer per mode (fixed colour each), so the legend names the modes; the layers fit the view together
    panel(6, "Route map (colour = mode)", "geomap", 0, 110, 24, 15,
          [target(src("drive", ' and r.route == "${route}" and (r._field == "lat" or r._field == "lon" or r._field == "mode_code"'
                                ' or r._field == "speed_mph")') +
                  '  |> aggregateWindow(every: 2s, fn: last, createEmpty: false)\n'
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
                  f'  |> filter(fn: (r) => exists r.lat and exists r.lon and r.mode_code == {code})\n  |> group()\n'
                  f'  |> map(fn: (r) => ({{_time: r._time, lat: r.lat, lon: r.lon, mode: "{label.split(" (")[0]}",'
                  f' speed: if exists r.speed_mph then r.speed_mph * {MI_TO_DIST} else 0.0}}))', mode)
           for mode, (c, label, code) in MODE_COLORS.items()],
          fieldConfig={"defaults": {}, "overrides": [{"matcher": {"id": "byName", "options": "speed"},
                                                      "properties": [{"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0}]}]},
          # fit to the drive; in fit mode zoom is the most it may zoom in (street level, so short drives fill the map)
          options={"view": {"id": "fit", "allLayers": True, "lat": 0, "lon": 0, "zoom": 18, "padding": 50},
                   "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True},
                   "basemap": MAP_BASE,
                   "layers": [{"type": "markers", "name": label.split(" (")[0], "tooltip": True, "filterData": {"id": "byRefId", "options": mode},
                               "location": {"mode": "coords", "latitude": "lat", "longitude": "lon"},
                               "config": {"showLegend": True,
                                          "style": {"size": {"fixed": 4}, "opacity": 0.9, "color": {"fixed": c},
                                                    "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"}}}}
                              for mode, (c, label, code) in MODE_COLORS.items()],
                   "tooltip": {"mode": "details"}}),
    panel(7, "Speed & steering angle", "timeseries", 0, 70, 18, 7,
          [target(src("drive", ' and r.route == "${route}" and (r._field == "speed_mph" or r._field == "steer_angle")') +
                  (f'  |> map(fn: (r) => ({{r with _value: if r._field == "speed_mph" then r._value * {MI_TO_DIST} else r._value}}))\n'
                   if METRIC else "") + DOWNSAMPLE),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")], maxDataPoints=2000,
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH // 2}},
                       "overrides": [{"matcher": {"id": "byName", "options": "speed_mph"},
                                      "properties": [{"id": "unit", "value": SPEED_UNIT}, {"id": "displayName", "value": "speed"}]},
                                     DISTANCE_TOOLTIP,
                                     {"matcher": {"id": "byName", "options": "steer_angle"},
                                      "properties": [{"id": "unit", "value": "degree"}, {"id": "displayName", "value": "steering angle"},
                                                     {"id": "custom.axisPlacement", "value": "left"}]}]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    panel(8, "Steering torque (driver / openpilot request / rack)", "timeseries", 0, 77, 18, 7,
          [target(src("drive", ' and r.route == "${route}" and (r._field == "driver_torque" or r._field == "op_torque")') + DOWNSAMPLE),
           # the EPS's own torque only where the car reports one (the Rivian port leaves it at 0)
           target('eps = ' + src("drive", ' and r.route == "${route}" and r._field == "eps_torque"').lstrip() +
                  'reported = eps |> filter(fn: (r) => r._value != 0.0) |> count() |> findRecord(fn: (key) => true, idx: 0)\n'
                  'eps\n  |> filter(fn: (r) => exists reported._value)\n' + DOWNSAMPLE, "C"),
           # the rack applying openpilot's torque (EPAS_SystemStatus H_CAN_EPSS_ToiActive), from the rlogs' CAN
           target(src("braking", ' and r.route == "${route}" and r._field == "rack_toi"') +
                  '  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\n'
                  '  |> keep(columns: ["_time", "_value", "_field"])', "D"),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")], maxDataPoints=2000,
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH // 2}},
                       "overrides": [{"matcher": {"id": "byName", "options": "op_torque"},
                                      "properties": [{"id": "custom.axisPlacement", "value": "left"},
                                                     {"id": "displayName", "value": "openpilot torque request (-1..1)"},
                                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}}]},
                                     # in the car's own units (carState steeringTorque / steeringTorqueEps)
                                     {"matcher": {"id": "byName", "options": "driver_torque"},
                                      "properties": [{"id": "displayName", "value": "driver torque"},
                                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": "#73BF69"}}]},
                                     {"matcher": {"id": "byName", "options": "eps_torque"},
                                      "properties": [{"id": "displayName", "value": "EPS torque"}]},
                                     {"matcher": {"id": "byName", "options": "rack_toi"},
                                      "properties": [{"id": "displayName", "value": "rack applying openpilot torque (1 = yes)"},
                                                     {"id": "custom.axisPlacement", "value": "left"},
                                                     {"id": "custom.lineInterpolation", "value": "stepAfter"},
                                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FADE2A"}}]},
                                     DISTANCE_TOOLTIP]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    panel(20, "Braking — regen vs friction (CAN, device-copied drives only)", "timeseries", 0, 132, 24, 9,
          [target(src("braking", ' and r.route == "${route}" and (r._field == "accel" or r._field == "brake_pressure"'
                                ' or r._field == "accel_pedal" or r._field == "brake_pedal_mm" or r._field == "op_accel_request")') + DOWNSAMPLE)],
          maxDataPoints=2000,
          description="- **Regen**: deceleration with no brake pressure\n"
                      "- **Friction brakes**: brake pressure while moving\n"
                      "- **Holding**: brake pressure while stopped\n\n"
                      "Motor torque isn't on the CAN buses the comma sees, so regen is inferred, not measured.",
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH // 2}},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "accel"},
                          "properties": [{"id": "displayName", "value": "acceleration (IMU)"}, {"id": "unit", "value": "accMS2"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#B877D9"}},
                                         {"id": "custom.fillOpacity", "value": 15}]},
                         {"matcher": {"id": "byName", "options": "op_accel_request"},
                          "properties": [{"id": "displayName", "value": "openpilot accel request"}, {"id": "unit", "value": "accMS2"},
                                         {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]},
                         *[{"matcher": {"id": "byName", "options": f},
                            "properties": [{"id": "displayName", "value": label}, {"id": "unit", "value": unit},
                                           # two visible axes like the other drive graphs (so the time axes line up);
                                           # the pedal positions keep their values in the tooltip
                                           {"id": "custom.axisPlacement", "value": "left" if f == "brake_pressure" else "hidden"},
                                           {"id": "color", "value": {"mode": "fixed", "fixedColor": colour}}]}
                           for f, label, unit, colour in (("accel_pedal", "accelerator pedal", "percent", "#5CC8C8"),
                                                          ("brake_pressure", "brake pressure", "pressurebar", "#FF780A"),
                                                          ("brake_pedal_mm", "brake pedal travel", "lengthmm", "#E03ED8"))]]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    # how well the cameras could see: lane confidence, vision-motion uncertainty, road-camera exposure
    panel(25, "Visibility (lane confidence · vision uncertainty · camera exposure)", "timeseries", 0, 91, 18, 6,
          [target(src("vision", ' and r.route == "${route}" and (r._field == "lane_left" or r._field == "lane_right"'
                                ' or r._field == "motion_std" or r._field == "exposure_pct")') +
                  # the 2 Hz values flicker; a 10 s trailing average reads better and keeps dips that last
                  '  |> timedMovingAverage(every: v.windowPeriod, period: 10s)\n  |> keep(columns: ["_time", "_value", "_field"])'),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")],
          maxDataPoints=2000, links=[{"title": "Zoom to this drive", "url": ZOOM_URL}],
          description="- **Lane confidence**: how sure the driving model is that it sees each lane line (fog, rain, glare and faded paint "
                      "pull it down)\n"
                      "- **Vision uncertainty**: how unsure the model is about the car's motion from the images (hover; its scale isn't shown)\n"
                      "- **Exposure**: how hard the road camera works for light (darker scenes = higher)",
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH // 2}},
                       "overrides": [
                         *[{"matcher": {"id": "byName", "options": f},
                            "properties": [{"id": "displayName", "value": label}, {"id": "unit", "value": "percentunit"},
                                           {"id": "min", "value": 0}, {"id": "max", "value": 1}, {"id": "custom.axisPlacement", "value": "left"},
                                           {"id": "color", "value": {"mode": "fixed", "fixedColor": colour}}]}
                           for f, label, colour in (("lane_left", "lane confidence (left)", "#73BF69"), ("lane_right", "lane confidence (right)", "#5794F2"))],
                         {"matcher": {"id": "byName", "options": "exposure_pct"},
                          "properties": [{"id": "displayName", "value": "road camera exposure"}, {"id": "unit", "value": "percent"},
                                         {"id": "custom.axisPlacement", "value": "left"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FADE2A"}}]},
                         {"matcher": {"id": "byName", "options": "motion_std"},
                          "properties": [{"id": "displayName", "value": "vision uncertainty"}, {"id": "decimals", "value": 3},
                                         {"id": "custom.axisPlacement", "value": "hidden"},
                                         {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [4, 4]}},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}}]},
                         DISTANCE_TOOLTIP]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    # the driver-monitoring camera: how much attention openpilot credits the driver with, when it saw them distracted,
    # and its warnings (level 1-3; 3 = it will slow down and lock you out after repeats)
    panel(34, "Driver attention (driver monitoring)", "timeseries", 0, 97, 18, 6,
          [target(src("driver_monitor", ' and r.route == "${route}" and r._field == "awareness_pct"') + DOWNSAMPLE),
           # distracted only in drive or reverse: parked, the camera's verdict means nothing
           target(src("driver_monitor", ' and r.route == "${route}" and (r._field == "distracted" or r._field == "in_gear")') +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> map(fn: (r) => ({_time: r._time, _field: "distracted",\n'
                  '      _value: if exists r.in_gear and r.in_gear == 1 then float(v: r.distracted) else 0.0}))\n'
                  '  |> group(columns: ["_field"])\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)', "E"),
           target(src("driver_monitor", ' and r.route == "${route}" and r._field == "alert_level"') +
                  '  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)\n  |> keep(columns: ["_time", "_value", "_field"])', "C"),
           # speed, faint, so stops and parking (where the camera often says distracted) are easy to tell apart
           target(src("drive", ' and r.route == "${route}" and r._field == "speed_mph"') +
                  (f'  |> map(fn: (r) => ({{r with _value: r._value * {MI_TO_DIST}}}))\n' if METRIC else "") + DOWNSAMPLE, "D"),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")],
          maxDataPoints=2000, links=[{"title": "Zoom to this drive", "url": ZOOM_URL}],
          description="The driver-facing camera.\n\n"
                      "- **Awareness**: openpilot's attention budget, which drains while you look away with openpilot engaged and "
                      "refills when you look back\n"
                      "- **Warnings**: come as it runs out: level 1 = on-screen, 2 = louder, 3 = openpilot slows and disengages; "
                      "repeated level 3s lock you out for the drive\n"
                      "- **Shaded**: the camera judged you distracted (head pose or eyes), in drive or reverse only\n"
                      "- **Dashed grey**: speed; the background shading is the mode (like Speed & steering angle), so stops stand out\n\n"
                      "On the hands-on-the-wheel fallback (the camera not yet calibrated), awareness is from steering-wheel touches "
                      "instead. Parked or in neutral the camera often says distracted; that isn't shaded and doesn't count against you.",
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH}},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "awareness_pct"},
                          "properties": [{"id": "displayName", "value": "awareness"}, {"id": "unit", "value": "percent"},
                                         {"id": "min", "value": 0}, {"id": "max", "value": 100}, {"id": "decimals", "value": 0},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}}]},
                         {"matcher": {"id": "byName", "options": "distracted"},
                          "properties": [{"id": "displayName", "value": "distracted"}, {"id": "unit", "value": "percentunit"},
                                         {"id": "custom.axisPlacement", "value": "hidden"}, {"id": "min", "value": 0}, {"id": "max", "value": 1},
                                         {"id": "custom.drawStyle", "value": "bars"}, {"id": "custom.fillOpacity", "value": 25},
                                         {"id": "custom.lineWidth", "value": 0},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FADE2A"}}]},
                         {"matcher": {"id": "byName", "options": "alert_level"},
                          "properties": [{"id": "displayName", "value": "warning level"}, {"id": "decimals", "value": 0},
                                         {"id": "custom.axisPlacement", "value": "hidden"}, {"id": "min", "value": 0}, {"id": "max", "value": 3},
                                         {"id": "custom.drawStyle", "value": "bars"}, {"id": "custom.fillOpacity", "value": 60},
                                         {"id": "custom.lineWidth", "value": 0},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#F2495C"}}]},
                         {"matcher": {"id": "byName", "options": "speed_mph"},
                          "properties": [{"id": "displayName", "value": "speed"}, {"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0},
                                         {"id": "custom.axisPlacement", "value": "hidden"}, {"id": "min", "value": 0},
                                         {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [4, 4]}},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#8e8e8e"}}]},
                         DISTANCE_TOOLTIP]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    # the road camera's once-a-minute thumbnails (base64 JPEGs in InfluxDB), as an HTML gallery built by the hidden
    # ${snapshots} variable: a text panel shows them at full size, which a table can't (fixed row height)
    panel(26, "Road snapshots (one a minute)", "text", 0, 141, 24, 10, [],
          description="The road camera's thumbnail, once a minute (hover for the time). Follows the drive and time range.",
          options={"mode": "html", "content": "${snapshots:raw}"}),

    # ---- Driving details: how engagements ended, and errors (collapsible) ----
    {"id": 103, "type": "row", "title": "Driving details", "gridPos": {"x": 0, "y": 151, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(28, "Engagements on this drive", "table", 0, 152, 24, 6,
          [target(src("engagement", ' and r.route == "${route}"') +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> sort(columns: ["_time"])\n  |> elapsed(unit: 1ms)\n'
                  '  |> filter(fn: (r) => r.event == "end")\n'
                  '  |> map(fn: (r) => {\n'
                  '      held = if exists r.duration_s then r.duration_s else float(v: r.elapsed) / 1000.0\n'
                  f'      short = held < {ENGAGE_SHORT_S} and r.cause != "brake" and r.cause != "stalk cancel" and r.cause != "steering switched off"'
                  ' and r.cause != "cruise turned off"\n'
                  '      return {time: r._time, held: held, ended_by: r.cause, '
                  f'speed: r.speed_mph * {MI_TO_DIST}, note: if short then "didn\'t stick" else "", source: r.source,\n'
                  '              from_ms: uint(v: r._time) / uint(v: 1000000) - uint(v: 30000), to_ms: uint(v: r._time) / uint(v: 1000000) + uint(v: 15000)}\n'
                  '  })')],
          description="Each time openpilot disengaged on this drive: when, how long it had been engaged, and what ended it. "
                      "Click a time to zoom to it.\n\n"
                      "- **you**: stalk cancel, brake, steering switched off\n"
                      "- **the car**: car dropped ACC (its cruise module left on its own)\n"
                      "- **openpilot**: openpilot cancel, openpilot: …\n"
                      "- **cruise turned off**: not known yet (only the qlog so far; the device copy tells stalk from car)",
          transformations=[{"id": "organize", "options": {"excludeByName": {"from_ms": True, "to_ms": True},
                                                          "indexByName": {"time": 0, "held": 1, "ended_by": 2, "note": 3, "speed": 4, "source": 5}}}],
          fieldConfig={"defaults": {},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "time"},
                          "properties": [{"id": "displayName", "value": "disengaged at"}, {"id": "unit", "value": "time:HH:mm:ss"},
                                         {"id": "links", "value": [{"title": "Zoom to this moment",
                            "url": "/d/" + DASH_UID + "/comma-drives?var-route=${route}&${driver:queryparam}&from=${__data.fields.from_ms}&to=${__data.fields.to_ms}"}]}]},
                         {"matcher": {"id": "byName", "options": "held"},
                          "properties": [{"id": "displayName", "value": "engaged for"}, {"id": "unit", "value": "s"}, {"id": "decimals", "value": 1}]},
                         {"matcher": {"id": "byName", "options": "ended_by"}, "properties": [{"id": "displayName", "value": "ended by"}]},
                         {"matcher": {"id": "byName", "options": "speed"}, "properties": [{"id": "unit", "value": SPEED_UNIT}, {"id": "decimals", "value": 0}]},
                         {"matcher": {"id": "byName", "options": "note"},
                          "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": FAULT_COLOURS[FAULTS["engage_dropped"][0]]}}]},
                         {"matcher": {"id": "byName", "options": "source"},
                          "properties": [{"id": "description", "value": "rlog = from the full logs copied off the device (exact cause); qlog = cloud logs only"}]}]},
          options={"showHeader": True, "cellHeight": "sm"}),
    panel(29, "Position in lane", "timeseries", 0, 84, 18, 7,
          # ct.extract.LanePosition, in metres. Every window is drawn; where a line was unclear (quality < 2) an
          # "estimated" overlay marks it. Windows of at least 1 s on 2 Hz cloud data (shorter leaves single points that
          # don't draw), 0.2 s where the device copy's 10 Hz data is there. Empty windows are nulls: gaps under 1 s are
          # joined (a drive with only some rlogs copied has 2 Hz stretches in 0.2 s windows), longer ones stay gaps
          [target('import "internal/debug"\n'
                  'full = ' + src("lane", ' and r.route == "${route}" and r.source == "rlog" and r._field == "quality"').lstrip() +
                  '  |> limit(n: 1)\n  |> count()\n  |> findRecord(fn: (key) => true, idx: 0)\n'
                  'floor = if exists full._value then 200ms else 1s\n'
                  'every = if int(v: v.windowPeriod) < int(v: floor) then floor else v.windowPeriod\n'
                  'base = ' + src("lane", ' and r.route == "${route}" and (r._field == "offset" or r._field == "width" or r._field == "quality")').lstrip() +
                  'w = union(tables: [\n'
                  '    base |> filter(fn: (r) => r._field != "quality") |> aggregateWindow(every: every, fn: mean, createEmpty: true),\n'
                  '    base |> filter(fn: (r) => r._field == "quality") |> map(fn: (r) => ({r with _value: float(v: r._value)}))\n'
                  '        |> aggregateWindow(every: every, fn: min, createEmpty: true)])\n'
                  '  |> keep(columns: ["_time", "_value", "_field"])\n  |> group()\n'
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
                  'union(tables: [\n'
                  f'    w |> map(fn: (r) => ({{_time: r._time, _field: "position", _value: if exists r.offset then r.offset * {LEAD_DIST_FACTOR} else debug.null(type: "float")}})),\n'
                  f'    w |> map(fn: (r) => ({{_time: r._time, _field: "position_estimated",\n'
                  f'        _value: if exists r.offset and exists r.quality and r.quality < 2.0 then r.offset * {LEAD_DIST_FACTOR} else debug.null(type: "float")}})),\n'
                  f'    w |> map(fn: (r) => ({{_time: r._time, _field: "left_line", _value: if exists r.width then r.width * {-LEAD_DIST_FACTOR / 2} else debug.null(type: "float")}})),\n'
                  f'    w |> map(fn: (r) => ({{_time: r._time, _field: "right_line", _value: if exists r.width then r.width * {LEAD_DIST_FACTOR / 2} else debug.null(type: "float")}}))])\n'
                  '  |> group(columns: ["_field"])\n  |> sort(columns: ["_time"])'),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")],
          maxDataPoints=2000, links=[{"title": "Zoom to this drive", "url": ZOOM_URL}],
          description="Where the car sat in its lane, from the driving model's lane lines: above the middle line = right of "
                      "the lane's centre, below = left.\n\n"
                      "- **position**: the middle of the car\n"
                      "- **estimated** (orange dashes over it): the model wasn't sure of one or both lines, so the position is "
                      "partly guessed (the missing line is placed the lane's last measured width away)\n"
                      "- **left / right line**: the lane's edges (half the lane width either side)\n"
                      "- the car's side touches a line when its middle is about half the car's width inside it\n"
                      "- a lane change shows as the position crossing a line and jumping to the new lane\n"
                      "- **blank**: no lane lines at all, or no data\n\n"
                      "More detail on drives copied off the device (Rivian: 10 Hz rather than 2). "
                      "Red bands are errors. Hover for the distance into the drive.",
          fieldConfig={"defaults": {"unit": LEAD_DIST_UNIT, "decimals": 1,
                                    "custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never",
                                               "axisWidth": AXIS_WIDTH, "spanNulls": 1000}},
                       "overrides": [
                         # ±2 m either way, on the lane series only (on the defaults the hidden distance series grows an axis)
                         {"matcher": {"id": "byRegexp", "options": "^(position|position_estimated|left_line|right_line)$"},
                          "properties": [{"id": "min", "value": -2 * LEAD_DIST_FACTOR}, {"id": "max", "value": 2 * LEAD_DIST_FACTOR}]},
                         {"matcher": {"id": "byName", "options": "position"},
                          "properties": [{"id": "displayName", "value": "position (+ = right of centre)"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#73BF69"}}, {"id": "custom.lineWidth", "value": 2}]},
                         # drawn over the position where it was partly guessed: green with orange dashes
                         {"matcher": {"id": "byName", "options": "position_estimated"},
                          "properties": [{"id": "displayName", "value": "estimated (a lane line unclear)"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}}, {"id": "custom.lineWidth", "value": 2},
                                         {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [4, 4]}}]},
                         *[{"matcher": {"id": "byName", "options": f"{side}_line"},
                            "properties": [{"id": "displayName", "value": f"{side} line"}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#8e8e8e"}},
                                           {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}}]}
                           for side in ("left", "right")],
                         DISTANCE_TOOLTIP]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),
    panel(30, "Following (car ahead)", "timeseries", 0, 103, 18, 7,
          # relative speed stays blank with no car ahead (0 would read as matching its speed)
          [target(zero_filled("lead", ["gap_s", "distance_m", "rel_speed"], ["gap_s", "distance_m"],
                              f'  |> map(fn: (r) => ({{r with _value: if r._field == "distance_m" then r._value * {LEAD_DIST_FACTOR} else r._value}}))\n'
                              if LEAD_DIST_FACTOR != 1.0 else "")),
           target(ODOMETER + '  |> aggregateWindow(every: v.windowPeriod, fn: last, column: "d", createEmpty: false)\n'
                  '  |> map(fn: (r) => ({_time: r._time, _value: r.d, _field: "distance"}))\n  |> group(columns: ["_field"])', "B")],
          maxDataPoints=2000, links=[{"title": "Zoom to this drive", "url": ZOOM_URL}],
          description="The radar's car ahead, while there is one:\n\n"
                      "- **time gap**: seconds until you'd reach where it is now\n"
                      "- **distance** to it\n"
                      "- hover for relative speed (negative = closing in)\n\n"
                      "0 = no car ahead (or nearly stopped).",
          fieldConfig={"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "axisWidth": AXIS_WIDTH // 2,
                                               "spanNulls": False}},
                       "overrides": [
                         {"matcher": {"id": "byName", "options": "gap_s"},
                          "properties": [{"id": "displayName", "value": "time gap"}, {"id": "unit", "value": "s"}, {"id": "min", "value": 0},
                                         {"id": "custom.axisPlacement", "value": "left"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}}]},
                         {"matcher": {"id": "byName", "options": "distance_m"},
                          "properties": [{"id": "displayName", "value": "distance to car ahead"}, {"id": "unit", "value": LEAD_DIST_UNIT},
                                         {"id": "min", "value": 0}, {"id": "custom.axisPlacement", "value": "left"},
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}}]},
                         {"matcher": {"id": "byName", "options": "rel_speed"},
                          "properties": [{"id": "displayName", "value": "relative speed"}, {"id": "unit", "value": "velocityms"},
                                         {"id": "custom.axisPlacement", "value": "hidden"},
                                         {"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}}]},
                         DISTANCE_TOOLTIP]},
          options={"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}),

    # beside the drive's charts, so the row for the moment under the cursor shows next to it (FOLLOW_JS scrolls it)
    panel(EVENT_LOG, "Event log (follows the cursor)", "table", 18, 63, 6, 47,
          [target(EVENT_LOG_QUERY)],
          description="Every event / alert on this drive, oldest first (*(manual)* = openpilot wasn't engaged), plus each "
                      "change of the mode timeline's steering control, driver attention, lane confidence and model intent rows (lane "
                      f"confidence: each 5 s mean, low below {LANE_STEPS[0]:.0%} / high from {LANE_STEPS[1]:.0%}; model intent: "
                      f"each second's mean in m/s², brake below -{INTENT_STEP} / speed up above {INTENT_STEP}).\n\n"
                      "Hover over any chart to its left and the table scrolls to the last event at or before that moment, "
                      "highlighted and level with the cursor. Leave the table unsorted for this to line up.",
          fieldConfig={"defaults": {"custom": {"wrapHeaderText": True}},
                       "overrides": [{"matcher": {"id": "byName", "options": "_time"},
                                      "properties": [{"id": "displayName", "value": "Time"}, {"id": "unit", "value": "time:HH:mm:ss"},
                                                     {"id": "custom.width", "value": 80}]},
                                     {"matcher": {"id": "byName", "options": "event"},
                                      "properties": [{"id": "displayName", "value": "Event"},
                                                     {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": "text"}},
                                                     {"id": "mappings", "value": EVENT_LOG_COLOURS}]}]},
          options={"showHeader": True, "cellHeight": "sm"}),
    *collision_panels(),
    panel(9, "Events / alerts on this drive", "table", 0, 125, 24, 7,
          [target(src("events", ' and r.route == "${route}" and r._field == "count"') +
                  '  |> group(columns: ["event"])\n  |> reduce(identity: {count: 0, first: time(v: 0)}, fn: (r, accumulator) => '
                  '({count: accumulator.count + 1, first: if accumulator.count == 0 then r._time else accumulator.first}))\n'
                  '  |> group()\n  |> keep(columns: ["event", "count", "first"])\n  |> sort(columns: ["count"], desc: true)')],
          options={"showHeader": True, "cellHeight": "sm"}),
  ]

  for p in panels:
    if p["id"] in (5, 6, 7, 8, 9, 20):
      p["links"] = [{"title": "Zoom to this drive", "url": ZOOM_URL}]

  last_ms = '  |> last()\n  |> map(fn: (r) => ({_value: int(v: r._time) / 1000000}))'
  stat_opts = {"colorMode": "background", "graphMode": "none", "textMode": "value", "justifyMode": "auto",
               "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
  AGO_SIZE = 24  # the "… ago" panels: one size, not each fitted to its own width
  status = [
    {"id": 102, "type": "row", "title": "Sync status", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(10, "comma token expires in", "stat", 0, 1, 5, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -30d)\n'
                  '  |> filter(fn: (r) => r._measurement == "sync_status" and r._field == "token_expires")\n'
                  '  |> last()\n  |> map(fn: (r) => ({_value: (float(v: r._value) - float(v: int(v: now())) / 1000000000.0) / 86400.0}))')],
          description="Renew at https://jwt.comma.ai, then replace `COMMA_TOKEN` in .env and run `docker compose up -d`.",
          fieldConfig={"defaults": {"unit": "d", "decimals": 0, "noValue": "no sync yet",
                                    "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                                                 {"color": "orange", "value": 7},
                                                                                 {"color": "green", "value": 14}]},
                                    "links": [{"title": "Get a new token", "url": "https://jwt.comma.ai", "targetBlank": True}]},
                       "overrides": []},
          options=stat_opts),
    panel(11, "Cloud sync", "stat", 5, 1, 4, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -30d)\n'
                  '  |> filter(fn: (r) => r._measurement == "sync_status" and r._field == "cloud_ok")\n  |> last()\n'
                  '  |> keep(columns: ["_value"])')],
          fieldConfig={"defaults": {"noValue": "no sync yet",
                                    "mappings": [{"type": "value", "options": {"1": {"text": "OK", "color": "green"},
                                                                               "0": {"text": "ERROR", "color": "red"}}}]},
                       "overrides": []},
          options=stat_opts),
    panel(12, "Last cloud sync", "stat", 10, 4, 4, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -30d)\n'
                  '  |> filter(fn: (r) => r._measurement == "sync_status" and r._field == "cloud_ok")\n' + last_ms)],
          fieldConfig={"defaults": {"unit": "dateTimeFromNow", "color": {"mode": "fixed", "fixedColor": "text"}}, "overrides": []},
          options={**stat_opts, "colorMode": "none", "text": {"valueSize": AGO_SIZE}}),
    panel(13, "Newest drive loaded", "stat", 5, 4, 5, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -365d)\n'
                  '  |> filter(fn: (r) => r._measurement == "route" and r._field == "distance_mi")\n  |> group()\n' + last_ms)],
          fieldConfig={"defaults": {"unit": "dateTimeFromNow", "color": {"mode": "fixed", "fixedColor": "text"}}, "overrides": []},
          options={**stat_opts, "colorMode": "none", "text": {"valueSize": AGO_SIZE}}),
    panel(14, "comma last seen at home", "stat", 0, 4, 5, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -365d)\n'
                  '  |> filter(fn: (r) => r._measurement == "sync_status" and r._field == "device_seen")\n' + last_ms)],
          description="Last time the comma was reachable on the home LAN for a direct log copy (rlogs).",
          fieldConfig={"defaults": {"unit": "dateTimeFromNow", "noValue": "never", "color": {"mode": "fixed", "fixedColor": "text"}},
                       "overrides": []},
          options={**stat_opts, "colorMode": "none", "text": {"valueSize": AGO_SIZE}}),
    panel(15, "Software (latest drive)", "stat", 9, 1, 12, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -365d)\n'
                  '  |> filter(fn: (r) => r._measurement == "route" and (r._field == "commit_date" or r._field == "fork"))\n'
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> sort(columns: ["_time"])\n  |> last(column: "commit_date")\n'
                  '  |> map(fn: (r) => ({release: (if exists r.fork and r.fork != "" then r.fork + " " else "") + r.version + " · " + r.branch,\n'
                  '      build: r.commit + " (" + r.commit_date + ")  —  AGNOS " + r.os}))')],
          description="Which openpilot, from the newest drive's logs:\n\n"
                      "- the fork (stock openpilot, sunnypilot, Adventure Pilot... named from the GitHub repo it was installed from)\n"
                      "- its release · branch · commit (commit date)\n"
                      "- the comma OS (AGNOS) version",
          fieldConfig={"defaults": {"color": {"mode": "fixed", "fixedColor": "text"}}, "overrides": []},
          options={**stat_opts, "colorMode": "none", "textMode": "value", "orientation": "horizontal", "text": {"valueSize": 18},
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False}}),
    panel(17, "Driving models (latest drive)", "stat", 14, 4, 6, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -365d)\n'
                  '  |> filter(fn: (r) => r._measurement == "route" and (r._field == "model_big" or r._field == "model_small"))\n'
                  '  |> group(columns: ["_field"])\n  |> last()\n'
                  '  |> map(fn: (r) => ({_value: if r._value == "" then "none" else r._value,\n'
                  '      name: if r._field == "model_big" then "big model" else "device"}))\n'
                  '  |> group(columns: ["name"])')],
          description="The driving models selected in sunnypilot's model manager on the newest drive: the big model "
                      "run on a Chestnut accelerator or an off-board Jetlink host, and the device's own model.",
          fieldConfig={"defaults": {"displayName": "${__field.labels.name}", "color": {"mode": "fixed", "fixedColor": "text"}},
                       "overrides": []},
          options={**stat_opts, "colorMode": "none", "textMode": "value_and_name", "orientation": "horizontal",
                   "text": {"titleSize": 12, "valueSize": 16},
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False}}),
    panel(41, "Rlogs waiting", "stat", 20, 4, 4, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -30d)\n'
                  '  |> filter(fn: (r) => r._measurement == "sync_status" and r._field == "rlogs_waiting")\n  |> last()\n'
                  '  |> keep(columns: ["_value"])')],
          description="How much time spent driving still needs to be copied off the comma as full logs (rlogs). \"1 hour\" means an hour "
                      "of driving, not how long the copy takes (about 5 minutes).\n\n"
                      "Copying happens by itself when the comma is on your home Wi-Fi, usually as the car starts. To catch "
                      "up, power the car on at home.\n\n"
                      "The comma holds roughly 12 hours of driving, then deletes its oldest logs to make room, so this "
                      "turns orange at 8 hours and red at 11.\n\n"
                      "Drives the cloud hasn't received yet aren't counted.",
          fieldConfig={"defaults": {"unit": "m", "decimals": 0, "noValue": "no device copy",
                                    "mappings": [{"type": "value", "options": {"0": {"text": "All copied"}}}],
                                    "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                                                 {"color": "orange", "value": 480},
                                                                                 {"color": "red", "value": 660}]}},
                       "overrides": []},
          options=stat_opts),
    # no content, only CSS for the bar of controls at the top (Grafana has no options for its order or colours). Above
    # the first row, in a headerless one of its own that the CSS hides: in a row, collapsing it would remove the CSS,
    # and further down Grafana would only draw it once scrolled to. Needs GF_PANELS_DISABLE_SANITIZE_HTML=true.
    # Lines: Drive, "Zoom to selected drive" (blue) and the time picker (on the right); the other dropdowns; the
    # annotation toggles.
    panel(CSS_PANEL, "", "text", 0, 0, 24, 1, [], transparent=True, options={"mode": "html", "content": TOP_BAR_CSS +
                                                                     f'<img src="data:," alt="" style="display: none" onerror="{html.escape(FOLLOW_JS)}">'}),
    panel(16, "vs GitHub", "stat", 21, 1, 3, 3,
          [target(f'from(bucket: "{BUCKET}")\n  |> range(start: -30d)\n'
                  '  |> filter(fn: (r) => r._measurement == "github" and r._field == "behind")\n  |> group()\n  |> last()\n'
                  '  |> map(fn: (r) => ({_value: r._value, repo: if exists r.repo then r.repo else "' + ENV.get("GITHUB_REPO", "") + '"}))\n'
                  '  |> group(columns: ["repo"])')],
          description="Commits on the newest drive's branch on GitHub that it wasn't running. Checked hourly.\n\n"
                      "The repo is the one the device's openpilot was installed from; `GITHUB_REPO` in .env overrides it.",
          fieldConfig={"defaults": {"noValue": "not checked yet", "decimals": 0,
                                    "mappings": [{"type": "value", "options": {"0": {"text": "Up to date", "color": "green"},
                                                                               "-1": {"text": "check failed", "color": "#8e8e8e"}}},
                                                 {"type": "range", "options": {"from": 1, "to": 1000000,
                                                                               "result": {"text": "${__value.raw} behind", "color": "orange"}}}],
                                    "links": [{"title": "Open repo", "url": "https://github.com/${__field.labels.repo}",
                                               "targetBlank": True}]},
                       "overrides": []},
          options=stat_opts),
  ]
  panels = status + panels
  for p in panels:
    if p["id"] in DESCRIPTIONS and not p.get("description"):
      p["description"] = DESCRIPTIONS[p["id"]]

  dash = {
    "uid": DASH_UID, "title": "comma drives", "timezone": ENV["TZ_NAME"], "editable": True, "schemaVersion": 39,
    # hovering one time graph shows a crosshair and dots at the same moment on all the others
    "graphTooltip": 1,
    "time": {"from": "now-30d", "to": "now"}, "refresh": "",  # until there's a drive: then latest_drive_time()
    "tags": ["comma", "openpilot"],
    "templating": {"list": [
      {"name": "driver", "label": "Driver", "type": "query", "datasource": DS, "multi": True, "includeAll": True,
       "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "refresh": 1, "sort": 1,
       "query": {"query": f'from(bucket: "{BUCKET}")\n  |> range(start: -10y)\n  |> filter(fn: (r) => r._measurement == "route" and r._field == "distance_mi")\n'
                          '  |> keep(columns: ["driver"])\n  |> group()\n  |> distinct(column: "driver")', "refId": "v"}},
      *[{"name": name, "label": label, "type": "query", "datasource": DS, "multi": True, "includeAll": True,
         "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "refresh": 1, "sort": 1,
         "query": {"query": src("route", f' and r._field == "{field}"').replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y") +
                            # {r with ...}: a map to a bare {_value: ...} record drops rows whose stored value is "" (InfluxDB 2.9.1)
                            '  |> group()\n  |> map(fn: (r) => ({r with _value: if r._value == "" then "none" else r._value}))\n'
                            '  |> distinct()', "refId": "v"}}
        for name, label, field in (("small_model", "Small model", "model_small"), ("big_model", "Big model", "model_big"))],
      # the drives that ran the picked models: what DRIVER_FILTER filters on (all of them while both are All)
      {"name": "model_routes", "type": "query", "datasource": DS, "hide": 2, "multi": True, "includeAll": True,
       "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "refresh": 1,
       "query": {"query": src("route", ' and (r._field == "model_small" or r._field == "model_big")').replace(
                            "start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y") +
                          '  |> group(columns: ["route", "_field"])\n  |> last()\n  |> group()\n'
                          '  |> pivot(rowKey: ["route"], columnKey: ["_field"], valueColumn: "_value")\n'
                          '  |> map(fn: (r) => ({r with\n'
                          '      small: if exists r.model_small and r.model_small != "" then r.model_small else "none",\n'
                          '      big: if exists r.model_big and r.model_big != "" then r.model_big else "none"}))\n'
                          '  |> filter(fn: (r) => contains(value: r.small, set: ${small_model:json}) and contains(value: r.big, set: ${big_model:json}))\n'
                          '  |> map(fn: (r) => ({_value: r.route}))', "refId": "v"}},
      {"name": "route", "label": "Drive", "type": "query", "datasource": DS, "refresh": 1, "sort": 0,
       # query returns "route|label"; the regex splits it so the dropdown shows the label but the value stays the route id
       "regex": "/^(?<value>[^|]+)\\|(?<text>.+)$/",
       "query": {"query": 'import "date"\n' + TZ +
                          'days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]\n'
                          'pad = (n) => if n < 10 then "0" + string(v: n) else string(v: n)\n' +
                          src("route", ' and r._field == "distance_mi"').replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y") + DRIVER_FILTER +
                          '  |> group()\n  |> sort(columns: ["_time"], desc: true)\n'
                          '  |> map(fn: (r) => {\n'
                          '      h = date.hour(t: r._time)\n'
                          f'      tenths = int(v: r._value * {MI_TO_DIST} * 10.0 + 0.5)\n'
                          '      label = pad(n: date.month(t: r._time)) + "-" + pad(n: date.monthDay(t: r._time)) + "-" + pad(n: date.year(t: r._time) % 100)\n'
                          '        + " " + days[date.weekDay(t: r._time)]\n'
                          '        + " " + pad(n: h) + ":" + pad(n: date.minute(t: r._time))\n'
                          '        + " · " + r.driver + " · " + string(v: tenths / 10) + "." + string(v: tenths % 10) + " ' + DIST_WORD + '"\n'
                          '      return {_value: r.route + "|" + label}\n'
                          '  })', "refId": "v"}},
      # the drive's collision alerts, for the close-up: the first one unless the callout's link picked another
      {"name": "alert", "label": "Collision alert", "type": "query", "datasource": DS, "hide": 2, "refresh": 1, "sort": 0,
       "regex": "/^(?<value>[^|]+)\\|(?<text>.+)$/",
       "query": {"query": 'import "date"\n' + TZ +
                          'pad = (n) => if n < 10 then "0" + string(v: n) else string(v: n)\n' +
                          src("collision_alert", ' and r.route == "${route}" and r._field == "text"').replace("start: v.timeRangeStart, stop: v.timeRangeStop", "start: -10y") +
                          '  |> group()\n  |> sort(columns: ["_time"])\n'
                          '  |> map(fn: (r) => ({_value: string(v: int(v: r._time) / 1000000) + "|" + pad(n: date.hour(t: r._time)) + ":"'
                          ' + pad(n: date.minute(t: r._time)) + ":" + pad(n: date.second(t: r._time)) + " " + r.kind}))', "refId": "v"}},
      {"name": "snapshots", "type": "query", "datasource": DS, "hide": 2, "refresh": 2, "skipUrlSync": True,
       "query": {"query": 'import "date"\n' + TZ +
                          'pad = (n) => if n < 10 then "0" + string(v: n) else string(v: n)\n' +
                          src("thumbnail", ' and r.route == "${route}" and r._field == "jpeg"') +
                          '  |> sort(columns: ["_time"])\n'
                          '  |> map(fn: (r) => ({r with md: "<img src=\\"data:image/jpeg;base64," + r._value + "\\" title=\\"" +\n'
                          '      pad(n: date.hour(t: r._time)) + ":" + pad(n: date.minute(t: r._time)) + "\\" style=\\"margin: 2px\\">"}))\n'
                          '  |> reduce(identity: {_value: ""}, fn: (r, accumulator) => ({_value: accumulator._value + r.md}))\n'
                          '  |> map(fn: (r) => ({_value: if r._value == "" then "no snapshots in this range" else r._value}))', "refId": "v"}},
      *[{"name": name, "type": "query", "datasource": DS, "hide": 2, "refresh": 1, "skipUrlSync": True,
         "query": {"query": f'from(bucket: "{BUCKET}")\n  |> range(start: -10y)\n'
                            '  |> filter(fn: (r) => r._measurement == "route" and r.route == "${route}" and r._field == "duration_min")\n'
                            '  |> last()\n  |> map(fn: (r) => ({_value: string(v: ' + expr + ')}))', "refId": "v"}}
        for name, expr in (("route_from", "int(v: r._time) / 1000000 - 60000"),
                           ("route_to", "int(v: r._time) / 1000000 + int(v: r._value * 60000.0) + 60000"))],
    ]},
    "links": [{"title": "Zoom to selected drive", "type": "link", "icon": "dashboard", "tooltip": "Set the time range to the selected drive",
               "url": ZOOM_URL, "keepTime": False, "includeVars": False, "targetBlank": False}],
    "annotations": {"list": [
      *[{"name": MODE_COLORS[m][1].split(" (")[0], "enable": True, "iconColor": colour, "datasource": DS,
         "description": f"Shades the stretches driven in {MODE_COLORS[m][1]} on Speed & steering angle, Braking and Driver attention.",
         "filter": {"exclude": False, "ids": [7, 20, 34]},
         "target": {"refId": "Anno", "query": src("mode_interval", ' and r.route == "${route}" and r._field == "end_ns" and r.mode == "' + m + '"') +
                    '  |> group()\n  |> map(fn: (r) => ({_time: r._time, timeEnd: time(v: r._value), text: r.mode}))'},
         "mappings": {"timeEnd": {"source": "field", "value": "timeEnd"}, "text": {"source": "field", "value": "text"}}}
        for m, colour in (("full", "rgba(115, 191, 105, 0.25)"), ("mads", "rgba(87, 148, 242, 0.25)"),
                          ("manual", "rgba(142, 142, 142, 0.2)"))],
      # a tick on the time axis every 1 / 5 / 10 mi or km (by drive length), so the drive graphs read in distance too
      {"name": "Distance markers", "enable": True, "iconColor": "rgba(150, 150, 150, 0.45)", "datasource": DS,
       "description": f"A tick every 1, 5 or 10 {DIST} along the drive (by its length), so the drive graphs read in distance too.",
       "filter": {"exclude": False, "ids": [5, 7, 8, 25, 29, 30, 34]},
       "target": {"refId": "Anno", "query": 'import "date"\nimport "math"\ndata = ' + ODOMETER_PIPE +
                  'tot = data |> last(column: "d") |> findRecord(fn: (key) => true, idx: 0)\n'
                  'total = if exists tot.d then tot.d else 0.0\n'
                  'step = if total <= 15.0 then 1 else if total <= 60.0 then 5 else 10\n'
                  'data\n  |> map(fn: (r) => ({_time: r._time, m: int(v: math.floor(x: r.d / float(v: step)))}))\n'
                  '  |> unique(column: "m")\n  |> filter(fn: (r) => r.m > 0 and r._time >= v.timeRangeStart)\n'
                  '  |> map(fn: (r) => ({_time: r._time, text: string(v: r.m * step) + " ' + DIST + '", tags: "distance"}))'},
       "mappings": {"text": {"source": "field", "value": "text"}}},
      {"name": "Collision alerts", "enable": True, "iconColor": ALERT_COLOUR, "datasource": DS,
       "description": "A marker at each forward collision warning or emergency braking alert on this drive, the car's or openpilot's.",
       "filter": {"exclude": False, "ids": [5, 7, 8, 20, 25, 29, 30, 34]},
       "target": {"refId": "Anno", "query": src("collision_alert", ' and r.route == "${route}" and r._field == "text"') +
                  '  |> group()\n  |> map(fn: (r) => ({_time: r._time, text: r.kind + ": " + r._value, tags: "collision alert"}))'},
       "mappings": {"text": {"source": "field", "value": "text"}, "tags": {"source": "field", "value": "tags"}}},
      {"name": "Errors", "enable": True, "iconColor": "rgba(196, 22, 42, 0.35)", "datasource": DS,
       "description": "Shades the stretches where openpilot reported a fault, on Speed & steering angle, Steering torque, "
                      "Position in lane and Braking.",
       "filter": {"exclude": False, "ids": [7, 8, 20, 29]},
       "target": {"refId": "Anno", "query": FAULT_LABELS + src("fault", ' and r.route == "${route}" and (r._field == "end_ns" or r._field == "duration_s")') +
                  '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n  |> group()\n'
                  '  |> map(fn: (r) => ({_time: r._time, timeEnd: time(v: r.end_ns),\n'
                  '      text: ' + FAULT_LABEL + ' + " · " + string(v: r.duration_s) + " s", tags: "error"}))'},
       "mappings": {"timeEnd": {"source": "field", "value": "timeEnd"}, "text": {"source": "field", "value": "text"},
                    "tags": {"source": "field", "value": "tags"}}},
      *[{"name": name, "enable": False, "iconColor": colour, "datasource": DS, "description": desc,
         "target": {"refId": "Anno", "query": src("events", ' and r.route == "${route}" and r.engaged == "' + flag + '"') +
                    # events carry an int count and a string mode: pivot them into one row before grouping
                    '  |> pivot(rowKey: ["_time", "event"], columnKey: ["_field"], valueColumn: "_value")\n'
                    '  |> group()\n  |> map(fn: (r) => ({_time: r._time, text: r.event, tags: r.mode}))'},
         "mappings": {"text": {"source": "field", "value": "text"}, "tags": {"source": "field", "value": "tags"}}}
        for name, colour, flag, desc in (
          ("Events (openpilot engaged)", "red", "1", "A marker at each openpilot event (alerts, warnings) while it was engaged. Off by default: busy."),
          ("Events (manual)", "#8e8e8e", "0", "A marker at each openpilot event while driving manually. Off by default: busy."))],
    ]},
    "panels": [overview_range(p) if p["id"] in OVERVIEW_PANELS else p for p in panels],
  }
  # Grafana 13 drops an annotation's description, so the toggles' tooltips are drawn by the top bar's CSS
  css = next(p for p in panels if p["id"] == CSS_PANEL)
  css["options"]["content"] = css["options"]["content"].replace("</style>", toggle_tips(dash["annotations"]["list"]) + "</style>")
  return dash


def toggle_tips(annotations: list[dict]) -> str:
  """CSS that shows each annotation toggle's description below it while its label is hovered."""
  rules = []
  for a in annotations:
    if a.get("description"):
      label = f'label[data-testid="data-testid Dashboard template variables submenu Label {a["name"]}"]'
      text = a["description"].replace("\\", "\\\\").replace('"', '\\"')
      rules.append(f'{label}:hover::after {{ content: "{text}" }}')
  return ("".join(rules) +
          '[class*="dashboard-data-layer-controls"] { position: relative }'
          'label[data-testid^="data-testid Dashboard template variables submenu Label"]::after { display: none }'
          'label[data-testid^="data-testid Dashboard template variables submenu Label"]:hover::after {'
          ' display: block; position: absolute; top: calc(100% + 6px); left: 0; z-index: 1000; width: max-content; max-width: 280px;'
          ' padding: 6px 8px; border-radius: 4px; background: #202226; border: 1px solid #3a3d44; color: #e0e0e0;'
          ' font-size: 12px; font-weight: 400; line-height: 1.4; white-space: normal; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4) }')


RETIRED = {31}  # panel ids removed from the dashboard (31: By software version)
# groups of panels that are hidden on drives where none of them has data: the collision close-ups, the CAN braking graph
HIDE_EMPTY = [{36, 37}, {20}]


def keep_layout(new: dict, old: dict) -> dict:
  """Keep positions/sizes (and collapsed rows) from the live dashboard, so edits made in Grafana survive setup."""
  def flat(ps):
    for p in ps:
      yield p
      yield from flat(p.get("panels", []))
  old_panels = list(flat(old.get("panels", [])))
  by_id = {p["id"]: p for p in old_panels if p["type"] != "row"}
  # Grafana renumbers rows, so match them by title prefix (the drive row was titled "Drive: ..." before it showed the drive)
  def row_key(title: str) -> str:
    return "drive" if title.startswith(("Drive:", "${route")) else title.split(":")[0].split(" (")[0]
  rows = {row_key(p.get("title", "")): p for p in old_panels if p["type"] == "row"}
  ours = set()
  for p in new["panels"]:
    if p["id"] == CSS_PANEL:
      ours.add(p["id"])
      continue
    o = rows.get(row_key(p.get("title", ""))) if p["type"] == "row" else by_id.get(p["id"])
    if o:
      p["gridPos"] = o["gridPos"]
      if p["type"] == "row":
        p["id"] = o["id"]
        p["collapsed"] = o.get("collapsed", False)
      ours.add(o["id"])
  # a row's old id can be one a newer panel uses (Grafana numbers rows from the lowest free id): give it a fresh one
  panel_ids = {p["id"] for p in new["panels"] if p["type"] != "row"}
  next_id = max([p["id"] for p in new["panels"]] + [p["id"] for p in old_panels] + [0]) + 1
  for p in new["panels"]:
    if p["type"] == "row" and p["id"] in panel_ids:
      p["id"], next_id = next_id, next_id + 1
  # panels the user added themselves stay too (not ones the dashboard used to have)
  new["panels"] += [p for p in old.get("panels", []) if p["id"] not in ours and p["id"] not in RETIRED and p["type"] != "row"]
  new["version"] = old.get("version")
  return new


READ_TOKEN_NAME = "grafana read-only (comma-analytics)"
TOKEN_NAMES = {READ_TOKEN_NAME, "grafana read-only (comma-dashboard)"}  # and its name under the project's old name


def influx_setup() -> str:
  """Make sure the bucket exists; return a fresh read-only token for Grafana.

  InfluxDB only shows a token's value when it's created, so the old one (found by name) is replaced on every
  run; the Grafana data source is updated with the new one right after."""
  org = ENV["INFLUX_ORG"]
  with InfluxDBClient(url=ENV["INFLUX_URL"], token=secret("influx_token"), timeout=30_000) as client:
    org_id = client.organizations_api().find_organizations(org=org)[0].id
    buckets = client.buckets_api()
    bucket = buckets.find_bucket_by_name(BUCKET)
    if bucket is None:
      bucket = buckets.create_bucket(bucket_name=BUCKET, org_id=org_id)  # infinite retention
      print(f"created bucket {BUCKET} in org {org}")
    if ENV.get("INFLUX_READ_TOKEN"):
      return ENV["INFLUX_READ_TOKEN"]
    auths = client.authorizations_api()
    read = Permission(action="read", resource=PermissionResource(type="buckets", id=bucket.id, org_id=org_id))
    new = auths.create_authorization(org_id=org_id, permissions=[read])
    token = new.token
    for a in auths.find_authorizations(org_id=org_id):
      if a.description in TOKEN_NAMES:
        auths.delete_authorization(a)
    new.description = READ_TOKEN_NAME
    auths.update_authorization(new)
    return token


def latest_drive_time() -> dict | None:
  """The newest drive's span, padded a minute each side like ${route_from}/${route_to}. Saved as the dashboard's
  default time range, so opening the page shows the drive the Drive dropdown starts on (its newest) without Zoom."""
  with InfluxDBClient(url=ENV["INFLUX_URL"], token=secret("influx_token"), org=ENV["INFLUX_ORG"], timeout=30_000) as client:
    tables = client.query_api().query(
      f'from(bucket: "{BUCKET}")\n  |> range(start: -10y)\n'
      '  |> filter(fn: (r) => r._measurement == "route" and r._field == "duration_min")\n'
      '  |> group()\n  |> sort(columns: ["_time"], desc: true)\n  |> limit(n: 1)')
  rec = next((r for t in tables for r in t.records), None)
  if rec is None:
    return None
  start = rec.get_time()
  iso = lambda t: t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"
  return {"from": iso(start - dt.timedelta(minutes=1)), "to": iso(start + dt.timedelta(minutes=rec.get_value() + 1))}


def dashboards_url() -> str:
  return f"{ENV['GRAFANA_URL']}/apis/dashboard.grafana.app/v2/namespaces/{ENV.get('GRAFANA_NAMESPACE', 'default')}/dashboards"


def save_meta(old: dict | None, message: str) -> dict:
  meta = {"name": DASH_UID, "annotations": {"grafana.app/message": message}}
  if old:
    meta["resourceVersion"] = old["resourceVersion"]
    if "grafana.app/folder" in old.get("annotations", {}):
      meta["annotations"]["grafana.app/folder"] = old["annotations"]["grafana.app/folder"]
  return meta


def follow_latest_drive() -> None:
  """Move the saved default time range to the newest drive when one lands or grows (the daemon calls this each pass)."""
  span = latest_drive_time()
  if span is None:
    return
  gf = grafana_session()
  cur = gf.get(f"{dashboards_url()}/{DASH_UID}")
  if not cur.ok:
    return  # not set up yet
  dash = cur.json()
  ts = dash["spec"]["timeSettings"]
  if (ts.get("from"), ts.get("to")) == (span["from"], span["to"]):
    return
  ts |= span
  body = {"apiVersion": dash["apiVersion"], "kind": "Dashboard", "metadata": save_meta(dash["metadata"], "comma-analytics: newest drive"),
          "spec": dash["spec"]}
  r = gf.put(f"{dashboards_url()}/{DASH_UID}", json=body)
  if not r.ok:
    raise RuntimeError(f"saving the dashboard's time range: {r.status_code} {r.text[:500]}")
  print(f"dashboard opens on the newest drive: {span['from']} to {span['to']}")


def grafana_session() -> requests.Session:
  gf = requests.Session()
  if secret("grafana_token", required=False):
    gf.headers["Authorization"] = f"Bearer {secret('grafana_token')}"
  else:
    gf.auth = (ENV["GRAFANA_USER"], secret("grafana_password"))
  return gf


def main(wait: float = 0) -> int:
  """`wait`: seconds to keep retrying while InfluxDB / Grafana are still starting."""
  deadline = time.time() + wait
  for name, url in (("InfluxDB", ENV["INFLUX_URL"] + "/health"), ("Grafana", ENV["GRAFANA_URL"] + "/api/health")):
    while True:  # quietly, so the logs don't fill with connection tracebacks on a cold start
      try:
        if requests.get(url, timeout=5).ok:
          break
      except requests.RequestException:
        pass
      if time.time() > deadline:
        break  # let the real call below raise a useful error
      print(f"waiting for {name}...")
      time.sleep(5)
  while True:
    try:
      read_token = influx_setup()
      grafana_setup(read_token)
      return 0
    except (requests.ConnectionError, OSError) as e:  # urllib3 errors from the influx client are OSErrors too
      if time.time() > deadline:
        raise
      print(f"waiting for InfluxDB / Grafana: {e.__class__.__name__}")
      time.sleep(5)


def grafana_setup(read_token: str) -> None:
  if "--influx-only" in sys.argv:
    return
  org = ENV["INFLUX_ORG"]
  gf = grafana_session()
  base = ENV["GRAFANA_URL"]
  ds = {"name": "comma (InfluxDB)", "uid": DS_UID, "type": "influxdb", "access": "proxy",
        "url": ENV.get("GRAFANA_INFLUX_URL", ENV["INFLUX_URL"]),
        "jsonData": {"version": "Flux", "organization": org, "defaultBucket": BUCKET},
        "secureJsonData": {"token": read_token}}
  r = gf.get(f"{base}/api/datasources/uid/{DS_UID}")
  r = gf.put(f"{base}/api/datasources/uid/{DS_UID}", json=ds) if r.ok else gf.post(f"{base}/api/datasources", json=ds)
  r.raise_for_status()
  print("data source ok")
  dash = build_dashboard()
  dash["time"] = latest_drive_time() or dash["time"]
  url = dashboards_url()
  cur = gf.get(f"{url}/{DASH_UID}")
  if cur.ok and "--reset-layout" not in sys.argv:  # after moving panels in the code: drop the layout saved in Grafana
    dash = keep_layout(dash, dashboard_v2.flat(cur.json()["spec"]))  # Grafana serves it as v2 even if saved as classic
  meta = save_meta(cur.json()["metadata"] if cur.ok else None, "comma-analytics setup")
  body = {"apiVersion": "dashboard.grafana.app/v2", "kind": "Dashboard", "metadata": meta,
          "spec": dashboard_v2.to_v2(dash, HIDE_EMPTY)}
  r = gf.put(f"{url}/{DASH_UID}", json=body) if cur.ok else gf.post(url, json=body)
  if not r.ok:
    raise RuntimeError(f"saving the dashboard: {r.status_code} {r.text[:500]}")
  print(f"dashboard: {ENV.get('GRAFANA_PUBLIC_URL', base)}/d/{DASH_UID}")


if __name__ == "__main__":
  if "--print" in sys.argv:
    print(json.dumps(dashboard_v2.to_v2(build_dashboard(), HIDE_EMPTY), indent=1))
    sys.exit(0)
  sys.exit(main())
