#!/usr/bin/env python3
"""Pull new comma drives (+ optional Home Assistant driver history) and load them into InfluxDB.

  ./sync.py --daemon        # container default: setup, then device pull every minute (if DEVICE_IP), cloud pull every hour
  ./sync.py                 # one cloud pull: last 4 days of routes
  ./sync.py --days 30       # look further back
  ./sync.py --device        # one device pull (if reachable) + ingest anything copied off the device
  ./sync.py --device --route 00000092   # pull one drive now (DEVICE_IP from .env or env)
  ./sync.py --reload ROUTE  # delete and re-ingest one route (e.g. after changing extract.py)
  ./sync.py --reload all    # re-ingest everything in the archive
"""
import argparse
import base64
import datetime as dt
import json
import sys
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from ct.config import ARCHIVE, ENV, load_state, save_state, secret
from ct import device
from ct.extract import extract_segment
from ct.faults import FAULTS, FaultTracker
from ct.rlog import BRANDS, extract_rlog
from ct.safety import extract_alerts
from ct.sources import (download_qlogs, driver_at, fork_name, github_status, list_routes, newest_software, route_dir,
                        saved_driver_history, sync_driver_history)


DELETE = None  # InfluxDB delete API, set in main()
QUERY = None  # InfluxDB query API, set in main()
STITCHED = ("rack_ignoring_torque",)  # rlog faults that often run on across segment boundaries


def replace_qlog_engagements(route: str, span: tuple[int, int] | None) -> None:
  """The rlog knows how each engagement ended; drop the qlog's guesses (and short-engagement errors) for its span."""
  if not span or DELETE is None:
    return
  start = dt.datetime.fromtimestamp(span[0] / 1e9, dt.UTC)
  stop = dt.datetime.fromtimestamp(span[1] / 1e9 + 0.001, dt.UTC)
  for pred in (f'_measurement="engagement" AND route="{route}" AND source="qlog"',
               f'_measurement="fault" AND route="{route}" AND fault="engage_dropped"'):
    DELETE.delete(start, stop, pred, bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])


def replace_qlog_model(route: str, span: tuple[int, int] | None) -> None:
  """The rlog's modelV2 says which model drove each moment; drop the qlog's summary for its span before writing it.
  The qlog and rlog each set their clock from their own first GPS fix, so the qlog's model_summary can sit a few ms
  before the rlog's span[0]: start that delete a second early (segments are 60 s apart, so the previous segment's
  summary is safe). model_rlog keeps the exact span, since the previous segment's last frames can fall in that second."""
  if not span or DELETE is None:
    return
  stop = dt.datetime.fromtimestamp(span[1] / 1e9 + 0.001, dt.UTC)
  for measurement, pad_s in (("model_summary", 1.0), ("model_rlog", 0.0)):
    start = dt.datetime.fromtimestamp(span[0] / 1e9 - pad_s, dt.UTC)
    DELETE.delete(start, stop, f'_measurement="{measurement}" AND route="{route}"', bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])


def rlog_model(state, name: str) -> set[int]:
  """Segments of route `name` whose model time came from the rlog (see replace_qlog_model)."""
  return set(state.get("rlog_model", {}).get(name, []))


def rlog_engage(state, name: str) -> set[int]:
  """Segments of route `name` whose engagements came from the rlog (see replace_qlog_engagements)."""
  return set(state.get("rlog_engage", {}).get(name, []))


def upgrade_state(state) -> None:
  """Fill in what older state files didn't keep."""
  if "rlog_engage" not in state:  # every Rivian rlog read so far replaced its segment's qlog engagements
    state["rlog_engage"] = {}
    for name, segs in state.get("rlog_loaded", {}).items():
      sw = route_dir(name) / "software.json"
      brand = json.loads(sw.read_text()).get("brand") if sw.exists() else None
      if brand is None or brand in BRANDS:  # same test as extract_rlog's
        state["rlog_engage"][name] = sorted(segs)
    save_state(state)


def stitch_faults(write, tags: dict, names=STITCHED) -> None:
  """Each rlog segment is decoded alone, so a fault running across segments is stored once per segment: rewrite the
  route's intervals of these faults with neighbours closer than the fault's merge gap joined into one."""
  if QUERY is None or DELETE is None:
    return
  route = tags["route"]
  for name in names:
    rows = QUERY.query(f'''from(bucket: "{ENV["INFLUX_BUCKET"]}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "fault" and r.route == "{route}" and r.fault == "{name}")
  |> filter(fn: (r) => r._field == "end_ns" or r._field == "frames")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"])
  |> map(fn: (r) => ({{start: int(v: r._time), end_ns: r.end_ns, frames: r.frames}}))''', org=ENV["INFLUX_ORG"])
    spans = [(r["start"], int(r["end_ns"]), int(r["frames"])) for t in rows for r in t.records]
    joined: list[list[int]] = []
    for start, end, frames in spans:
      if joined and start - joined[-1][1] <= FAULTS[name][2] * 1e9:
        joined[-1][1:] = [max(end, joined[-1][1]), joined[-1][2] + frames]
      else:
        joined.append([start, end, frames])
    if len(joined) == len(spans):
      continue  # nothing to join
    tracker = FaultTracker(tags, 0)
    for start, end, frames in joined:
      tracker.add(name, start, end, frames=frames)
    points = tracker.points
    # a µs either side: datetimes don't hold nanoseconds; the next unjoined interval is at least the merge gap away
    DELETE.delete(dt.datetime.fromtimestamp(spans[0][0] / 1e9 - 1e-6, dt.UTC), dt.datetime.fromtimestamp(spans[-1][1] / 1e9 + 1e-6, dt.UTC),
                  f'_measurement="fault" AND route="{route}" AND fault="{name}"', bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])
    write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], points)
    log(f"{route}: {name}: {len(spans)} per-segment intervals joined into {len(joined)}")


def write_alerts(write, route: dict, segs, tags: dict) -> int:
  """Collision alerts (FCW / AEB) starting in these segments or next to them, from the best log there is for each.

  Re-run as segments and rlogs arrive: an alert's close-up can reach into a neighbour that wasn't there yet."""
  qdir = route_dir(route["fullname"])
  dev = device.DEVICE / route["fullname"].split("|")[1]

  def qlog_for(s):
    p = qdir / f"{s}.qlog.zst"
    return p if p.exists() else None

  def best_for(s):
    p = dev.with_name(f"{dev.name}--{s}") / "rlog.zst"
    return p if p.exists() else qlog_for(s)

  n = 0
  for seg in sorted({x for s in segs for x in (s - 1, s, s + 1) if x >= 0}):
    try:
      points, spans, seg_span = extract_alerts(seg, qlog_for, best_for, tags, segment_start_ns(route, seg))
    except Exception as e:  # truncated/corrupt log: the alert waits for a later pass
      log(f"{route['fullname']} seg {seg}: collision alert decode failed: {e}")
      continue
    if seg_span and DELETE is not None:  # alerts written for this segment before (one may now belong to its neighbour)
      DELETE.delete(dt.datetime.fromtimestamp(seg_span[0] / 1e9, dt.UTC), dt.datetime.fromtimestamp(seg_span[1] / 1e9, dt.UTC),
                    f'_measurement="collision_alert" AND route="{tags["route"]}"', bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])
    for start, stop in spans:  # the close-up's earlier copy (qlog, or a shorter window) goes first
      if DELETE is not None:
        DELETE.delete(dt.datetime.fromtimestamp(start / 1e9, dt.UTC), dt.datetime.fromtimestamp(stop / 1e9, dt.UTC),
                      f'_measurement="collision_detail" AND route="{tags["route"]}"', bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])
    if points:
      write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], points)
      n += len(spans)
  return n


def log(msg: str) -> None:
  print(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def route_tags(route: dict, driver: str) -> dict:
  return {"route": route["fullname"].split("|")[1], "driver": driver, "branch": route.get("git_branch") or "?"}


def segment_start_ns(route: dict, seg: int) -> int | None:
  try:
    i = route["segment_numbers"].index(seg)
    return int(route["segment_start_times"][i]) * 1_000_000
  except (KeyError, ValueError, IndexError):
    return None


def delete_route(client: InfluxDBClient, route: dict) -> None:
  short = route["fullname"].split("|")[1]
  client.delete_api().delete("1970-01-01T00:00:00Z", "2100-01-01T00:00:00Z", f'route="{short}"',
                             bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])


def ingest_route(client, write, route: dict, segs: list[int], done: set[int], changes,
                 rlog_model: set[int] = frozenset(), rlog_engage: set[int] = frozenset()) -> int:
  """`rlog_model` / `rlog_engage`: segments whose model time / engagements already came from the rlog, so a qlog read
  after it (a late cloud upload, or one unreadable the first time) doesn't write its own as well."""
  driver = driver_at(changes, route["start_time_utc_millis"] / 1000)
  tags = route_tags(route, driver)
  n = 0
  metres = 0.0
  info_file = route_dir(route["fullname"]) / "software.json"
  info = json.loads(info_file.read_text()) if info_file.exists() else {}
  new = []
  for seg in segs:
    if seg in done:
      continue
    path = route_dir(route["fullname"]) / f"{seg}.qlog.zst"
    try:
      points, summary, seg_info = extract_segment(path, tags, segment_start_ns(route, seg), seg not in rlog_model,
                                                 seg not in rlog_engage)
    except Exception as e:  # truncated/corrupt qlog (e.g. upload cut off): skip it, retried next pass
      log(f"{route['fullname']} seg {seg}: unreadable qlog, skipped ({str(e).splitlines()[0]})")
      continue
    if seg_info and any(info.get(k) != v for k, v in seg_info.items()):
      info = {**info, **seg_info}
      info_file.write_text(json.dumps(info, indent=1))
    metres += sum(m for _, m in summary.values())
    if points:
      write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], points)
    done.add(seg)
    n += len(points)
    new.append(seg)
  if new:
    write_alerts(write, route, new, tags)
  if route.get("distance") is None:  # device-only route: running total from the logs, kept in route.json
    route["log_metres"] = route.get("log_metres", 0.0) + metres
    distance = route["log_metres"] / 1609.344
  else:
    distance = float(route["distance"])
  # one summary point per route (overwritten each run as segments arrive)
  meta = (Point("route").time(int(route["start_time_utc_millis"]) * 1_000_000, WritePrecision.NS)
          .field("distance_mi", distance)
          .field("duration_min", (route["end_time_utc_millis"] - route["start_time_utc_millis"]) / 60e3)
          .field("segments", len(segs)).field("git_commit", route.get("git_commit") or "")
          .field("commit_date", info.get("commit_date", "")).field("device", info.get("device", ""))
          .field("fork", fork_name(info.get("remote") or route.get("git_remote") or ""))
          .field("model_small", info.get("model_small", "")).field("model_big", info.get("model_big", "")))
  for k, v in tags.items():
    meta = meta.tag(k, v)
  # software versions as tags so the dashboard can group by them
  meta = (meta.tag("version", info.get("version") or route.get("version") or "?").tag("os", info.get("os") or "?")
          .tag("commit", info.get("commit") or (route.get("git_commit") or "?")[:7]))
  write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], meta)
  return n


def token_expiry(token: str) -> int:
  """Unix seconds when a comma JWT expires (read from its payload; the signature isn't checked)."""
  payload = token.split(".")[1]
  return int(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"])


def write_status(write, cloud_ok: bool, error: str = "") -> None:
  p = (Point("sync_status").time(dt.datetime.now(dt.UTC), WritePrecision.S)
       .field("cloud_ok", int(cloud_ok)).field("error", error[:200])
       .field("token_expires", token_expiry(secret("access_token"))))
  write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], p)


def ingest_device(write, state, changes) -> None:
  # route -> the segments it had when none of them had a GPS time fix. Checked again once more segments arrive:
  # a drive's first minute is often copied before the GPS has a fix (e.g. the device on home wifi as the car starts)
  no_gps = state.get("no_gps")
  if not isinstance(no_gps, dict):  # older state: a plain list, given up on for good; look at each once more
    no_gps = state["no_gps"] = {route: [] for route in no_gps or []}
  for route, segs in device.device_routes().items():
    name = f"{ENV['DONGLE_ID']}|{route}"
    loaded = set(state["loaded"].get(name, []))
    new = [x for x in segs if x not in loaded]
    if not new or no_gps.get(route) == segs:
      continue
    r = device.stage(route, segs)
    if r is None:  # car switched on but no fix yet (or never, e.g. parked): can't be placed on the timeline
      no_gps[route] = segs
      save_state(state)
      log(f"{name}: no GPS time fix in {len(segs)} segment(s) yet, skipped until more arrive")
      continue
    no_gps.pop(route, None)
    done = set(state["loaded"].get(name, []))
    if r.get("source") == "device":
      r["log_metres"] = r.get("log_metres", 0.0) if done else 0.0
    n = ingest_route(None, write, r, segs, done, changes, rlog_model(state, name), rlog_engage(state, name))
    if r.get("source") == "device":
      (route_dir(name) / "route.json").write_text(json.dumps(r, indent=1))
    state["loaded"][name] = sorted(done)
    save_state(state)
    log(f"{name}: {len(new)} segs from device, {n} points written")


RLOG_TRIES = 3  # tries before an rlog that won't decode is given up on
RLOG_RETRY_S = 300  # at least this long between tries, so a passing problem (e.g. out of memory) has cleared


def ingest_rlogs(write, state, changes, limit: int | None = 30) -> int:
  """Braking + EPS fault data from rlogs copied off the device. A few segments per call so the daemon keeps polling."""
  loaded = state.setdefault("rlog_loaded", {})
  failed = state.setdefault("rlog_failed", {})  # name -> {seg: [tries, file size, last try]}: rlogs that wouldn't decode
  n_segs = 0
  for route, _ in device.device_routes().items():
    name = f"{ENV['DONGLE_ID']}|{route}"
    meta = route_dir(name) / "route.json"
    if not meta.exists():
      continue  # route itself not ingested (e.g. no GPS fix)
    sw = meta.parent / "software.json"
    brand = json.loads(sw.read_text()).get("brand") if sw.exists() else None
    r = json.loads(meta.read_text())
    tags = route_tags(r, driver_at(changes, r["start_time_utc_millis"] / 1000))
    done = set(loaded.get(name, []))
    segs = sorted(int(d.name.rsplit("--", 1)[1]) for d in device.DEVICE.glob(f"{route}--*") if (d / "rlog.zst").exists())
    new = []
    for seg in segs:
      if seg in done:
        continue
      if limit is not None and n_segs >= limit:
        break
      path = device.DEVICE / f"{route}--{seg}" / "rlog.zst"
      tries, size, last_try = failed.get(name, {}).get(str(seg), [0, None, 0])
      if size != path.stat().st_size:
        tries = 0  # copied again since it failed: a fresh start
      elif tries >= RLOG_TRIES or time.time() - last_try < RLOG_RETRY_S:
        continue  # gave up on this file (--reload ROUTE tries it again), or tried it only just now
      try:
        points, summary = extract_rlog(path, tags, segment_start_ns(r, seg), brand)
      except Exception as e:  # a bad file, or a one-off (e.g. out of memory): try again on later passes, then give up
        tries += 1
        failed.setdefault(name, {})[str(seg)] = [tries, path.stat().st_size, int(time.time())]
        save_state(state)
        last = " (giving up)" if tries >= RLOG_TRIES else f", will try again in {RLOG_RETRY_S // 60} min"
        log(f"{name} seg {seg}: rlog decode failed, try {tries} of {RLOG_TRIES}{last}: {e}")
        continue
      if failed.get(name, {}).pop(str(seg), None) is not None and not failed[name]:
        del failed[name]
      if points:
        if not summary.get("model_only"):  # another car's rlog says nothing about how engagements ended
          replace_qlog_engagements(tags["route"], summary.get("span"))
          # a qlog read after this must not write its guesses again
          engage = state.setdefault("rlog_engage", {})
          engage[name] = sorted(set(engage.get(name, [])) | {seg})
        if summary.get("has_model"):
          replace_qlog_model(tags["route"], summary.get("span"))
          # a qlog read after this (a late cloud upload) must not write its model time again
          models = state.setdefault("rlog_model", {})
          models[name] = sorted(set(models.get(name, [])) | {seg})
        write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], points)
      done.add(seg)
      new.append(seg)
      n_segs += 1
      loaded[name] = sorted(done)
      save_state(state)
    if new:  # redo the close-ups of collision alerts in these segments from the rlog
      write_alerts(write, r, new, tags)
      stitch_faults(write, tags)
    if limit is not None and n_segs >= limit:
      return n_segs
  return n_segs


def device_pull(write, state, changes, verbose: bool = False, route: str | None = None) -> None:
  if not ENV.get("DEVICE_IP") or not device.reachable():
    if verbose:
      log(f"device: {ENV.get('DEVICE_IP') or 'DEVICE_IP not set'} not reachable")
    return
  started = int(time.time() * 1000)
  try:
    if device.pull(log, route) and not route:
      # everything still on the device is here now, so an older rlog still missing is gone, not waiting. Two minutes'
      # margin for the segment being recorded as the copy started: it counts as waiting until the next copy
      state["rlog_copied_until"] = started - 120_000
      save_state(state)
  except Exception as e:  # powered off mid-copy etc.; resume next time
    log(f"device: pull failed: {e}")
  changes = drivers(write)  # the profile may have changed since the last hourly refresh
  ingest_device(write, state, changes)
  n = ingest_rlogs(write, state, changes)
  if n:
    log(f"device: braking + fault data from {n} rlog segments")
  p = Point("sync_status").time(dt.datetime.now(dt.UTC), WritePrecision.S).field("device_seen", 1)
  write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], p)


def write_rlogs_waiting(write, state) -> None:
  """Minutes of driving whose rlogs are still only on the device, which deletes its oldest logs when it fills up."""
  if ENV.get("DEVICE_IP"):
    p = (Point("sync_status").time(dt.datetime.now(dt.UTC), WritePrecision.S)
         .field("rlogs_waiting", device.rlogs_waiting(state.get("rlog_copied_until", 0))))
    write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], p)


def cloud_pull(write, state, changes, days: float) -> int:
  try:
    routes = list_routes(days)
  except Exception as e:  # e.g. 401 once the comma token expires
    log(f"listing routes failed: {e}")
    write_status(write, False, str(e))
    return 1
  failed = []
  for r in routes:
    name = r["fullname"]
    try:
      segs = download_qlogs(r)
    except Exception as e:  # keep going; retried next run
      log(f"download failed {name}: {e}")
      failed.append(name)
      continue
    done = set(state["loaded"].get(name, []))
    # always rewrite the route summary: comma fills in distance/duration after the upload finishes
    n = ingest_route(None, write, r, segs, done, changes, rlog_model(state, name), rlog_engage(state, name))
    state["loaded"][name] = sorted(done)
    save_state(state)
    if n:
      log(f"{name}: {len(segs)} segs archived, {n} points written")
  write_status(write, not failed, f"download failed: {', '.join(failed)}" if failed else "")
  return 0


def github_check(write) -> bool:
  sw = newest_software()
  repo = ENV.get("GITHUB_REPO") or sw.get("repo")  # .env overrides the repo the device was installed from
  if not sw.get("commit") or not repo:
    return True
  p = Point("github").time(dt.datetime.now(dt.UTC), WritePrecision.S).tag("branch", sw["branch"]).tag("repo", repo)
  try:
    gh = github_status(repo, sw["branch"], sw["commit"])
    p = (p.field("behind", gh["behind"]).field("head", gh["head"]).field("head_date", gh["head_date"])
         .field("device_commit", sw["commit"]))
    if gh["behind"]:
      log(f"github: {sw['branch']} is {gh['behind']} commits ahead of the device ({sw['commit']} -> {gh['head']})")
  except Exception as e:
    log(f"github check failed: {e}")
    write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], p.field("behind", -1).field("device_commit", sw["commit"]))
    return False
  write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], p)
  return True


def drivers(write) -> list:
  """Driver changes, refreshed from Home Assistant when it's set up. HA is optional and may be down: then the
  changes saved so far are used, so the rest of the sync carries on."""
  try:
    changes = sync_driver_history()
  except Exception as e:  # HA down, token expired, ...: drives are labelled from what was saved before
    saved = saved_driver_history()
    log(f"driver history: Home Assistant not reachable, using the {len(saved)} changes saved before: {e}")
    return saved
  if changes:
    write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"],
                [Point("active_driver").time(int(t * 1e9), WritePrecision.NS).field("driver", n) for t, n in changes])
  return changes


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--days", type=float, default=4)
  ap.add_argument("--reload", metavar="ROUTE|all")
  ap.add_argument("--device", action="store_true", help="one device pull + ingest of anything copied off the device")
  ap.add_argument("--route", help="with --device: only this drive (route id or prefix, e.g. 00000092)")
  ap.add_argument("--daemon", action="store_true")
  ap.add_argument("--rlogs", action="store_true", help="ingest braking + EPS fault data from all rlogs copied off the device")
  args = ap.parse_args()

  state = load_state()
  upgrade_state(state)
  with InfluxDBClient(url=ENV["INFLUX_URL"], token=secret("influx_token"), org=ENV["INFLUX_ORG"], timeout=60_000) as client:
    write = client.write_api(write_options=SYNCHRONOUS)
    global DELETE, QUERY
    DELETE = client.delete_api()
    QUERY = client.query_api()

    if args.daemon:
      import setup_grafana
      setup_grafana.main(wait=600)  # bucket, Grafana data source + dashboard (InfluxDB/Grafana may still be starting)
      days = float(ENV.get("FIRST_DAYS", "30"))  # first pull looks further back (comma keeps ~3 days, or longer with prime)
      cloud_every = int(ENV.get("CLOUD_INTERVAL", "3600"))
      retry_every = 300  # after a failure (DNS hiccup, GitHub/comma down): try again soon, not in an hour
      device_every = int(ENV.get("DEVICE_INTERVAL", "60"))
      next_cloud = 0.0
      changes = []
      while True:
        try:
          if time.time() >= next_cloud:
            changes = drivers(write)
            ok = cloud_pull(write, state, changes, days) == 0
            if ok:  # the first look-back (FIRST_DAYS) stays until a pull gets through
              days = args.days
            ok = github_check(write) and ok
            next_cloud = time.time() + (cloud_every if ok else retry_every)
          device_pull(write, state, changes)
          write_rlogs_waiting(write, state)
          setup_grafana.follow_latest_drive()
          if ingest_rlogs(write, state, changes):
            continue  # backlog left: go again without sleeping
        except Exception as e:  # never let one bad run kill the loop
          log(f"error: {e}")
          next_cloud = min(next_cloud, time.time() + retry_every)
        time.sleep(device_every)

    changes = drivers(write)
    log(f"driver history: {len(changes)} profile changes")

    if args.reload:
      routes = [json.loads(p.read_text()) for p in sorted(ARCHIVE.glob("*/route.json"))]
      if args.reload != "all":
        routes = [r for r in routes if args.reload in r["fullname"]]
      for r in routes:
        delete_route(client, r)
        state["loaded"].pop(r["fullname"], None)
        state.get("rlog_loaded", {}).pop(r["fullname"], None)  # braking data goes too; re-ingested by the daemon
        state.get("rlog_model", {}).pop(r["fullname"], None)  # and the rlog's model time with it
        state.get("rlog_engage", {}).pop(r["fullname"], None)  # and its engagements
        state.get("rlog_failed", {}).pop(r["fullname"], None)  # rlogs that wouldn't decode get another go
        segs = sorted(int(p.name.split(".")[0]) for p in route_dir(r["fullname"]).glob("*.qlog.zst"))
        done: set[int] = set()
        if r.get("source") == "device":
          r["log_metres"] = 0.0
        n = ingest_route(client, write, r, segs, done, changes)
        if r.get("source") == "device":
          (route_dir(r["fullname"]) / "route.json").write_text(json.dumps(r, indent=1))
        state["loaded"][r["fullname"]] = sorted(done)
        log(f"reloaded {r['fullname']}: {len(segs)} segs, {n} points")
      save_state(state)
      return 0

    if args.device:
      device_pull(write, state, changes, True, args.route)
      while ingest_rlogs(write, state, changes):  # the whole backlog, not one daemon-sized batch
        pass
      return 0

    if args.rlogs:
      log(f"rlogs: {ingest_rlogs(write, state, changes, limit=None)} segments ingested")
      return 0

    rc = cloud_pull(write, state, changes, args.days)
    github_check(write)
    return rc


if __name__ == "__main__":
  sys.exit(main())
