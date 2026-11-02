#!/usr/bin/env python3
"""Pull new comma drives (+ optional Home Assistant driver history) and load them into InfluxDB.

  ./sync.py --daemon        # container default: setup, then device pull every minute (if DEVICE_IP), cloud pull every hour
  ./sync.py                 # one cloud pull: last 4 days of routes
  ./sync.py --days 30       # look further back
  ./sync.py --device        # one device pull (if reachable) + ingest anything copied off the device
  ./sync.py --device --now --route 00000092   # pull one drive now, even with the car on (DEVICE_IP from .env or env)
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
from ct.rlog import BRANDS, extract_rlog
from ct.safety import extract_alerts
from ct.sources import (download_qlogs, driver_at, fork_name, github_status, list_routes, newest_software, route_dir,
                        sync_driver_history)


DELETE = None  # InfluxDB delete API, set in main()


def replace_qlog_engagements(route: str, span: tuple[int, int] | None) -> None:
  """The rlog knows how each engagement ended; drop the qlog's guesses (and short-engagement errors) for its span."""
  if not span or DELETE is None:
    return
  start = dt.datetime.fromtimestamp(span[0] / 1e9, dt.UTC)
  stop = dt.datetime.fromtimestamp(span[1] / 1e9 + 0.001, dt.UTC)
  for pred in (f'_measurement="engagement" AND route="{route}" AND source="qlog"',
               f'_measurement="fault" AND route="{route}" AND fault="engage_dropped"'):
    DELETE.delete(start, stop, pred, bucket=ENV["INFLUX_BUCKET"], org=ENV["INFLUX_ORG"])


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
      points, spans = extract_alerts(seg, qlog_for, best_for, tags, segment_start_ns(route, seg))
    except Exception as e:  # truncated/corrupt log: the alert waits for a later pass
      log(f"{route['fullname']} seg {seg}: collision alert decode failed: {e}")
      continue
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


def ingest_route(client, write, route: dict, segs: list[int], done: set[int], changes) -> int:
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
      points, summary, seg_info = extract_segment(path, tags, segment_start_ns(route, seg))
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
  for route, segs in device.device_routes().items():
    name = f"{ENV['DONGLE_ID']}|{route}"
    new = [x for x in segs if x not in set(state["loaded"].get(name, []))]
    if not new or route in state.setdefault("no_gps", []):
      continue
    r = device.stage(route, segs)
    if r is None:
      state["no_gps"].append(route)  # never moved / no time fix: can't be placed on the timeline
      save_state(state)
      log(f"{name}: no GPS time fix in any segment, skipped")
      continue
    done = set(state["loaded"].get(name, []))
    if r.get("source") == "device":
      r["log_metres"] = r.get("log_metres", 0.0) if done else 0.0
    n = ingest_route(None, write, r, segs, done, changes)
    if r.get("source") == "device":
      (route_dir(name) / "route.json").write_text(json.dumps(r, indent=1))
    state["loaded"][name] = sorted(done)
    save_state(state)
    log(f"{name}: {len(new)} segs from device, {n} points written")


def ingest_rlogs(write, state, changes, limit: int | None = 30) -> int:
  """Braking + EPS fault data from rlogs copied off the device. A few segments per call so the daemon keeps polling."""
  loaded = state.setdefault("rlog_loaded", {})
  n_segs = 0
  for route, _ in device.device_routes().items():
    name = f"{ENV['DONGLE_ID']}|{route}"
    meta = route_dir(name) / "route.json"
    if not meta.exists():
      continue  # route itself not ingested (e.g. no GPS fix)
    sw = meta.parent / "software.json"
    brand = json.loads(sw.read_text()).get("brand") if sw.exists() else None
    if brand and brand not in BRANDS:
      continue  # no DBC for this car
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
      try:
        points, summary = extract_rlog(device.DEVICE / f"{route}--{seg}" / "rlog.zst", tags, segment_start_ns(r, seg))
      except Exception as e:  # truncated/corrupt file: leave it for a later pass
        log(f"{name} seg {seg}: rlog decode failed: {e}")
        done.add(seg)
        continue
      if points:
        replace_qlog_engagements(tags["route"], summary.get("span"))
        write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"], points)
      done.add(seg)
      new.append(seg)
      n_segs += 1
      loaded[name] = sorted(done)
      save_state(state)
    if new:  # redo the close-ups of collision alerts in these segments from the rlog
      write_alerts(write, r, new, tags)
    if limit is not None and n_segs >= limit:
      return n_segs
  return n_segs


def device_pull(write, state, changes, now: bool = False, route: str | None = None) -> None:
  if not ENV.get("DEVICE_IP") or not device.reachable():
    if now:
      log(f"device: {ENV.get('DEVICE_IP') or 'DEVICE_IP not set'} not reachable")
    return
  try:
    device.pull(log, now, route)
  except Exception as e:  # powered off mid-copy etc.; resume next time
    log(f"device: pull failed: {e}")
  try:
    changes = drivers(write)  # the profile may have changed since the last hourly refresh
  except Exception as e:
    log(f"driver history refresh failed: {e}")
  ingest_device(write, state, changes)
  n = ingest_rlogs(write, state, changes)
  if n:
    log(f"device: braking + fault data from {n} rlog segments")
  p = Point("sync_status").time(dt.datetime.now(dt.UTC), WritePrecision.S).field("device_seen", 1)
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
    n = ingest_route(None, write, r, segs, done, changes)
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
  changes = sync_driver_history()
  if changes:
    write.write(ENV["INFLUX_BUCKET"], ENV["INFLUX_ORG"],
                [Point("active_driver").time(int(t * 1e9), WritePrecision.NS).field("driver", n) for t, n in changes])
  return changes


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--days", type=float, default=4)
  ap.add_argument("--reload", metavar="ROUTE|all")
  ap.add_argument("--device", action="store_true", help="one device pull + ingest of anything copied off the device")
  ap.add_argument("--now", action="store_true", help="with --device: copy finished drives even while the car is on")
  ap.add_argument("--route", help="with --device: only this drive (route id or prefix, e.g. 00000092)")
  ap.add_argument("--daemon", action="store_true")
  ap.add_argument("--rlogs", action="store_true", help="ingest braking + EPS fault data from all rlogs copied off the device")
  args = ap.parse_args()

  state = load_state()
  with InfluxDBClient(url=ENV["INFLUX_URL"], token=secret("influx_token"), org=ENV["INFLUX_ORG"], timeout=60_000) as client:
    write = client.write_api(write_options=SYNCHRONOUS)
    global DELETE
    DELETE = client.delete_api()

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
            days = args.days
            ok = github_check(write) and ok
            next_cloud = time.time() + (cloud_every if ok else retry_every)
          device_pull(write, state, changes)
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
      device_pull(write, state, changes, args.now, args.route)
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
