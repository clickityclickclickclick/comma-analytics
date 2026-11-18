"""Remote sources: comma API (routes + qlogs), Home Assistant (active driver, optional) and GitHub (optional)."""
import datetime as dt
import json
import re
import time

import requests

from .config import ARCHIVE, ENV, ha_enabled, secret

COMMA_API = "https://api.commadotai.com"


def _comma_get(path: str, **params):
  r = requests.get(f"{COMMA_API}/{path}", params=params, timeout=60,
                   headers={"Authorization": f"JWT {secret('access_token')}"})
  r.raise_for_status()
  return r.json()


def list_routes(days: float) -> list[dict]:
  now = int(time.time() * 1000)
  routes = _comma_get(f"v1/devices/{ENV['DONGLE_ID']}/routes_segments",
                      start=now - int(days * 86400e3), end=now, limit=1000)
  return sorted(routes, key=lambda r: r["start_time_utc_millis"])


def route_dir(route: str):
  return ARCHIVE / route.replace("|", "_")


def download_qlogs(route: dict) -> list[int]:
  """Download any qlog segments not yet archived. Returns all archived segment numbers."""
  name = route["fullname"]
  d = route_dir(name)
  d.mkdir(parents=True, exist_ok=True)
  (d / "route.json").write_text(json.dumps(route, indent=1))
  files = _comma_get(f"v1/route/{name}/files")
  for url in files.get("qlogs", []):
    seg = int(re.search(r"/(\d+)/qlog", url).group(1))
    out = d / f"{seg}.qlog.zst"
    if out.exists():
      continue
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    tmp = out.with_suffix(".part")
    tmp.write_bytes(r.content)
    tmp.replace(out)
  return sorted(int(p.name.split(".")[0]) for p in d.glob("*.qlog.zst"))


DRIVER_HISTORY = ARCHIVE / "active_driver.json"


def saved_driver_history() -> list[tuple[float, str]]:
  """The driver changes saved locally so far (empty without HA, or before its first read)."""
  if not DRIVER_HISTORY.exists():
    return []
  return sorted((float(t), n) for t, n in json.loads(DRIVER_HISTORY.read_text()))


def sync_driver_history(days: float = 10) -> list[tuple[float, str]]:
  """Merge HA's active-driver history into a local file (HA purges after ~10 days). Empty without HA."""
  hist = dict(saved_driver_history())
  if not ha_enabled():
    return sorted(hist.items())
  now = dt.datetime.now(dt.UTC)
  start = (now - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
  r = requests.get(f"{ENV['HA_URL']}/api/history/period/{start}", timeout=60,  # HA defaults to a 1-day window
                   params={"filter_entity_id": ENV["HA_DRIVER_ENTITY"], "end_time": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
                   headers={"Authorization": f"Bearer {secret('ha_token')}"})
  r.raise_for_status()
  rows = r.json()[0] if r.json() else []
  # HA's first row is the state at the window start, not a real change; only trust it if we have nothing earlier
  for i, row in enumerate(rows):
    if row["state"] in ("unknown", "unavailable", ""):
      continue
    t = dt.datetime.fromisoformat(row["last_changed"]).timestamp()
    if i == 0 and hist and t <= max(hist):
      continue
    hist[t] = row["state"]
  changes = sorted(hist.items())
  # collapse consecutive duplicates
  changes = [c for i, c in enumerate(changes) if i == 0 or c[1] != changes[i - 1][1]]
  DRIVER_HISTORY.write_text(json.dumps(changes, indent=1))
  return changes


def driver_at(changes: list[tuple[float, str]], t: float) -> str:
  """Active driver at unix time t (profile is set a few seconds before the drive starts)."""
  name = ENV["DRIVER_NAME"] if not changes else "unknown"
  for ct, n in changes:
    if ct <= t + 60:
      name = n
    else:
      break
  return name


GITHUB_REPO_RE = r"(?:https?://)?github\.com/([\w.-]+)/([\w.-]+?)(?:\.git)?/?"
# GitHub owner -> the fork's name, for the forks whose repo is just called "openpilot"
FORK_OWNERS = {"commaai": "openpilot", "sunnypilot": "sunnypilot", "adventurepilotdev": "Adventure Pilot",
               "frogai": "FrogPilot", "dragonpilot-community": "dragonpilot"}


def fork_name(remote: str) -> str:
  """Which openpilot the device ran, from its git remote: a known owner's name, else the repo's name unless that's
  just "openpilot" (then the owner's), "" if it isn't on GitHub."""
  m = re.fullmatch(GITHUB_REPO_RE, remote or "")
  if not m:
    return ""
  owner, repo = m[1], m[2]
  return FORK_OWNERS.get(owner.lower()) or (repo if repo.lower() != "openpilot" else owner)


def newest_software() -> dict:
  """Software info of the newest drive in the archive (from its initData), plus "repo": the GitHub owner/repo the
  device's openpilot was installed from (comma's git_remote for the route), "" if it isn't on GitHub."""
  best, info = 0, {}
  for meta in ARCHIVE.glob("*/route.json"):
    sw = meta.parent / "software.json"
    route = json.loads(meta.read_text())
    start = route.get("start_time_utc_millis", 0)
    if sw.exists() and start > best:
      m = re.fullmatch(GITHUB_REPO_RE, route.get("git_remote") or "")
      best, info = start, {**json.loads(sw.read_text()), "repo": f"{m[1]}/{m[2]}" if m else ""}
  return info


def github_status(repo: str, branch: str, commit: str) -> dict:
  """How many commits the branch head on GitHub is ahead of the commit the device ran."""
  api = f"https://api.github.com/repos/{repo}"
  head = requests.get(f"{api}/branches/{branch}", timeout=30)
  head.raise_for_status()
  h = head.json()["commit"]
  cmp = requests.get(f"{api}/compare/{commit}...{h['sha']}", timeout=30)
  cmp.raise_for_status()
  return {"head": h["sha"][:7], "head_date": h["commit"]["committer"]["date"], "behind": cmp.json()["ahead_by"]}
