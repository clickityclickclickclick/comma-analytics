# comma-analytics

A self-hosted Grafana dashboard for your comma device's drives. It pulls each drive's logs from the comma cloud
(and optionally straight off the device on your home Wi-Fi), keeps a permanent local copy, and charts them.

Opening the dashboard shows your newest drive, zoomed to fit. Pick another in the **Drive** dropdown and click
**Zoom to selected drive**, or click a route in the Drives table. All the time graphs share one crosshair.

**Overview** (always the last 30 days, whatever the time picker says; filter by driving model, and by driver when
Home Assistant names them: see *driver names* under Setup):

- **Miles and time by mode:** full openpilot, steering only (MADS), stock ACC, manual. Per day, per driver (with Home
  Assistant), by day of the week, and per drive.
- **Drives table:** distance, time, share engaged, errors, collision alerts, engagements, driver attention, lane
  confidence, steering error and follow gap per drive.
- **Software by drive:** the version, branch, commit, AGNOS and driving models each drive ran, to compare builds.
- **Hands on wheel by driver** (Rivian, drives copied off the device): one bar per driver, like the miles beside
  it, splitting the hours driven into openpilot steering with hands on / hands off and the driver steering with hands
  on / hands off. It reads the car's capacitive wheel-touch sensor, so a light rest of the hands counts, not only
  pushing on the wheel. Turning the wheel counts too. **Sensor artifact:** the sensor often loses hands that are on
  the wheel for a few seconds at a time, even while the driver is turning it, so while the driver steers, hands stay
  counted for 5 seconds after the sensor or a turn of the wheel last showed them. There's no such hold while
  openpilot steers, so its hands-off share is never understated but can still include some dropouts.

**Each drive:**

- **Mode timeline:** one row each for the driving model, the mode, driver attention, lane confidence, steering control
  (torque / angle, including Adventure Pilot's Rivian angle steering), steering / accelerator / brake overrides,
  model intent (the acceleration the model wants: red brake, yellow hold, green speed up, as the device tints its
  path), braking, errors and events.
- **Event log (follows the cursor):** beside the charts, every event and alert plus each change of the timeline's
  steering control, attention, lane confidence and model intent rows. Hover over any chart and it scrolls to that
  moment.
- **Route map** coloured by mode, **speed and steering angle**, and **steering torque**: the driver's, what openpilot
  asked for, the EPS's own where the car reports it, and on a Rivian whether the rack was applying openpilot's torque
  (drives copied off the device).
- **Errors:** the panda blocking steering commands, openpilot steering faults, the steering rack silently ignoring
  openpilot's torque (Rivian, drives copied off the device), CAN errors and openpilot system alerts (commIssue,
  locationd/paramsd errors, ...). Startup noise is filtered out.
- **Driving model:** which model drove each moment and per drive, by name, from sunnypilot's model manager. With a
  Chestnut accelerator or an off-board Jetlink host this covers the big model, its loading and any fallback to the device's own model.
  Names come from the models picked in the device's model settings when the drive starts. With none picked the
  device runs the models built into the software and the dashboard shows only "device model" or "big model". Pick
  both models in the model settings, and pick them again after a software update, which can clear them.
- **Time to ready:** from the car coming on to openpilot running and then to the first driving-model output,
  coloured by which model came up (the big model on a Chestnut or Jetlink host, or the device's own).
- **Engagements:** every disengagement with what ended it (brake, stalk cancel, the car dropping ACC, an openpilot
  cancel or alert), and engagements that didn't stick (under 3 s, not ended by you) flagged as errors. The exact
  cause needs the full logs copied off the device (Rivian CAN); cloud-only drives show "cruise turned off".
- **Collision alerts:** openpilot's forward collision warning ("BRAKE! Risk of Collision") and the car's own FCW /
  emergency braking, called out at the top of the drive (speed, the car ahead, time to impact, how soon you braked,
  peak braking, closest gap), marked in red on the graphs and counted in the Drives table, with a close-up of the
  6 s before to 8 s after (chart and half-second table; 100 Hz from the full logs when copied off the device).
- **Driver attention:** from the driver-monitoring camera: openpilot's awareness budget, when it judged you
  distracted, and its alerts as the device names them (Pay Attention / Touch Steering Wheel, levels 1–3); per drive,
  the share of steered time you were distracted and the warning count.
- **Position in lane and following:** where the car sat between the lane lines, from the driving model (2 Hz, or 10 Hz
  on Rivian drives copied off the device), with stretches marked where a missing line had to be estimated; per drive,
  how closely the car followed openpilot's steering (Steer Error); and time gap / distance to the car ahead.
- **Visibility:** the driving model's lane confidence, vision uncertainty and road-camera exposure (fog, rain and
  glare show up as dips), plus the road camera's snapshot from every minute (about 3.5 KB each; already in the logs).
- **Braking, regen vs friction** (Rivian, drives copied off the device): shown once any drive has it. Panels like
  this and the collision close-ups hide themselves on drives without data.

**Sync status:** token expiry, last sync, the software the newest drive ran (named by fork: openpilot, sunnypilot,
Adventure Pilot, ...) and its driving models, how far behind its GitHub branch it was, and with direct device copy, how much driving is still waiting to be
copied as full logs (it turns orange at 8 hours and red at 11, as the device fills up and starts deleting its oldest).

The toggles under the dropdowns shade the graphs by mode (Full / MADS / Manual) and mark distance, collision alerts,
errors and events.

It runs as three containers: `sync` (this code), InfluxDB 2 and Grafana. Everything stays on your machine.
The dashboard is saved in Grafana's v2 format (its panels can hide themselves on drives where they'd be empty), so it
needs Grafana 13.2 or newer: the bundled one is, and so must be any Grafana of your own you point it at.

A community project: not made by or affiliated with comma.ai. See [CHANGELOG.md](CHANGELOG.md) for what's changed.

## Requirements

- Docker with the compose plugin (`docker compose version`)
- Python 3.9+ on the host, only for `./setup.py` (standard library, nothing to install)
- A comma account with a paired device. comma prime isn't required: without it the cloud keeps logs for only a
  few days, so leave this running and it builds up its own archive.

## Setup

```sh
git clone <this repo> comma-analytics && cd comma-analytics
./setup.py                  # asks a few questions, writes .env
docker compose up -d --build
```

Open `http://localhost:3000/d/comma-telemetry`, or whichever port setup chose, and log in as `admin` with the
password setup printed (`GRAFANA_PASSWORD` in `.env`). The first sync pulls up to 30 days that the cloud still
has. Watch it with `docker compose logs -f sync`.

### What setup asks for

| Setting | Where to get it |
| --- | --- |
| **comma token** (required) | Log in at <https://jwt.comma.ai> with the account the device is paired to and copy the token. It lasts about 90 days; the dashboard's "token expires in" tile goes orange at 14 days. To renew: get a new one, re-run `./setup.py` (or edit `COMMA_TOKEN` in `.env`), then `docker compose up -d`. |
| **device** (required) | Setup lists the devices on your account; pick one. |
| **time zone, units, port** | Defaults to this machine's time zone, imperial or metric to match it (US/UK → miles), and the first free port from 3000. Times are stored in UTC and shown in this time zone; distances are stored in miles and converted for display, so switching units later needs no re-import. |
| InfluxDB / Grafana passwords and tokens | Generated for you, kept in `.env`. |
| **driver names** (optional) | Without Home Assistant, every drive gets one name (`DRIVER_NAME`). With it, any HA sensor whose state is the current driver's name labels each drive. On a **Rivian**, that's the *Active Driver* sensor (`sensor.<vehicle>_active_driver`) from the [Rivian (Unofficial)](https://github.com/bretterer/home-assistant-rivian) integration (HACS), which reports the car's active driver profile; other cars need an integration with a similar sensor. You need the HA URL (as seen from docker: use the LAN IP, not `localhost`), a long-lived access token (HA → your profile → Security → *Long-lived access tokens*) and the sensor's entity id. HA only keeps ~10 days of history, so driver changes are saved locally as they're seen. |
| **direct device copy** (optional) | Whenever the device is on your home Wi-Fi (car on or off), the sync copies the full logs (rlogs) of finished segments over SSH, checking every minute; the copy runs at idle priority on the device and resumes where it stopped. You need the device's LAN IP (give it a DHCP reservation) and an SSH key it trusts. The device trusts the keys on a GitHub account: setup makes a key in `secrets/comma_ssh_key`; add its `.pub` at <https://github.com/settings/keys>, then on the device enable SSH and add your GitHub username under SSH Keys. On any car the rlogs show when an off-board Jetlink host's big model was driving, which the quick logs miss. On a **Rivian R1** they also give regen-vs-friction braking, EPS fault detection, the rack's activity on the steering torque graph, position in lane at 10 Hz and the Hands on wheel chart. |

`.env.example` documents every setting if you'd rather fill `.env` in by hand.

## Day to day

```sh
docker compose logs -f sync                 # what it's doing
git pull && docker compose up -d --build    # update; the dashboard is regenerated on start

# one-off commands: stop the daemon first (they share its state file), start it again after
docker compose stop sync
docker compose run --rm sync python sync.py --days 30      # pull further back from the cloud
docker compose run --rm sync python sync.py --reload all   # re-ingest the whole archive (after changing ct/*.py)
docker compose run --rm -e DEVICE_IP=192.168.1.50 sync python sync.py --device --route 00000092
                                            # copy one drive's full logs off the comma now
docker compose start sync
```

- **Your layout is kept.** On each start the sync container regenerates the dashboard but keeps the panel
  positions and sizes you changed in Grafana, and any panels you added. To go back to the default layout:
  `docker compose exec sync python setup_grafana.py --reset-layout`.
- **The dashboard follows your newest drive.** The sync saves the newest drive's time span as the dashboard's default
  time range whenever a drive arrives, so a default range you save in Grafana is replaced at the next drive.
- **`archive/` is the permanent copy of your raw logs.** The database can always be rebuilt from it with
  `--reload all`, so back it up.
- **Changing `INFLUX_*` in `.env` after the first start does nothing:** InfluxDB only reads them on first start.
  To start over: `docker compose down -v` (deletes the database and Grafana, keeps `archive/`), then up again.

## Using your own Grafana

Point the sync at an existing Grafana (13.2+) with a `docker-compose.override.yml` (not tracked; see
`docker-compose.override.example.yml` for mounting tokens and keys from elsewhere):

```yaml
services:
  grafana:
    profiles: [bundled-grafana]         # don't start the bundled one
  influxdb:
    ports: ["127.0.0.1:8096:8086"]      # if your Grafana isn't on the compose network
  sync:
    environment:
      GRAFANA_URL: http://host.docker.internal:3000     # how the sync reaches Grafana
      GRAFANA_TOKEN: ...                                # a service account token (Editor) instead of admin's password
      GRAFANA_INFLUX_URL: http://localhost:8096         # how Grafana reaches InfluxDB
      GRAFANA_PUBLIC_URL: https://grafana.example.com   # only for the link it logs
    extra_hosts:
      - host.docker.internal:host-gateway
```

That Grafana needs `GF_PANELS_DISABLE_SANITIZE_HTML=true` (the bundled one sets it). The top bar's layout, the
drive's Copy button and the event log following the cursor are HTML in text panels, which Grafana strips otherwise.
The event log's following uses Grafana internals, so check it after upgrading Grafana.

## Layout

- `sync.py` is the daemon (cloud pull hourly, device pull every minute) plus one-off commands. See its docstring.
- `setup_grafana.py` sets up the InfluxDB bucket, Grafana's read-only data source and the dashboard. It's
  generated in code; `--print` dumps the JSON.
- `ct/dashboard_v2.py` converts the dashboard to Grafana's v2 format and reads a saved layout back.
- `ct/config.py` reads settings from the environment / `.env` and the secrets.
- `ct/extract.py` decodes qlogs into modes, the per-drive series, events and errors.
- `ct/engage.py` works out when openpilot engaged and what ended each engagement.
- `ct/faults.py` holds the error types, codes and severity.
- `ct/safety.py` finds collision alerts and records the seconds around them.
- `ct/rlog.py` decodes rlogs: the big-model flag on any car, and full-rate CAN on a Rivian (braking, rack activity, wheel touch).
- `ct/device.py` handles the SSH copy off the device.
- `ct/sources.py` talks to the comma API, Home Assistant and GitHub.
- `schema/` holds openpilot's cereal capnp schema and the Rivian DBC. Their source is in `schema/SOURCE`.

## License

MIT, the same as openpilot: see [LICENSE](LICENSE). The files in `schema/` come from openpilot and opendbc (MIT,
Comma.ai); see `schema/SOURCE`.
