# Changelog

What changed in each update, newest first. To update, run
`git pull && docker compose up -d --build`.

## 2026-11-02

**After updating:** drives you already have won't show the new lane and steering data until you re-import them.
This is optional. Run the three commands below; the full logs then re-import in the background, which can take a
couple of hours with a large archive.

```sh
docker compose stop sync
docker compose run --rm sync python sync.py --reload all
docker compose start sync
```

### Added

- **Position in lane** graph, replacing Steering tracking: where the car sat between the lane lines, with stretches
  marked where a missing line had to be estimated. 10 Hz on Rivian drives copied off the device, 2 Hz otherwise.
- **Rack ignoring torque** error (Rivian, drives copied off the device): the steering rack silently not applying
  openpilot's torque.

### Changed

- **Steering torque** graph: on a Rivian it now shows whether the rack was applying openpilot's torque. EPS torque
  only appears when the car reports it.
- **Position in lane** sits directly below Steering torque. If you've moved panels around, your layout is kept; use
  `--reset-layout` to get the new order (see the README).
- **Device copy** now runs whenever the device is on your home Wi-Fi, car on or off, at low priority on the device.
  The `--now` option is gone because it's no longer needed.

### Fixed

- A damaged or incomplete log segment no longer stops the cloud sync: it's skipped and noted in the log. Thanks to
  [@Nimijneb](https://github.com/Nimijneb) (#1).
- Drives with no driving model selected were hidden when the model dropdowns were set to All.
- The Mode timeline's errors row was missing.
- Position in lane now draws partly copied drives as lines rather than scattered points.

## 2026-10-11

First public release.
