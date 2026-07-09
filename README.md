# Assignment 1 – Programming (Temperature & Humidity Sensor Monitor)

## Programming Language
Python 3, with two optional third-party packages for the bonus features
(`paho-mqtt` for MQTT, `flask` for the REST API). The core program runs
fine on the standard library alone if those two are not installed —
both features degrade gracefully to "disabled" instead of crashing.

## What it does
- Generates a simulated temperature/humidity reading every second (bounded
  random walk, so values drift realistically instead of jumping randomly).
- Keeps the **last 100 readings** in memory using a fixed-size
  `collections.deque`.
- Raises an **alarm** when temperature exceeds a threshold set in
  `config.ini`.
- Appends every reading to `output/readings.csv`.
- Prints the latest reading to the console once per second.
- Logs every reading (and alarms as WARNING) to `output/sensor_monitor.log`.
- Uses two threads (producer generates/stores/logs/publishes data, main
  thread displays it) to show non-blocking, concurrent design — the
  "Multithreading" bonus item.
- **MQTT (bonus):** publishes every reading as JSON to a public MQTT
  broker (`test.mosquitto.org`) on topic `tvs_assignment/sensor`.
- **REST API (bonus):** runs a small Flask server in its own background
  thread exposing:
  - `GET /latest` – the single most recent reading
  - `GET /history` – up to the last 100 readings currently in the buffer

## Build / Run Instructions
```bash
# Requires Python 3.10+ (uses the `X | None` type hint syntax)
pip install paho-mqtt flask   # only needed for the MQTT / REST API bonuses
python3 sensor_monitor.py config.ini
```

Press `Ctrl+C` to stop. On exit it flushes the log and prints where the
CSV was saved.

To point it at a different config file, pass the path as the first
argument. If no argument is given, it defaults to `config.ini` in the
current directory.

While the program is running, the latest reading can be viewed live at:
http://127.0.0.1:8899/latest
http://127.0.0.1:8899/history

## Configuration (`config.ini`)
| Section | Key | Meaning |
|---|---|---|
| sensor | read_interval_sec | Seconds between readings |
| sensor | buffer_size | Max readings kept in memory (100 per spec) |
| thresholds | temp_alarm_celsius | Temperature above which an alarm fires |
| simulation | start_temp_celsius / start_humidity_percent | Starting values |
| simulation | temp_drift_max / humidity_drift_max | Max change per tick |
| simulation | min/max_temp_celsius, min/max_humidity_percent | Clamp bounds |
| output | csv_path | Where readings are appended |
| output | log_path | Where the log file is written |
| mqtt | enabled | Set `true` to publish readings over MQTT |
| mqtt | broker / port / topic | MQTT broker connection details |
| rest_api | enabled | Set `true` to expose readings over HTTP |
| rest_api | host / port | Address the Flask server binds to |

## Assumptions
- No physical sensor hardware is available, so readings are simulated in
  software as specified in the assignment ("If you do not own any
  hardware, you may simulate the solution").
- A real deployment would replace only `SensorSimulator.read()` with a
  call into a hardware driver (e.g. Adafruit's DHT library on a
  Raspberry Pi) — everything else (buffering, CSV writing, alarms,
  logging, config, MQTT, REST API) is hardware-agnostic and would not
  change.
- "Last 100 readings" is interpreted as an in-memory rolling window
  (all readings are still persisted to CSV, so history isn't lost).
- Alarm handling is interpreted as: log a WARNING-level entry and flag
  the console/CSV/MQTT/REST row. No external notification (SMS/email)
  was requested, so none is implemented.
- `test.mosquitto.org` is used as a convenient public MQTT broker for
  demonstration; in production this would point to a private broker.

## Dependencies
- Standard library: `configparser`, `csv`, `logging`, `threading`,
  `collections`, `dataclasses`, `datetime`, `json`.
- Optional (for bonuses): `paho-mqtt`, `flask`. If either is missing,
  the corresponding feature logs a warning and is disabled — the rest
  of the program is unaffected.

## Known Limitations
- Simulated data only — not connected to real hardware.
- Alarm state is per-reading (not debounced), so a single noisy spike
  above the threshold will log one alarm entry; there's no hysteresis
  or "sustained for N seconds" logic.
- CSV grows unbounded over long runs (by design — the 100-reading limit
  applies only to the in-memory buffer, not the persisted history).
- The REST API uses Flask's built-in development server, which is fine
  for this assignment but is explicitly not recommended for production
  use (Flask itself warns about this on startup).
- Default REST API port is `8899` rather than the more common `5000` or
  `5050`, since those were blocked by local corporate security software
  during testing; the port is fully configurable via `config.ini`.
- MQTT publishing has no delivery confirmation/retry logic beyond what
  `paho-mqtt`'s client provides by default.

## Files
assignment1/
├── sensor_monitor.py   # main program
├── config.ini          # configuration
├── README.md
└── output/
├── readings.csv       # generated at runtime
└── sensor_monitor.log # generated at runtime
