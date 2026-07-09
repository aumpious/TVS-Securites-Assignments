#!/usr/bin/env python3
"""
sensor_monitor.py

Simulates a temperature/humidity sensor, keeps the last N readings in
memory, writes every reading to a CSV file, raises an alarm when the
temperature crosses a configurable threshold, and prints the latest
reading to the console.

Design overview
----------------
Two threads are used, coordinated by a threading.Event for clean shutdown:

  1. Producer thread (`sensor_loop`)
       - generates one reading per second
       - appends it to a thread-safe ring buffer (collections.deque)
       - writes it to the CSV file
       - checks it against the alarm threshold and logs an ALARM line

  2. Main thread (`display_loop`)
       - every second, reads the most recent value from the shared
         buffer and prints it to the console

A `threading.Lock` guards the buffer since both threads touch it
(producer appends, display reads). Using a `collections.deque(maxlen=100)`
means "keep only the last 100 readings" is enforced automatically -
older entries are dropped once the deque is full.

This file has no hardware dependency, so it runs identically on any
machine. If real hardware (e.g. a DHT22 on a Raspberry Pi) is attached,
only `SensorSimulator.read()` needs to be replaced with a call into the
sensor's driver library - everything else (buffering, CSV, alarms,
console output, config) stays the same.
"""

import configparser
import csv
import json
import logging
import os
import random
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None  # MQTT becomes a no-op if the library isn't installed

try:
    from flask import Flask, jsonify
except ImportError:
    Flask = None  # REST API becomes a no-op if Flask isn't installed


@dataclass
class Reading:
    """A single temperature/humidity sample."""
    timestamp: str
    temperature_c: float
    humidity_pct: float
    alarm: bool


class Config:
    """Loads and validates settings from config.ini."""

    def __init__(self, path: str):
        parser = configparser.ConfigParser()
        if not parser.read(path):
            raise FileNotFoundError(f"Config file not found: {path}")

        try:
            self.read_interval_sec = parser.getfloat("sensor", "read_interval_sec")
            self.buffer_size = parser.getint("sensor", "buffer_size")

            self.temp_alarm_celsius = parser.getfloat("thresholds", "temp_alarm_celsius")

            self.start_temp = parser.getfloat("simulation", "start_temp_celsius")
            self.start_humidity = parser.getfloat("simulation", "start_humidity_percent")
            self.temp_drift_max = parser.getfloat("simulation", "temp_drift_max")
            self.humidity_drift_max = parser.getfloat("simulation", "humidity_drift_max")
            self.min_temp = parser.getfloat("simulation", "min_temp_celsius")
            self.max_temp = parser.getfloat("simulation", "max_temp_celsius")
            self.min_humidity = parser.getfloat("simulation", "min_humidity_percent")
            self.max_humidity = parser.getfloat("simulation", "max_humidity_percent")

            self.csv_path = parser.get("output", "csv_path")
            self.log_path = parser.get("output", "log_path")
        except (configparser.NoSectionError, configparser.NoOptionError) as exc:
            raise ValueError(f"Invalid config file: {exc}") from exc

        # MQTT is optional -- if the [mqtt] section is missing, it's just disabled
        self.mqtt_enabled = parser.getboolean("mqtt", "enabled", fallback=False)
        self.mqtt_broker = parser.get("mqtt", "broker", fallback="localhost")
        self.mqtt_port = parser.getint("mqtt", "port", fallback=1883)
        self.mqtt_topic = parser.get("mqtt", "topic", fallback="sensor/readings")

        # REST API is optional -- if the [rest_api] section is missing, it's just disabled
        self.rest_api_enabled = parser.getboolean("rest_api", "enabled", fallback=False)
        self.rest_api_host = parser.get("rest_api", "host", fallback="127.0.0.1")
        self.rest_api_port = parser.getint("rest_api", "port", fallback=5000)


class SensorSimulator:
    """
    Generates plausible temperature/humidity values using a bounded
    random walk (each new value = previous value + small random step),
    which looks far more like a real environment than independent
    random numbers every tick.
    """

    def __init__(self, config: Config):
        self.cfg = config
        self.temp = config.start_temp
        self.humidity = config.start_humidity

    def read(self) -> Reading:
        temp_step = random.uniform(-self.cfg.temp_drift_max, self.cfg.temp_drift_max)
        humidity_step = random.uniform(-self.cfg.humidity_drift_max, self.cfg.humidity_drift_max)

        self.temp = min(max(self.temp + temp_step, self.cfg.min_temp), self.cfg.max_temp)
        self.humidity = min(max(self.humidity + humidity_step, self.cfg.min_humidity), self.cfg.max_humidity)

        alarm = self.temp > self.cfg.temp_alarm_celsius
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

        return Reading(
            timestamp=timestamp,
            temperature_c=round(self.temp, 2),
            humidity_pct=round(self.humidity, 2),
            alarm=alarm,
        )


class ReadingBuffer:
    """Thread-safe fixed-size buffer holding the last N readings."""

    def __init__(self, maxlen: int):
        self._buffer = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, reading: Reading) -> None:
        with self._lock:
            self._buffer.append(reading)

    def latest(self) -> Reading | None:
        with self._lock:
            return self._buffer[-1] if self._buffer else None

    def snapshot(self) -> list:
        with self._lock:
            return list(self._buffer)


class CsvWriter:
    """Appends readings to a CSV file, writing the header once."""

    HEADER = ["timestamp", "temperature_c", "humidity_pct", "alarm"]

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        file_is_new = not os.path.exists(path)
        self._lock = threading.Lock()
        if file_is_new:
            with open(self.path, "w", newline="") as f:
                csv.writer(f).writerow(self.HEADER)

    def write(self, reading: Reading) -> None:
        with self._lock:
            with open(self.path, "a", newline="") as f:
                csv.writer(f).writerow([
                    reading.timestamp,
                    reading.temperature_c,
                    reading.humidity_pct,
                    reading.alarm,
                ])


class MqttPublisher:
    """
    Thin wrapper around paho-mqtt. If MQTT is disabled in config, or the
    paho-mqtt library isn't installed, or the broker can't be reached,
    this simply logs a warning and continues -- it never crashes the
    sensor loop just because the network/broker is unavailable.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        self.enabled = config.mqtt_enabled and mqtt is not None
        self.topic = config.mqtt_topic
        self.logger = logger
        self.client = None

        if config.mqtt_enabled and mqtt is None:
            logger.warning("MQTT enabled in config but paho-mqtt is not installed. "
                            "Run: pip install paho-mqtt")
            self.enabled = False
            return

        if self.enabled:
            try:
                # paho-mqtt v2+ requires an explicit callback API version.
                # Fall back to the old-style constructor for paho-mqtt v1.
                if hasattr(mqtt, "CallbackAPIVersion"):
                    self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                else:
                    self.client = mqtt.Client()
                self.client.connect(config.mqtt_broker, config.mqtt_port, keepalive=60)
                self.client.loop_start()  # handles network traffic in the background
                logger.info("MQTT connected to %s:%d, publishing to '%s'",
                            config.mqtt_broker, config.mqtt_port, self.topic)
            except Exception as exc:
                logger.warning("MQTT connection failed (%s). Continuing without MQTT.", exc)
                self.enabled = False
                self.client = None

    def publish(self, reading: Reading) -> None:
        if not self.enabled or self.client is None:
            return
        payload = json.dumps({
            "timestamp": reading.timestamp,
            "temperature_c": reading.temperature_c,
            "humidity_pct": reading.humidity_pct,
            "alarm": reading.alarm,
        })
        try:
            self.client.publish(self.topic, payload)
        except Exception as exc:
            self.logger.warning("MQTT publish failed: %s", exc)

    def close(self) -> None:
        if self.client is not None:
            self.client.loop_stop()
            self.client.disconnect()


class RestApiServer:
    """
    Small Flask server exposing the sensor data over HTTP.

      GET /latest   -> the single most recent reading
      GET /history  -> up to the last 100 readings currently in the buffer

    Runs in its own background thread so it never blocks the sensor loop.
    If Flask isn't installed, or REST API is disabled in config, this
    class simply does nothing -- it never crashes the rest of the program.
    """

    def __init__(self, config: Config, buffer: "ReadingBuffer", logger: logging.Logger):
        self.enabled = config.rest_api_enabled and Flask is not None
        self.host = config.rest_api_host
        self.port = config.rest_api_port
        self.logger = logger
        self.buffer = buffer
        self.thread = None

        if config.rest_api_enabled and Flask is None:
            logger.warning("REST API enabled in config but Flask is not installed. "
                            "Run: pip install flask")
            self.enabled = False

    @staticmethod
    def _reading_to_dict(reading: "Reading") -> dict:
        return {
            "timestamp": reading.timestamp,
            "temperature_c": reading.temperature_c,
            "humidity_pct": reading.humidity_pct,
            "alarm": reading.alarm,
        }

    def start(self) -> None:
        if not self.enabled:
            return

        app = Flask(__name__)

        @app.route("/latest")
        def latest():
            reading = self.buffer.latest()
            if reading is None:
                return jsonify({"error": "no readings yet"}), 404
            return jsonify(self._reading_to_dict(reading))

        @app.route("/history")
        def history():
            readings = self.buffer.snapshot()
            return jsonify([self._reading_to_dict(r) for r in readings])

        def run_app():
            try:
                # use_reloader=False is required: the reloader would try to
                # restart the whole process, which breaks our threading setup
                app.run(host=self.host, port=self.port, use_reloader=False)
            except Exception as exc:
                self.logger.error("REST API failed to start: %s", exc)
                print(f"REST API FAILED TO START: {exc}")

        self.thread = threading.Thread(target=run_app, daemon=True)
        self.thread.start()
        self.logger.info("REST API thread launched, attempting to bind to http://%s:%d",
                          self.host, self.port)


def setup_logger(log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    logger = logging.getLogger("sensor_monitor")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    ))
    logger.addHandler(file_handler)
    return logger


def sensor_loop(
    simulator: SensorSimulator,
    buffer: ReadingBuffer,
    csv_writer: CsvWriter,
    mqtt_publisher: "MqttPublisher",
    logger: logging.Logger,
    interval: float,
    stop_event: threading.Event,
) -> None:
    """Producer thread: generate -> store -> persist -> alarm-check, once per interval."""
    while not stop_event.is_set():
        reading = simulator.read()
        buffer.append(reading)
        csv_writer.write(reading)
        mqtt_publisher.publish(reading)

        if reading.alarm:
            logger.warning(
                "ALARM: temperature %.2f C exceeded threshold at %s",
                reading.temperature_c, reading.timestamp,
            )
        else:
            logger.info(
                "Reading OK: temp=%.2fC humidity=%.2f%%",
                reading.temperature_c, reading.humidity_pct,
            )

        stop_event.wait(interval)


def display_loop(buffer: ReadingBuffer, interval: float, stop_event: threading.Event) -> None:
    """Main-thread loop: print the latest reading to the console each interval."""
    while not stop_event.is_set():
        reading = buffer.latest()
        if reading is not None:
            status = "ALARM!" if reading.alarm else "OK"
            print(
                f"[{reading.timestamp}] Temp: {reading.temperature_c:6.2f} C | "
                f"Humidity: {reading.humidity_pct:6.2f} % | Status: {status}"
            )
        stop_event.wait(interval)


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.ini"
    config = Config(config_path)
    logger = setup_logger(config.log_path)

    logger.info("Sensor monitor starting up (interval=%ss, buffer=%d, alarm_threshold=%.1fC)",
                config.read_interval_sec, config.buffer_size, config.temp_alarm_celsius)

    simulator = SensorSimulator(config)
    buffer = ReadingBuffer(maxlen=config.buffer_size)
    csv_writer = CsvWriter(config.csv_path)
    mqtt_publisher = MqttPublisher(config, logger)
    rest_api = RestApiServer(config, buffer, logger)
    rest_api.start()
    stop_event = threading.Event()

    producer = threading.Thread(
        target=sensor_loop,
        args=(simulator, buffer, csv_writer, mqtt_publisher, logger, config.read_interval_sec, stop_event),
        daemon=True,
    )
    producer.start()

    print("Sensor monitor running. Press Ctrl+C to stop.\n")
    try:
        display_loop(buffer, config.read_interval_sec, stop_event)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        stop_event.set()
        producer.join(timeout=2)
        mqtt_publisher.close()
        logger.info("Sensor monitor stopped. Total readings buffered: %d", len(buffer.snapshot()))
        print("Stopped. Readings saved to:", config.csv_path)


if __name__ == "__main__":
    main()
