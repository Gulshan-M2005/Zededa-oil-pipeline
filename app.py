import os
import csv
import time
import yaml
import json
import random
import logging
import threading
from datetime import datetime

from flask import Flask, send_from_directory, jsonify

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False

try:
    from twilio.rest import Client as TwilioClient
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("OilPipelineMonitor")


class PipelinePressureMonitor:
    VERSION = "v1.7"

    def __init__(self):
        logger.info("=" * 70)
        logger.info("OIL PIPELINE PRESSURE MONITORING SYSTEM")
        logger.info("ZEDEDA EDGE APPLICATION %s", self.VERSION)
        logger.info("=" * 70)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.join(self.base_dir, "config.yaml")
        self.audit_file = os.path.join(self.base_dir, "audit_log.csv")

        self.config = self.load_config()

        self.sampling_rate = float(
            self.config.get("sampling_rate_hz", 1.0)
        )
        if self.sampling_rate <= 0:
            self.sampling_rate = 1.0

        self.sample_interval = 1.0 / self.sampling_rate

        self.stations = ["INLET", "MIDLINE", "OUTLET"]

        self.latest_pressure = {
            "INLET": 100.0,
            "MIDLINE": 98.0,
            "OUTLET": 97.0
        }

        self.previous_pressure = {
            "INLET": 100.0,
            "MIDLINE": 98.0,
            "OUTLET": 97.0
        }

        self.latest_temperature = {
            "INLET": 30.0,
            "MIDLINE": 30.0,
            "OUTLET": 30.0
        }

        self.latest_dp_dt = {
            "INLET": 0.0,
            "MIDLINE": 0.0,
            "OUTLET": 0.0
        }

        self.stuck_count = {
            "INLET": 0,
            "MIDLINE": 0,
            "OUTLET": 0
        }

        self.sensor_valid = {
            "INLET": True,
            "MIDLINE": True,
            "OUTLET": True
        }

        self.alarm_state = {
            "INLET": "INFO",
            "MIDLINE": "INFO",
            "OUTLET": "INFO"
        }

        self.pending_alarm = {
            "INLET": None,
            "MIDLINE": None,
            "OUTLET": None
        }

        self.pending_count = {
            "INLET": 0,
            "MIDLINE": 0,
            "OUTLET": 0
        }

        self.current_cycle = 0

        self.mqtt_client = None
        self.mqtt_connected = False
        self.setup_mqtt()

        self.twilio_client = None
        self.setup_twilio()

        self.web = Flask(__name__, static_folder=None)
        self.setup_dashboard()

        self.setup_audit_log()

        logger.info("Application initialization completed.")

    def load_config(self):
        try:
            with open(self.config_file, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)

            if config is None:
                config = {}

            logger.info("Configuration loaded from %s", self.config_file)
            return config

        except Exception as error:
            logger.error("Unable to load config.yaml: %s", error)
            return {}

    def setup_audit_log(self):
        file_exists = os.path.exists(self.audit_file)

        try:
            if not file_exists:
                with open(
                    self.audit_file,
                    "w",
                    newline="",
                    encoding="utf-8"
                ) as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        "Timestamp",
                        "Event Type",
                        "Station",
                        "Pressure PSI",
                        "Temperature C",
                        "dP/dt",
                        "Severity",
                        "Message",
                        "Horn",
                        "Beacon",
                        "Pipeline Status"
                    ])

                logger.info("Created audit_log.csv")

        except Exception as error:
            logger.error("Audit log setup failed: %s", error)

    def write_audit(
        self,
        event_type,
        station,
        pressure,
        temperature,
        dp_dt,
        severity,
        message,
        horn=False,
        beacon=False
    ):
        if severity == "CRITICAL":
            pipeline_status = "CRITICAL"
        elif severity == "WARNING":
            pipeline_status = "WARNING"
        else:
            pipeline_status = "NORMAL"

        try:
            with open(
                self.audit_file,
                "a",
                newline="",
                encoding="utf-8"
            ) as file:
                writer = csv.writer(file)
                writer.writerow([
                    datetime.now().isoformat(),
                    event_type,
                    station,
                    round(pressure, 2),
                    round(temperature, 2),
                    round(dp_dt, 2),
                    severity,
                    message,
                    horn,
                    beacon,
                    pipeline_status
                ])

        except Exception as error:
            logger.error("Unable to write audit log: %s", error)

    def setup_mqtt(self):
        mqtt_config = self.config.get("mqtt", {})

        if not mqtt_config.get("enabled", True):
            logger.info("MQTT disabled in configuration.")
            return

        if not MQTT_AVAILABLE:
            logger.warning("paho-mqtt is not installed.")
            return

        broker = mqtt_config.get("broker", "test.mosquitto.org")
        port = int(mqtt_config.get("port", 1883))

        try:
            try:
                self.mqtt_client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION1
                )
            except Exception:
                self.mqtt_client = mqtt.Client()

            self.mqtt_client.on_connect = self.on_mqtt_connect
            self.mqtt_client.on_disconnect = self.on_mqtt_disconnect

            logger.info(
                "Connecting MQTT broker: %s:%s",
                broker,
                port
            )

            self.mqtt_client.connect(
                broker,
                port,
                keepalive=60
            )

            self.mqtt_client.loop_start()

        except Exception as error:
            logger.error("MQTT connection failed: %s", error)

    def on_mqtt_connect(
        self,
        client,
        userdata,
        flags,
        rc,
        properties=None
    ):
        if rc == 0:
            self.mqtt_connected = True
            logger.info("MQTT connected successfully.")
        else:
            self.mqtt_connected = False
            logger.warning("MQTT connection failed. RC=%s", rc)

    def on_mqtt_disconnect(
        self,
        client,
        userdata,
        rc,
        properties=None
    ):
        self.mqtt_connected = False
        logger.warning("MQTT disconnected. RC=%s", rc)

    def publish_mqtt(
        self,
        station,
        pressure,
        temperature,
        dp_dt,
        severity,
        message,
        event_type="READING"
    ):
        mqtt_config = self.config.get("mqtt", {})

        if not mqtt_config.get("enabled", True):
            return

        if self.mqtt_client is None:
            return

        topic = mqtt_config.get(
            "topic",
            "pipeline/mohamedammagulshan/alerts"
        )

        horn = severity == "CRITICAL"
        beacon = severity in ["WARNING", "CRITICAL"]

        if severity == "CRITICAL":
            pipeline_status = "CRITICAL"
        elif severity == "WARNING":
            pipeline_status = "WARNING"
        else:
            pipeline_status = "NORMAL"

        payload = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "station": station,
            "pressure_psi": round(pressure, 2),
            "temperature_c": round(temperature, 2),
            "dp_dt": round(dp_dt, 2),
            "severity": severity,
            "message": message,
            "horn": horn,
            "beacon": beacon,
            "status": severity,
            "pipeline_status": pipeline_status,
            "sensor_valid": self.sensor_valid.get(station, True),
            "stuck_count": self.stuck_count.get(station, 0),
            "cycle": self.current_cycle
        }

        try:
            self.mqtt_client.publish(
                topic,
                json.dumps(payload),
                qos=0,
                retain=False
            )
        except Exception as error:
            logger.error("MQTT publish failed: %s", error)

    def setup_twilio(self):
        sms_config = self.config.get("sms", {})

        if not sms_config.get("enabled", False):
            logger.info("SMS escalation disabled.")
            return

        account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN")

        if not account_sid or not auth_token:
            logger.warning("Twilio credentials not found.")
            logger.warning(
                "Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN "
                "for real SMS."
            )
            return

        if not TWILIO_AVAILABLE:
            logger.warning("Twilio package not installed.")
            return

        try:
            self.twilio_client = TwilioClient(
                account_sid,
                auth_token
            )
            logger.info("Twilio SMS client initialized successfully.")

        except Exception as error:
            logger.error("Twilio setup failed: %s", error)

    def send_sms(self, station, severity, message):
        sms_config = self.config.get("sms", {})

        if not sms_config.get("enabled", False):
            return

        phone_number = sms_config.get("phone_number", "")

        if not phone_number:
            logger.warning("SMS phone number not configured.")
            return

        pressure = self.latest_pressure.get(station, 0)
        temperature = self.latest_temperature.get(station, 0)

        sms_message = (
            "OIL PIPELINE CRITICAL ALERT\n\n"
            f"Station      : {station}\n"
            f"Severity     : {severity}\n"
            f"Pressure     : {pressure:.2f} PSI\n"
            f"Temperature  : {temperature:.2f} C\n\n"
            f"Issue:\n{message}\n\n"
            f"Time:\n{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n\n"
            "ZEDEDA Edge Pipeline Monitoring System"
        )

        if self.twilio_client is None:
            logger.error(
                "REAL SMS NOT SENT: Twilio client unavailable."
            )
            return

        from_number = os.getenv("TWILIO_FROM_NUMBER")

        if not from_number:
            logger.error(
                "REAL SMS NOT SENT: TWILIO_FROM_NUMBER not configured."
            )
            return

        try:
            result = self.twilio_client.messages.create(
                body="sms_internal_alerts",
                from_=from_number,
                to=phone_number
            )

            logger.warning("=" * 60)
            logger.warning("REAL CRITICAL SMS SENT")
            logger.warning(
                "Station=%s | SID=%s",
                station,
                result.sid
            )
            logger.warning("=" * 60)

        except Exception as error:
            logger.error(
                "REAL SMS SENDING FAILED: %s",
                error
            )

    def validate_pressure(self, station, pressure):
        validation = self.config.get("validation", {})

        min_valid = float(
            validation.get("min_valid_psi", 0)
        )

        max_valid = float(
            validation.get("max_valid_psi", 200)
        )

        max_spike = float(
            validation.get("max_spike_psi", 60)
        )

        stuck_limit = int(
            validation.get("stuck_count_limit", 5)
        )

        previous = self.latest_pressure.get(
            station,
            pressure
        )

        if pressure < min_valid or pressure > max_valid:
            self.sensor_valid[station] = False

            logger.error(
                "%s SENSOR INVALID: %.2f PSI "
                "(outside %.2f - %.2f)",
                station,
                pressure,
                min_valid,
                max_valid
            )

            return False, previous

        difference = abs(pressure - previous)

        if difference > max_spike:
            demo_cycles = (
                20 <= self.current_cycle <= 23
                or 30 <= self.current_cycle <= 33
                or 40 <= self.current_cycle <= 43
                or 50 <= self.current_cycle <= 53
                or 60 <= self.current_cycle <= 63
            )

            if not demo_cycles:
                self.sensor_valid[station] = False

                logger.warning(
                    "%s SPIKE REJECTED: %.2f -> %.2f PSI",
                    station,
                    previous,
                    pressure
                )

                return False, previous

        if abs(pressure - previous) < 0.01:
            self.stuck_count[station] += 1
        else:
            self.stuck_count[station] = 0

        if self.stuck_count[station] >= stuck_limit:
            self.sensor_valid[station] = False

            logger.warning(
                "%s SENSOR STUCK. Count=%s",
                station,
                self.stuck_count[station]
            )

            return False, pressure

        self.sensor_valid[station] = True
        return True, pressure

    def calculate_dp_dt(self, station, current_pressure):
        previous = self.previous_pressure.get(
            station,
            current_pressure
        )

        dp_dt = (
            current_pressure - previous
        ) / self.sample_interval

        return dp_dt

    def calculate_differential_pressure(self):
        inlet = self.latest_pressure["INLET"]
        midline = self.latest_pressure["MIDLINE"]
        outlet = self.latest_pressure["OUTLET"]

        return {
            "INLET_MIDLINE": abs(inlet - midline),
            "MIDLINE_OUTLET": abs(midline - outlet)
        }

    def is_shelved(self, station):
        shelving = self.config.get(
            "alarm_shelving",
            {}
        )

        if not shelving.get("enabled", False):
            return False

        if shelving.get("maintenance_mode", False):
            return True

        if shelving.get("pigging_mode", False):
            return True

        station_settings = shelving.get(
            station,
            {}
        )

        if isinstance(station_settings, dict):
            if station_settings.get("shelved", False):
                return True

        return False

    def evaluate_rules(self, station, pressure, dp_dt):
        limits = self.config.get("limits", {})
        surge = self.config.get("surge_rules", {})
        differential_config = self.config.get(
            "differential_pressure",
            {}
        )
        filter_config = self.config.get("filter", {})

        lolo = float(limits.get("lolo", 30))
        lo = float(limits.get("lo", 60))
        hi = float(limits.get("hi", 130))
        hihi = float(limits.get("hihi", 150))

        dp_dt_max = float(
            surge.get("dp_dt_max", 20)
        )

        differential_warning = float(
            differential_config.get("warning_psi", 30)
        )

        differential_critical = float(
            differential_config.get("critical_psi", 50)
        )

        deadband = float(
            filter_config.get("deadband_psi", 3)
        )

        if self.is_shelved(station):
            return (
                "INFO",
                "Alarm shelving active - "
                "expected excursion suppressed"
            )

        if pressure >= hihi:
            return (
                "CRITICAL",
                f"HIHI pressure exceeded: "
                f"{pressure:.2f} PSI >= {hihi:.2f} PSI"
            )

        if pressure <= lolo:
            return (
                "CRITICAL",
                f"LOLO pressure exceeded: "
                f"{pressure:.2f} PSI <= {lolo:.2f} PSI"
            )

        if abs(dp_dt) >= dp_dt_max:
            direction = (
                "rapid pressure rise"
                if dp_dt > 0
                else "rapid pressure drop"
            )

            return (
                "CRITICAL",
                f"{direction}: "
                f"dP/dt = {dp_dt:.2f} PSI/s"
            )

        if pressure >= hi:
            return (
                "WARNING",
                f"HI pressure exceeded: "
                f"{pressure:.2f} PSI >= {hi:.2f} PSI"
            )

        if pressure >= hi - deadband:
            return (
                "INFO",
                f"Pressure approaching HI limit: "
                f"{pressure:.2f} PSI"
            )

        if pressure <= lo:
            return (
                "WARNING",
                f"LO pressure exceeded: "
                f"{pressure:.2f} PSI <= {lo:.2f} PSI"
            )

        if pressure <= lo + deadband:
            return (
                "INFO",
                f"Pressure approaching LO limit: "
                f"{pressure:.2f} PSI"
            )

        differential = self.calculate_differential_pressure()

        if station == "OUTLET":
            max_differential = max(
                differential.values()
            )

            if max_differential >= differential_critical:
                return (
                    "CRITICAL",
                    "Critical differential pressure: "
                    f"{max_differential:.2f} PSI"
                )

            if max_differential >= differential_warning:
                return (
                    "WARNING",
                    "High differential pressure: "
                    f"{max_differential:.2f} PSI"
                )

        return (
            "INFO",
            "Pressure normal"
        )

    def control_horn_beacon(self, station, severity):
        if severity == "CRITICAL":
            logger.critical(
                "HORN ON | BEACON ON | Station=%s",
                station
            )
            return True, True

        if severity == "WARNING":
            logger.warning(
                "WARNING INDICATION | Station=%s",
                station
            )
            return False, True

        logger.info(
            "HORN OFF | BEACON OFF | Station=%s",
            station
        )

        return False, False

    def handle_alarm_transition(
        self,
        station,
        severity,
        message
    ):
        current_state = self.alarm_state[station]

        if severity == current_state:
            self.pending_alarm[station] = None
            self.pending_count[station] = 0
            return

        filter_config = self.config.get("filter", {})

        debounce_seconds = float(
            filter_config.get("debounce_seconds", 3)
        )

        required_samples = max(
            1,
            int(debounce_seconds * self.sampling_rate)
        )

        if self.pending_alarm[station] != severity:
            self.pending_alarm[station] = severity
            self.pending_count[station] = 1

            logger.info(
                "%s pending alarm: %s (1/%s)",
                station,
                severity,
                required_samples
            )

            return

        self.pending_count[station] += 1

        logger.info(
            "%s debounce: %s (%s/%s)",
            station,
            severity,
            self.pending_count[station],
            required_samples
        )

        if self.pending_count[station] < required_samples:
            return

        self.alarm_state[station] = severity
        self.pending_alarm[station] = None
        self.pending_count[station] = 0

        horn, beacon = self.control_horn_beacon(
            station,
            severity
        )

        pressure = self.latest_pressure[station]
        temperature = self.latest_temperature[station]
        dp_dt = self.latest_dp_dt[station]

        logger.warning(
            "ALARM TRANSITION | %s | %s | %s",
            station,
            severity,
            message
        )

        self.write_audit(
            event_type="ALARM",
            station=station,
            pressure=pressure,
            temperature=temperature,
            dp_dt=dp_dt,
            severity=severity,
            message=message,
            horn=horn,
            beacon=beacon
        )

        self.publish_mqtt(
            station=station,
            pressure=pressure,
            temperature=temperature,
            dp_dt=dp_dt,
            severity=severity,
            message=message,
            event_type="ALARM"
        )

        if severity == "CRITICAL":
            self.send_sms(
                station,
                severity,
                message
            )

    def generate_pressure(self, station):
        cycle = self.current_cycle

        if station == "INLET":
            pressure = 100 + random.uniform(-1, 1)
        elif station == "MIDLINE":
            pressure = 98 + random.uniform(-1, 1)
        else:
            pressure = 97 + random.uniform(-1, 1)

        if 20 <= cycle <= 23:
            if station == "INLET":
                pressure = 135

        if 30 <= cycle <= 33:
            if station == "INLET":
                pressure = 155

        if 40 <= cycle <= 43:
            if station == "OUTLET":
                pressure = 25

        if 50 <= cycle <= 53:
            if station == "INLET":
                pressure = 145

        if 60 <= cycle <= 63:
            if station == "INLET":
                pressure = 150
            elif station == "MIDLINE":
                pressure = 100
            elif station == "OUTLET":
                pressure = 98

        return pressure

    def generate_temperature(self, station):
        temperature = random.uniform(28, 35)

        if 70 <= self.current_cycle <= 73:
            temperature = 105

        return temperature

    def process_cycle(self):
        current_readings = {}

        for station in self.stations:
            raw_pressure = self.generate_pressure(station)
            temperature = self.generate_temperature(station)

            valid, pressure = self.validate_pressure(
                station,
                raw_pressure
            )

            if not valid:
                pressure = self.latest_pressure[station]

            current_readings[station] = {
                "pressure": pressure,
                "temperature": temperature,
                "valid": valid
            }

        for station in self.stations:
            self.previous_pressure[station] = (
                self.latest_pressure[station]
            )

            self.latest_pressure[station] = (
                current_readings[station]["pressure"]
            )

            self.latest_temperature[station] = (
                current_readings[station]["temperature"]
            )

        for station in self.stations:
            pressure = self.latest_pressure[station]

            dp_dt = self.calculate_dp_dt(
                station,
                pressure
            )

            self.latest_dp_dt[station] = dp_dt

        for station in self.stations:
            pressure = self.latest_pressure[station]
            temperature = self.latest_temperature[station]
            dp_dt = self.latest_dp_dt[station]

            severity, message = self.evaluate_rules(
                station,
                pressure,
                dp_dt
            )

            logger.info(
                "%-8s | Pressure=%7.2f PSI | "
                "Temp=%6.2f C | dP/dt=%7.2f | "
                "Severity=%s",
                station,
                pressure,
                temperature,
                dp_dt,
                severity
            )

            self.write_audit(
                event_type="READING",
                station=station,
                pressure=pressure,
                temperature=temperature,
                dp_dt=dp_dt,
                severity=severity,
                message=message,
                horn=False,
                beacon=False
            )

            self.publish_mqtt(
                station=station,
                pressure=pressure,
                temperature=temperature,
                dp_dt=dp_dt,
                severity=severity,
                message=message,
                event_type="READING"
            )

            self.handle_alarm_transition(
                station,
                severity,
                message
            )

    def run_simulation(self, cycles=120):
        logger.info("Starting pipeline simulation...")
        logger.info(
            "Sampling rate: %.2f Hz",
            self.sampling_rate
        )

        for cycle in range(1, cycles + 1):
            self.current_cycle = cycle

            logger.info("")
            logger.info("=" * 70)
            logger.info(
                "PIPELINE CYCLE %s / %s",
                cycle,
                cycles
            )
            logger.info("=" * 70)

            cycle_start = time.time()

            self.process_cycle()

            elapsed = time.time() - cycle_start

            remaining = max(
                0,
                self.sample_interval - elapsed
            )

            time.sleep(remaining)

        logger.info("Simulation cycles completed.")

    def setup_dashboard(self):
        @self.web.route("/")
        def dashboard():
            return send_from_directory(
                self.base_dir,
                "dashboard.html"
            )

        @self.web.route("/health")
        def health():
            return jsonify({
                "status": "running",
                "application": "Oil Pipeline Pressure Monitoring",
                "version": self.VERSION,
                "mqtt_connected": self.mqtt_connected,
                "cycle": self.current_cycle,
                "stations": self.stations
            })

        @self.web.route("/api/status")
        def api_status():
            return jsonify({
                "version": self.VERSION,
                "mqtt_connected": self.mqtt_connected,
                "cycle": self.current_cycle,
                "pressure": self.latest_pressure,
                "temperature": self.latest_temperature,
                "dp_dt": self.latest_dp_dt,
                "severity": self.alarm_state,
                "sensor_valid": self.sensor_valid,
                "stuck_count": self.stuck_count,
                "differential": self.calculate_differential_pressure()
            })

    def start_dashboard(self):
        logger.info(
            "Starting SCADA dashboard on port 5000..."
        )

        self.web.run(
            host="0.0.0.0",
            port=5000,
            debug=False,
            use_reloader=False,
            threaded=True
        )

    def shutdown(self):
        logger.info("Shutting down application...")

        if self.mqtt_client is not None:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                pass

        logger.info("Application shutdown completed.")


def main():
    monitor = PipelinePressureMonitor()

    dashboard_thread = threading.Thread(
        target=monitor.start_dashboard,
        daemon=True
    )

    dashboard_thread.start()

    try:
        monitor.run_simulation(cycles=120)

        logger.info("Simulation finished.")
        logger.info("SCADA dashboard remains active.")

        while True:
            time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")

    except Exception as error:
        logger.exception(
            "Application error: %s",
            error
        )

    finally:
        monitor.shutdown()


if __name__ == "__main__":
    main()