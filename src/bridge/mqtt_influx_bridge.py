"""
MQTT to InfluxDB data transfer bridge.

This module implements the subscription and persistence logic for the system.
Note: OS signal handling (SIGINT/SIGTERM) is delegated to the caller 
(main_subscriber.py) to enable coordinated shutdowns.
"""
import logging
import threading
import time
import uuid

import msgpack
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from core import DataPoint
from config import MQTTSettings, InfluxSettings


class MQTTInfluxBridge:
    """
    Manager of the connection and data flow between MQTT and InfluxDB.

    Responsibilities:
        * Subscription to healthcare/# topics
        * Deserialization of MessagePack payloads (batches of DataPoint)
        * Writing to InfluxDB with local retry

    Architectural rationale (threading & backpressure):
        The bridge intentionally operates on the Paho thread loop, performing 
        blocking writes to InfluxDB. This creates a natural backpressure chain:
        1. TCP Buffer (kernel): by blocking the loop, Paho stops calling recv(). 
           The kernel's TCP buffer fills up, the TCP window closes, and the 
           broker naturally slows down data delivery at the transport level.
        2. QoS1 + no-ACK (application level): the broker retains unacknowledged 
           messages and stops delivering new ones once the in-flight window is 
           reached.
        Note: The absence of internal queues is intentional. Adding them would 
        silently break this mechanism by causing premature ACKs for data that 
        has not yet been persisted.

    Reliability policy:
        * QoS1 + manual_ack: ACK only after successful write to InfluxDB
        * If write fails: NO ACK => broker redelivery (at-least-once)
        * If payload/data is invalid: ACK and drop (avoids infinite retry on "poison" messages)
        * After MAX_CONSECUTIVE_INFLUX_FAILURES write failures: stop() (controlled auto-shutdown)

    Usage:
        * bridge.run()  Blocks until stop() is called or a fatal error occurs
    """

    # Configuration
    MQTT_QOS = 1
    MQTT_KEEPALIVE = 60    # Paho describes this as Int
    INFLUX_MAX_RETRIES = 3
    INFLUX_RETRY_BASE_DELAY = 0.1
    MAX_CONSECUTIVE_INFLUX_FAILURES = 10
    LOG_INTERVAL = 10.0
    IDLE_TIMEOUT = 2.0

    def __init__(self, mqtt_settings: MQTTSettings, influx_settings: InfluxSettings):
        """
        Initializes a bridge ready to be started with run().

        Args:
            * mqtt_config: MQTT configuration (host, port, topic_base)
            * influx_config: InfluxDB configuration (url, token, org, bucket)
        """
        # MQTT
        self._mqtt_host = mqtt_settings.mqtt_host
        self._mqtt_port = mqtt_settings.mqtt_port
        self._mqtt_topic_base = mqtt_settings.mqtt_topic_base

        # InfluxDB
        self._influx_url = influx_settings.influx_url
        self._influx_token = influx_settings.influx_token
        self._influx_org = influx_settings.influx_org
        self._influx_bucket = influx_settings.influx_bucket

        # Clients and resources
        self._client_id = f"MQTTInfluxBridge_{uuid.uuid4().hex[:8]}"
        self._mqtt_client: mqtt.Client | None = None
        self._influx_client: InfluxDBClient | None = None
        self._write_api = None

        # Protects access to _write_api/_influx_client during write and shutdown.
        self._io_lock = threading.Lock()

        # State
        self._mqtt_connected = threading.Event()
        self._stop_requested = threading.Event()

        # Statistics for logging and diagnostics
        self._stats = {
            "messages_received": 0,
            "datapoints_received": 0,
            "batches_written": 0,
            "datapoints_written": 0,
            "influx_retries": 0,
            "failed_writes": 0,
            "consecutive_failures": 0,
            "messages_discarded": 0,
        }

        # Active time tracking
        self._accumulated_active_time = 0.0
        self._burst_start_time = None

        # Idle state tracking
        self._last_msg_time = time.monotonic()
        self._has_unlogged_data = False

        self._logger = logging.getLogger(f"Bridge[{self._client_id[-8:]}]")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def run(self) -> None:
        """
        Starts the bridge and blocks until termination.

        Terminates when:
            * stop() is called
            * A fatal error occurs (handled with stop())

        Side effects:
            * Opens MQTT and InfluxDB connections
            * Starts Paho MQTT loop thread
        """
        # Logging and timer
        start_time = time.monotonic()
        self._log_startup_banner()

        try:
            # Initial connections (fail-fast)
            if not self._connect_influx():
                return

            if not self._connect_mqtt():
                return

            self._logger.info("Bridge started - listening for MQTT messages")
            
            # Main loop (interruptible)
            last_log_time = start_time
            while not self._stop_requested.is_set():
                now = time.monotonic()

                if self._has_unlogged_data:
                    if now - last_log_time >= self.LOG_INTERVAL:
                        self._log_stats(now-start_time)
                        last_log_time = now
                        self._has_unlogged_data = False
                    elif now - self._last_msg_time >= self.IDLE_TIMEOUT:
                        if self._burst_start_time is not None:
                            self._accumulated_active_time += now - self._burst_start_time
                            self._burst_start_time = None

                        self._log_stats(now - start_time)
                        last_log_time = now
                        self._has_unlogged_data = False
                    
                self._stop_requested.wait(timeout=0.5)

        except Exception as e:
            self._logger.error(f"Critical error in run: {e}", exc_info=True)

        finally:
            # Shutdown
            self._disconnect_mqtt()    # Shutdown order to avoid messages while _write_api is already None
            self._disconnect_influx()
            try:
                self._log_stats(time.monotonic() - start_time)
                self._logger.info("Bridge terminated")
            except Exception:
                pass

    def stop(self) -> None:
        """
        Requests bridge shutdown (idempotent).

        Intended to be called from:
            * Caller's signal handler
            * Internal logic in case of systemic error
        """
        if self._stop_requested.is_set():
            return
        
        self._stop_requested.set()
        try:
            self._logger.info("Stop requested")
        except Exception:
            pass

    # =========================================================================
    # CONNECTIONS
    # =========================================================================

    def _connect_influx(self) -> bool:
        """
        Connects to InfluxDB and initializes the synchronous Write API.

        Returns:
            True if the connection and health check succeed, False otherwise.
        """
        client = InfluxDBClient(
            url=self._influx_url,
            token=self._influx_token,
            org=self._influx_org
        )

        try:
            self._logger.info(f"Connecting to InfluxDB ({self._influx_url})...")

            health = client.health()    # Blocking call for a small time t

            if health.status != "pass":
                self._logger.error(f"InfluxDB health check failed: {health.status} - {health.message}")
                self._disconnect_influx()
                return False

            write_api = client.write_api(write_options=SYNCHRONOUS)

            with self._io_lock:
                self._influx_client = client
                self._write_api = write_api

            return True

        except Exception as e:
            self._logger.error(f"InfluxDB connection error: {e}", exc_info=True)
            self._disconnect_influx()
            return False

    def _connect_mqtt(self) -> bool:
        """
        Connects to the MQTT broker and waits for the handshake (on_connect).

        Returns:
            True if the handshake completes within the timeout, False otherwise.
        """
        self._mqtt_connected.clear()

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._client_id,
            clean_session=False,    # Persistent session, keeps messages if disconnected
            manual_ack=True
        )

        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_message = self._on_mqtt_message

        self._mqtt_client = client

        try:
            self._logger.info(f"Connecting to MQTT ({self._mqtt_host}:{self._mqtt_port})...")

            client.connect(self._mqtt_host, self._mqtt_port, keepalive=self.MQTT_KEEPALIVE)
            client.loop_start()

            # Wait for MQTT handshake (interruptible)
            timeout = 10.0
            step = 0.2
            deadline = time.monotonic() + timeout

            while time.monotonic() < deadline:
                if self._stop_requested.is_set():
                    self._logger.info("Stop requested during MQTT connection")
                    self._disconnect_mqtt()
                    return False

                if self._mqtt_connected.wait(timeout=step):
                    return True

            self._logger.warning("MQTT handshake timeout")
            self._disconnect_mqtt()
            return False

        except Exception as e:
            self._logger.error(f"MQTT connection failed: {e}", exc_info=True)
            self._disconnect_mqtt()
            return False
    
    def _disconnect_mqtt(self) -> None:
        """Disconnects from the MQTT broker and stops the loop thread (best-effort)."""
        client = self._mqtt_client
        if client is None:
            return
            
        try:
            client.disconnect()
        except Exception as e:
            self._logger.debug(f"MQTT disconnect error: {e}")

        try:
            client.loop_stop()
        except Exception as e:
            self._logger.debug(f"MQTT loop_stop error: {e}")
        
        self._mqtt_client = None

    def _disconnect_influx(self) -> None:
        """Closes the InfluxDB client (best-effort) and invalidates write resources."""
        with self._io_lock:
            client = self._influx_client
            self._influx_client = None
            self._write_api = None

        if client is None:
            return

        try:
            client.close()
        except Exception as e:
            self._logger.debug(f"InfluxDB close error: {e}")

    # =========================================================================
    # MQTT CALLBACKS
    # =========================================================================

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """
        Paho callback: connection completed.

        Effect:
            - subscribes to {topic_base}/# and signals the handshake event.
        """
        if reason_code.is_failure:
            self._logger.error(f"MQTT connection failed: {reason_code}")
            return

        self._logger.info(f"Connected to MQTT ({self._mqtt_host}:{self._mqtt_port})")

        # Subscription
        topic = f"{self._mqtt_topic_base}/#"
        client.subscribe(topic, qos=self.MQTT_QOS)    # subscribe is actually async, ideally we'd wait for on_subscribe but this is acceptable
        self._logger.info(f"Subscribed to: {topic}")

        self._mqtt_connected.set()

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Paho callback: disconnected from MQTT broker."""
        self._mqtt_connected.clear()

        if reason_code.is_failure:
            self._logger.warning(f"Unexpected MQTT disconnection: {reason_code}")

    def _on_mqtt_message(self, client, userdata, msg):
        """
        Paho callback: message received.

        WARNING: Intentional design choice:
            This method performs blocking operations (InfluxDB writes) directly
            on the Paho thread loop. This is critical for the system's
            backpressure mechanism. DO NOT introduce asynchronous queues here.
            (See the MQTTInfluxBridge class docstring for the full technical rationale).

        Pipeline:
            1) decode MessagePack
            2) validate/normalize batch (list)
            3) convert dict -> Point
            4) write to InfluxDB with retry
            5) ACK only if write OK

        Error policy:
            - non-decodable payload / invalid data -> ACK and drop (avoids infinite loop)
            - failed Influx write -> NO ACK (broker will redeliver)
            - too many consecutive failures -> stop() (considered a systemic error)
        """
        if self._stop_requested.is_set():
            return
        
        if self._burst_start_time is None:
            self._burst_start_time = time.monotonic()
        
        # Try because if the loop thread has unhandled exceptions it can terminate silently
        try:
            self._stats["messages_received"] += 1

            # Deserialization (policy: if it fails, ACK and move to the next message)
            try:
                batch = msgpack.unpackb(msg.payload, raw=False)
            except Exception as e:
                self._logger.warning(f"Undecodable payload. msgpack.unpackb error: {e}", exc_info=True)
                self._stats["messages_discarded"] += 1
                self._safe_ack(client, msg)
                return
            
            if batch is None:
                self._logger.warning("Decoded payload but empty (None). Drop.")
                self._stats["messages_discarded"] += 1
                self._safe_ack(client, msg)
                return
            # Guard if we don't receive a list
            if not isinstance(batch, list):
                batch = [batch]
            self._stats["datapoints_received"] += len(batch)

            # Convert dict -> points
            points: list[Point] = []

            for dp in batch:
                try:
                    points.append(self._dict_to_influx_point(dp))
                except Exception as e:
                    self._logger.debug(f"Invalid datapoint discarded: {e}")

            if not points:
                self._stats["messages_discarded"] += 1
                self._logger.warning("Completely invalid batch: 0 valid datapoints. Drop + ACK.")
                self._safe_ack(client, msg)
                return

            # Write with retry (_write_to_influx is protected from exceptions)
            written = self._write_to_influx(points)
            if written > 0:
                # Success: ACK to broker
                self._stats["consecutive_failures"] = 0
                self._stats["batches_written"] += 1
                self._stats["datapoints_written"] += written
                self._safe_ack(client, msg)
                return
            else:
                # Failure -> no ACK, broker will retry
                self._stats["consecutive_failures"] += 1
                self._stats["failed_writes"] += 1

                # Too many consecutive failures -> systemic error
                if self._stats["consecutive_failures"] >= self.MAX_CONSECUTIVE_INFLUX_FAILURES:
                    error_msg = (f"InfluxDB unreachable: {self._stats['consecutive_failures']} consecutive failures")
                    self._logger.error(error_msg)
                    self.stop()

        except Exception as e:
            self._logger.error(f"Unhandled error in on_message: {e}", exc_info=True)
            self.stop()
        
        finally:
            self._last_msg_time = time.monotonic()
            self._has_unlogged_data = True

    def _safe_ack(self, client: mqtt.Client, msg: mqtt.MQTTMessage) -> None:
        """
        Performs MQTT ACK in best-effort mode.

        Note:
            During shutdown/disconnect a race condition may exist: the client can become
            invalid while ACK is being attempted. In that case, log and continue.
        """
        try:
            client.ack(msg.mid, msg.qos)
        except Exception as e:
            self._logger.warning(f"client.ack error: {e}", exc_info=True)

    # =========================================================================
    # INFLUXDB
    # =========================================================================

    def _write_to_influx(self, points: list[Point]) -> int:
        """
        Writes a batch to InfluxDB with exponential retry.

        Args:
            points: List of Points to write.

        Returns:
            Number of points written; 0 if the write fails.
        """
        last_error = None

        # Retry loop to handle transient network errors or timeouts
        for attempt in range(self.INFLUX_MAX_RETRIES):
            if self._stop_requested.is_set():
                return 0

            try:
                # Acquire lock for InfluxDB write
                with self._io_lock:
                    # Snapshot
                    write_api = self._write_api
                    # Validation
                    if write_api is None:
                        return 0
                    # Synchronous write, waits for response
                    write_api.write(bucket=self._influx_bucket, record=points)
                return len(points)
            
            except Exception as e:
                last_error = e
                self._stats["influx_retries"] += 1

                # Retry condition
                if attempt < self.INFLUX_MAX_RETRIES - 1:
                    delay = self.INFLUX_RETRY_BASE_DELAY * (2 ** attempt)
                    self._logger.debug(f"Retrying InfluxDB write in {delay}s...")
                    if self._stop_requested.wait(timeout=delay):
                        return 0
                    
        # All retries exhausted
        if "401" in str(last_error):
            self._logger.error("InfluxDB: Unauthorized token (401)")
        else:
            self._logger.error(
                f"InfluxDB write failed after {self.INFLUX_MAX_RETRIES} attempts: {last_error}"
            )
        return 0

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def _dict_to_influx_point(self, data: dict) -> Point:
        """
        Converts a dictionary into an InfluxDB Point via DataPoint.

        Args:
            data: Dictionary compatible with DataPoint.from_dict().

        Returns:
            Point ready for the write API.

        Note:
            device_type and device_id are stored as top-level DataPoint attributes
            (not inside tags) to reduce msgpack payload size. They are injected
            here as InfluxDB tags so that queries can still filter by device
        """
        dp = DataPoint.from_dict(data)
        point = Point(dp.measurement).time(dp.timestamp)

        if dp.device_type:
            point.tag("device_type", dp.device_type)
        if dp.device_id:
            point.tag("device_id", dp.device_id)

        # Tags: truthiness check. Empty string is as useless as None in InfluxDB
        # Fields: is not None. 0, 0.0, False are legitimate measurement values
        for k, v in dp.tags.items():
            if v:
                point.tag(k, str(v))

        for k, v in dp.fields.items():
            if v is not None:
                point.field(k, v)

        return point

    def _log_startup_banner(self) -> None:
        """Logs a configuration summary at startup."""
        self._logger.info(f"""
╔═════════════════════════════════════════════════════════╗
║      BRIDGE STARTUP - HEALTHCARE MONITORING SYSTEM      ║
╚═════════════════════════════════════════════════════════╝
  MQTT Broker:        {self._mqtt_host}:{self._mqtt_port}
  Subscription:       {self._mqtt_topic_base}/#
  InfluxDB:           {self._influx_url}
  Bucket:             {self._influx_bucket}
  Max Retries:        {self.INFLUX_MAX_RETRIES}
  Shutdown Threshold: {self.MAX_CONSECUTIVE_INFLUX_FAILURES} failures
""")

    def _log_stats(self, elapsed: float) -> None:
        """
        Logs runtime statistics.

        Args:
            elapsed: Seconds elapsed since startup (monotonic).
        
        Throughput calculates persisted points per second based on actual
        working time (total_active_time), not total elapsed time since startup.
        To obtain a true average speed, the calculation sums the time of past
        bursts (_accumulated_active_time) with that of the current burst
        started from _burst_start_time.
        """
        s = self._stats
        total_active_time = self._accumulated_active_time

        if self._burst_start_time is not None:  # Includes the duration of the current burst not yet accounted for in accumulated time.
            current_burst_time = time.monotonic() - self._burst_start_time
            total_active_time += current_burst_time

        if total_active_time > 0:
            rate = s["datapoints_written"] / total_active_time
        else:
            rate = 0 

        self._logger.info(f"""
    [BRIDGE STATISTICS]
    Elapsed time since startup:                   {elapsed:.1f}s
    Batches received:                             {s['messages_received']}
    Batches persisted:                            {s['batches_written']}
    Datapoints received:                          {s['datapoints_received']}
    Datapoints persisted:                         {s['datapoints_written']}
    Persistence throughput:                       {rate:,.0f} dp/s
    InfluxDB write retries:                       {s['influx_retries']}
    Discarded batches (invalid data):             {s['messages_discarded']}
    Failed persistence attempts:                  {s['failed_writes']}
    Consecutive failed persistence attempts:      {s['consecutive_failures']}
""")