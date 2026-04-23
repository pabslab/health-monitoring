"""
MQTT publisher for message delivery.

This module implements batching logic and secure publication for the system. 
Note: OS signal handling (SIGINT/SIGTERM) is delegated to the caller 
(main_publisher.py) to allow for a coordinated shutdown.
"""
import logging
import random
import threading
import time
import uuid
from collections.abc import Iterator

import msgpack
import paho.mqtt.client as mqtt

from core import DataPoint
from config import MQTTSettings
from datasources import BaseDataSource


class MQTTPublisher:
    """
    Manages batch publishing and communication with the MQTT broker.

    Responsibilities:
        * Automatic connection and reconnection to the broker
        * Batching DataPoints into MessagePack messages
        * Publishing with QoS 1 (at-least-once)
        * Dynamic routing based on device_type and device_id

    Reliability policy:
        * QoS 1: ACK from broker (PUBACK) required for each message
        * Retry with exponential backoff on temporary failures
        * Automatic reconnection on unexpected disconnection
        * File marked as processed only after broker confirmation

    Usage:
        * publisher.run(datasource: BaseDataSource) - Blocking call, returns on stop or error
    """

    # Configuration
    MQTT_QOS = 1
    MQTT_KEEPALIVE = 60
    LOG_INTERVAL = 15.0
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 0.1
    PUBACK_TIMEOUT = 5.0
    PUBLISH_INTERVAL = 0.2
    TCP_CONNECT_TIMEOUT = 5.0
    TCP_MAX_ATTEMPTS = 3
    HANDSHAKE_TIMEOUT = 10.0
    RECONNECT_TIMEOUT = 30.0

    def __init__(self, settings: MQTTSettings, stop_requested: threading.Event,):
        """
        Initializes a publisher ready to be started with run().

        Args:
            mqtt_config: MQTT configuration (host, port, batch_size, topic_base).
        """
        # MQTT Configuration
        self._mqtt_host = settings.mqtt_host
        self._mqtt_port = settings.mqtt_port
        self._batch_size = settings.mqtt_batch_size
        self._topic_base = settings.mqtt_topic_base

        # Client and resources
        self._client_id = f"Publisher_{uuid.uuid4().hex[:8]}"
        self._mqtt_client: mqtt.Client | None = None

        # State
        self._mqtt_connected = threading.Event()
        self._stop_requested = stop_requested

        # Statistics
        self._stats = {
            "messages_sent": 0,
            "datapoints_sent": 0,
            "bytes_sent": 0,
            "mqtt_retries": 0,
            "files_sent": 0,
        }

        # Active time tracking
        self._accumulated_active_time = 0.0
        self._burst_start_time = None

        self._logger = logging.getLogger(f"Publisher[{self._client_id[-8:]}]")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def run(self, datasource: BaseDataSource) -> bool:
        """
        Starts the publisher and processes all files from the datasource.

        Args:
            datasource: Data source with iter_files() interface.

        Returns:
            True if completed successfully, False on fatal errors.

        Terminates when:
            * All files have been processed
            * The threading event _stop_requested is set
            * A fatal error occurs
        """
        start_time = time.monotonic()
        self._log_startup_banner(datasource)

        try:
            # Initial connection (fail-fast)
            if not self._connect_mqtt():
                return False

            self._logger.info("Publisher started - processing files")

            # Tracking for periodic logging
            last_log_time = start_time

            for file_path, datapoints in datasource.iter_files():
                if self._stop_requested.is_set():
                    self._logger.info("Stop requested, interrupting processing")
                    break
                
                if self._burst_start_time is None:
                    self._burst_start_time = time.monotonic()

                if self._publish_file(datapoints):
                    # Broker confirmed - archive the file
                    datasource.mark_as_processed(file_path)
                    self._stats["files_sent"] += 1

                    # Periodic log
                    now = time.monotonic()
                    if now - last_log_time >= self.LOG_INTERVAL:
                        self._log_stats(now - start_time)
                        last_log_time = now
                    elif datasource.is_idle():
                        self._accumulated_active_time += now - self._burst_start_time
                        self._burst_start_time = None

                        self._log_stats(now - start_time)
                        last_log_time = now
                else:
                    # Critical failure - file remains in inbox
                    if not self._stop_requested.is_set():
                        self._logger.warning(f"File {file_path.name} not confirmed, remains in inbox")
                        datasource.mark_as_failed(file_path)
                    return False

            return True

        except Exception as e:
            self._logger.critical(f"Critical error in run: {e}", exc_info=True)
            return False

        finally:
            self._disconnect_mqtt()
            try:
                self._log_stats(time.monotonic() - start_time)
                self._logger.info("Publisher terminated")
            except Exception:
                pass

    # =========================================================================
    # PUBLISHING
    # =========================================================================

    def _publish_file(self, datapoints: Iterator[DataPoint]) -> bool:
        """
        Publishes the DataPoints of a file by grouping them into batches.

        Args:
            datapoints: Iterator of DataPoints to process.

        Returns:
            True if all batches were confirmed by the broker.
        """
        batch: list[DataPoint] = []

        for dp in datapoints:
            if self._stop_requested.is_set():
                return False

            batch.append(dp)

            # Batch full -> publish
            if len(batch) >= self._batch_size:
                if not self._publish_batch(batch):
                    return False

                batch = []

                # Throttle to avoid saturating the network
                if self.PUBLISH_INTERVAL > 0:
                    if self._stop_requested.wait(self.PUBLISH_INTERVAL):
                        return False

        # Flush remaining datapoints
        if batch:
            if not self._publish_batch(batch):
                return False

        return True

    def _publish_batch(self, batch: list[DataPoint]) -> bool:
        """
        Publishes a batch of DataPoints as a single MQTT message.

        Implements resilience at two levels:
            1. Waiting for reconnection if broker is momentarily offline
            2. Retry with exponential backoff on missing PUBACK

        Args:
            batch: List of DataPoints to publish.

        Returns:
            True if confirmed by the broker (QoS 1), False otherwise.
        """
        if not batch:
            return True

        topic = self._build_topic(batch[0])
        payload = msgpack.packb([dp.to_dict() for dp in batch])
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            if self._stop_requested.is_set():
                return False

            # If disconnected, wait for reconnection
            if not self._mqtt_connected.is_set():
                self._logger.warning("Connection lost. Waiting for reconnection...")
                if not self._wait_for_connection(self.RECONNECT_TIMEOUT):
                    if not self._stop_requested.is_set():
                        self._logger.error("Reconnection failed after timeout")
                    return False

            try:
                result = self._mqtt_client.publish(topic, payload, qos=self.MQTT_QOS)
                result.wait_for_publish(self.PUBACK_TIMEOUT)

                if result.is_published(): 
                    self._stats["messages_sent"] += 1
                    self._stats["datapoints_sent"] += len(batch)
                    self._stats["bytes_sent"] += len(payload)
                    return True

                last_error = "PUBACK timeout"

            except Exception as e:
                last_error = str(e)

            self._stats["mqtt_retries"] += 1

            # Exponential backoff with jitter
            if attempt < self.MAX_RETRIES - 1:
                delay = self.RETRY_BASE_DELAY * (2 ** attempt)
                delay += random.uniform(0, delay * 0.1)

                self._logger.debug(f"Batch failed ({last_error}), retry {attempt + 1}/{self.MAX_RETRIES} in {delay:.2f}s")

                if self._stop_requested.wait(timeout=delay):
                    return False

        self._logger.error(f"Publish failed after {self.MAX_RETRIES} attempts: {last_error}")
        return False

    # =========================================================================
    # MQTT CONNECTION
    # =========================================================================

    def _connect_mqtt(self) -> bool:
        """
        Connects to the MQTT broker and waits for the handshake.

        Returns:
            True if connected successfully, False otherwise.
        """
        self._mqtt_connected.clear()

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._client_id
        )

        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect

        self._mqtt_client = client

        # Phase 1: TCP connection with retry
        for attempt in range(self.TCP_MAX_ATTEMPTS):
            if self._stop_requested.is_set():
                self._logger.info("Stop requested during connection")
                self._disconnect_mqtt()
                return False

            try:
                self._logger.info(f"Connecting to MQTT ({self._mqtt_host}:{self._mqtt_port})...")
                client.connect(self._mqtt_host, self._mqtt_port, keepalive=self.MQTT_KEEPALIVE)
                break

            except (OSError, ConnectionError) as e:
                self._logger.warning(f"TCP connection failed ({attempt + 1}/{self.TCP_MAX_ATTEMPTS}): {e}")

                if attempt < self.TCP_MAX_ATTEMPTS - 1:
                    if self._stop_requested.wait(timeout=self.TCP_CONNECT_TIMEOUT):
                        self._logger.info("Stop requested during TCP retry")
                        self._disconnect_mqtt()
                        return False
                else:
                    self._logger.error(f"TCP connection failed to {self._mqtt_host}:{self._mqtt_port}")
                    self._disconnect_mqtt()
                    return False

        # Phase 2: MQTT handshake
        client.loop_start()

        if self._wait_for_connection(self.HANDSHAKE_TIMEOUT):
            return True

        if not self._stop_requested.is_set():
            self._logger.error("MQTT handshake timeout")

        self._disconnect_mqtt()
        return False

    def _disconnect_mqtt(self) -> None:
        """Disconnects from the MQTT broker and stops the loop thread (best-effort)."""
        client = self._mqtt_client
        if client is None:
            self._mqtt_connected.clear()
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
        self._mqtt_connected.clear()

    def _wait_for_connection(self, timeout: float) -> bool:
        """
        Waits for connection in an interruptible way.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if connected, False on timeout or stop requested.
        """
        deadline = time.monotonic() + timeout

        while not self._mqtt_connected.is_set():
            if self._stop_requested.is_set():
                return False

            if time.monotonic() >= deadline:
                return False

            self._mqtt_connected.wait(timeout=0.5)

        return self._mqtt_connected.is_set()

    # =========================================================================
    # MQTT CALLBACKS
    # =========================================================================

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """Paho callback: connection completed."""
        if reason_code.is_failure:
            self._logger.error(f"MQTT connection rejected: {reason_code}")
            return

        self._logger.info(f"Connected to MQTT ({self._mqtt_host}:{self._mqtt_port})")
        self._mqtt_connected.set()

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Paho callback: disconnection from broker."""
        self._mqtt_connected.clear()

        if reason_code.is_failure:
            self._logger.warning(f"Unexpected MQTT disconnection: {reason_code}")

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def _build_topic(self, datapoint: DataPoint) -> str:
        """
        Builds the MQTT topic for a DataPoint.

        Schema: {base}/{device_type}/{device_id}/biometrics

        Args:
            datapoint: DataPoint from which to extract device_type and device_id.

        Returns:
            Full MQTT topic.
        """
        return f"{self._topic_base}/{datapoint.device_type}/{datapoint.device_id}/biometrics"

    def _log_startup_banner(self, datasource) -> None:
        """Logs a configuration summary at startup."""
        source_info = datasource.get_info()
        self._logger.info(f"""
╔════════════════════════════════════════════════════════════╗
║      PUBLISHER STARTUP - HEALTHCARE MONITORING SYSTEM      ║
╚════════════════════════════════════════════════════════════╝
  Device Type:        {source_info['device_type']}
  Source ID:          {source_info['device_id']}
  Data Path:          {source_info['data_path']}
  MQTT Broker:        {self._mqtt_host}:{self._mqtt_port}
  Base Topic:         {self._topic_base}
  Batch Size:         {self._batch_size}
  Max Retry:          {self.MAX_RETRIES}
""")

    def _log_stats(self, elapsed: float) -> None:
        """
        Logs runtime statistics.

        Args:
            elapsed: Seconds elapsed since startup.

        Throughput calculates points sent per second based on actual working time
        (total_active_time), not total time since startup. To obtain a real average
        speed, the calculation sums the time of past bursts (_accumulated_active_time)
        with that of the current burst started by _burst_start_time.
        """
        s = self._stats
        mb_sent = s["bytes_sent"] / (1024 * 1024)
        total_active_time = self._accumulated_active_time

        if self._burst_start_time is not None:  # Includes the duration of the current burst not yet accounted for in accumulated time.
            current_burst_time = time.monotonic() - self._burst_start_time
            total_active_time += current_burst_time

        if total_active_time > 0:
            rate = s["datapoints_sent"] / total_active_time
        else:
            rate = 0 

        self._logger.info(f"""
    [PUBLISHER STATISTICS - BROKER-CONFIRMED DATA]
    Elapsed time since startup:         {elapsed:.1f}s
    Files sent:                         {s['files_sent']}
    Batches sent:                       {s['messages_sent']:,}
    Points sent:                        {s['datapoints_sent']:,}
    Data transmitted:                   {mb_sent:.2f} MB
    Send throughput:                    {rate:,.0f} dp/s
    Batch send retries:                 {s['mqtt_retries']}
""")
