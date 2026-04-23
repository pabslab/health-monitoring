#!/usr/bin/env python3
"""
Orchestrate the concurrent execution of MQTT publishers and manages their lifecycle.
"""
import logging
import signal
import sys
import threading
import time

from datasources import SUPPORTED_SENSORS
from publisher import MQTTPublisher
from config import MQTTSettings, DATASOURCES, setup_logging


class Application:
    """
    Manage the lifecycle of data producers (Datasources).

    Responsibilities:
        * Logging setup and signal handlers
        * Load sensor configuration
        * Instantiate enabled DataSources
        * Start a dedicated thread per source for MQTT publishing
        * Coordinate clean shutdown on SIGINT and SIGTERM signals
        * Determine exit code for orchestrators

    Exit code policy:
        * exit 0: explicit shutdown (SIGINT/SIGTERM) -> no restart
        * exit 1: crash or unhandled error -> restart recommended
    """

    # Configuration
    THREAD_JOIN_TIMEOUT = 15.0

    def __init__(self):
        """Initialize application state."""
        self._logger: logging.Logger | None = None

        # Configuration
        self._settings: MQTTSettings | None = None

        # Coordination event for DataSources (generators)
        self._stop_requested = threading.Event()

        # Flag to distinguish voluntary shutdown from crash.
        # Default False = any non-explicit termination -> exit 1
        self._graceful_shutdown_requested = False

        # Managed resources
        self._datasources: list = []
        self._publishers: list[MQTTPublisher] = []
        self._threads: list[threading.Thread] = []

    def run(self) -> int:
        """
        Start the application and block until termination.

        Returns:
            Exit code: 0 for voluntary shutdown, 1 for errors.
        """
        try:
            self._settings = MQTTSettings()
            
            setup_logging(self._settings.log_level)
            self._logger = logging.getLogger("PublisherMain")

            # Register signal handlers BEFORE anything else
            signal.signal(signal.SIGINT, self._on_termination_signal)
            signal.signal(signal.SIGTERM, self._on_termination_signal)

            # Create DataSources
            if self._create_datasources():
                # Start publishing threads
                self._start_threads()
                # Main loop: wait for completion or error
                self._wait_for_completion()
        except Exception as e:
            self._safe_log("error", f"Unhandled exception during startup: {e}", exc_info=True)
        finally:
            self._cleanup()

        return self._determine_exit_code()

    # =========================================================================
    # RESOURCES CREATION
    # =========================================================================

    def _create_datasources(self) -> bool:
        """
        Dynamically create DataSource instances based on the configuration.
        Iterate over DATASOURCES and use SUPPORTED_SENSORS to instantiate the correct class.

        Policy:
            * Structural Integrity: Validate that DATASOURCES is a dictionary.
            * Disabled Sources: Skipped intentionally.
            * Unsupported Types: Ignored (non-fatal for the application).
            * Broken/Malformed Config: Logged as failure, but allows other sources to load.
            * Initialization Requirement: At least one valid source must be created 
              for the process to continue (returns False otherwise).
        """
        for ds_key, cfg in DATASOURCES.items():
            try:
                if not cfg.enabled:
                    self._safe_log("info", f"DataSource '{ds_key}' is disabled, skipping.")
                    continue

                # Identify the sensor type
                sensor_type = cfg.type
                ds_class = SUPPORTED_SENSORS.get(sensor_type)

                if not ds_class:
                    self._safe_log("warning", f"Sensor type '{sensor_type}' not supported for '{ds_key}'.")
                    continue
                    
                instance = ds_class(
                    data_path=cfg.data_path,
                    poll_interval=cfg.poll_interval,
                    device_id=ds_key,
                    stop_requested=self._stop_requested,
                )
                self._datasources.append(instance)
                
            except Exception as e:
                self._safe_log("error", f"Failed to initialize {ds_key} (check permissions or path): {e}")

        # Final check: is at least one sensor active?
        if not self._datasources:
            self._safe_log("error", "No datasources created. Check config.py and supported types.")
            return False

        return True

    def _start_threads(self) -> None:
        """Create and start one thread per DataSource."""
        for ds in self._datasources:
            publisher = MQTTPublisher(settings=self._settings, stop_requested=self._stop_requested)
            self._publishers.append(publisher)

            thread = threading.Thread(
                target=self._publish_worker,
                args=(publisher, ds),
                name=f"Thread-{ds.device_id}",
                daemon=False
            )
            self._threads.append(thread)

        for thread in self._threads:
            thread.start()

    # =========================================================================
    # WORKER THREAD
    # =========================================================================

    def _publish_worker(self, publisher: MQTTPublisher, datasource) -> None:
        """
        Worker thread to publish a DataSource.

        Args:
            publisher: MQTTPublisher instance to use.
            datasource: Data source to publish.
        """
        logger = logging.getLogger(f"Worker[{datasource.device_id}]")

        try:
            success = publisher.run(datasource)

            # Failure not caused by shutdown -> signal error
            if not success and not self._stop_requested.is_set():
                logger.error("Publisher terminated with error")
                self._stop_requested.set()

        except Exception as e:
            logger.error(f"Thread crashed: {e}", exc_info=True)
            self._stop_requested.set()

    # =========================================================================
    # MAIN LOOP
    # =========================================================================

    def _wait_for_completion(self) -> None:
        """
        Main supervision loop.

        Stays active until shutdown is requested (SIGINT/SIGTERM or worker
        error) and workers are still alive.

        If all workers terminate without a shutdown request -> anomaly.
        """
        check_interval = 0.1

        while not self._stop_requested.is_set():
            # Check if all threads have terminated
            threads_alive = [t for t in self._threads if t.is_alive()]
            if not threads_alive:
                # Check if threads terminated without shutdown
                if not self._stop_requested.is_set():
                    self._safe_log("error", "All threads terminated without shutdown request (anomaly).")
                break

            # "Cooperative" wait: does not busy-wait and wakes immediately on stop
            self._stop_requested.wait(timeout=check_interval)

    # =========================================================================
    # SIGNAL HANDLERS
    # =========================================================================

    def _on_termination_signal(self, signum: int, frame) -> None:
        """Handler for SIGINT (Ctrl+C) and SIGTERM (e.g. docker stop)."""
        # Avoid re-entrancy if signal received multiple times
        if self._graceful_shutdown_requested:
            return

        # Set the flag first
        self._graceful_shutdown_requested = True

        # Then log and try to stop publisher threads (in try/except for safety)
        try:
            sig_name = signal.Signals(signum).name
            self._safe_log("warning", f"Received {sig_name} -> voluntary shutdown requested")

            self._stop_requested.set()

        except Exception as e:
            # Even if this fails, the flag is already True -> exit 0
            self._safe_log("debug", f"Error during shutdown: {e}")

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def _cleanup(self) -> None:
        """Wait for all threads to terminate with timeout."""
        try:
            self._stop_requested.set()

            self._safe_log("info", "Waiting for threads to close...")
            start_wait = time.monotonic()

            for thread in self._threads:
                # Calculate remaining time for global timeout
                elapsed = time.monotonic() - start_wait
                remaining = max(0, self.THREAD_JOIN_TIMEOUT - elapsed)

                thread.join(timeout=remaining)

                if thread.is_alive():
                    self._safe_log("warning", f"Thread {thread.name} forced to shutdown")
        except Exception as e:
            self._safe_log("debug", f"Error during cleanup: {e}")

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def _determine_exit_code(self) -> int:
        """
        Determine the final exit code based on the shutdown state.

        Returns:
            0 for voluntary shutdown or completion, 1 for errors.
        """
        if self._graceful_shutdown_requested:
            self._safe_log("info", "Clean termination completed (exit 0)")
            return 0

        self._safe_log("error", "Unvoluntary termination (exit 1) -> restart recommended")
        return 1

    def _safe_log(self, level: str, message: str, **kwargs) -> None:
        """
        Log that does not raise exceptions.

        During shutdown the logging system may be compromised.
        """
        try:
            if self._logger is not None:
                getattr(self._logger, level)(message, **kwargs)
        except Exception:
            pass

    # =============================================================================
    # ENTRY POINT
    # =============================================================================

def main():
    app = Application()
    exit_code = app.run()
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
