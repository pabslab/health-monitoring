#!/usr/bin/env python3
"""
Subscriber Entry Point. Starts the MQTT-InfluxDB bridge.
"""
import logging
import signal
import sys

from bridge import MQTTInfluxBridge
from config import MQTTSettings, InfluxSettings, setup_logging


class Application:
    """
    Manages the lifecycle of the MQTT-InfluxDB bridge.

    Responsibilities:
        * Logging setup and signal handlers
        * Create and start the bridge
        * Handle clean shutdown on OS signals
        * Determine exit code for orchestrators

    Exit code policy:
        * exit 0: explicit shutdown (SIGINT/SIGTERM) -> no restart
        * exit 1: crash or unhandled error -> restart recommended
    """
    
    def __init__(self):
        """Initialize application state."""
        self._logger: logging.Logger | None = None
        
        self._mqtt_settings: MQTTSettings | None = None
        self._influx_settings: InfluxSettings | None = None

        # Flag to distinguish voluntary shutdown from crash.
        # Default False = any non-explicit termination -> exit 1
        self._graceful_shutdown_requested = False
    
        self._bridge: MQTTInfluxBridge | None = None

    def run(self) -> int:
        """
        Start the application and block until termination.
        
        Returns:
            Exit code: 0 for voluntary shutdown, 1 for errors.
        """
        try:
            self._mqtt_settings = MQTTSettings()
            self._influx_settings = InfluxSettings()

            setup_logging(self._mqtt_settings.log_level)
            self._logger = logging.getLogger("SubscriberMain")
            
            # Register signal handlers BEFORE anything else
            signal.signal(signal.SIGINT, self._on_termination_signal)
            signal.signal(signal.SIGTERM, self._on_termination_signal)
            
            self._bridge = MQTTInfluxBridge(
                mqtt_settings=self._mqtt_settings,
                influx_settings=self._influx_settings
            )
            
            # run() blocks until it terminates (via stop() or error)
            self._bridge.run()
            
        except Exception as e:
            # Any unhandled exception -> exit 1
            self._safe_log("error", f"Unhandled exception during bridge startup: {e}", exc_info=True)
        
        # Determine exit code based on _graceful_shutdown_requested
        return self._determine_exit_code()
    
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
        
        # Then log and try to stop the bridge (in try/except for safety)
        try:
            sig_name = signal.Signals(signum).name
            self._safe_log("warning", f"Received {sig_name} -> voluntary shutdown requested")
            
            if self._bridge is not None:
                self._bridge.stop()
        except Exception as e:
            # Even if something fails here, the flag is already True
            # so we will exit with exit 0 regardless
            self._safe_log("debug", f"Error during shutdown: {e}")

    # =========================================================================
    # UTILITIES
    # =========================================================================
 
    def _determine_exit_code(self) -> int:
        """
        Determine the final exit code based on the shutdown state.
        """
        if self._graceful_shutdown_requested:
            self._safe_log("info", "Clean termination completed (exit 0)")
            return 0
        else:
            self._safe_log("error", "Unvoluntary termination (exit 1) → Docker will restart")
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
            pass  # Logging may fail during shutdown, ignore

    # =========================================================================
    # ENTRY POINT
    # =========================================================================

def main():
    app = Application()
    exit_code = app.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
