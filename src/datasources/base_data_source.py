"""
Base DataSource for CSV file data extraction.

This module contains the abstract base class for:
    * Directory monitoring for new CSV files (inbox pattern)
    * Structural validation and data normalization
    * DataPoint streaming for the Publisher
    * File archiving after publish confirmation

Does NOT handle MQTT publishing - that responsibility belongs to MQTTPublisher.
"""
from dataclasses import dataclass
import logging
import shutil
import threading
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd

from core import DataPoint


@dataclass(slots=True)
class MappedRow:
    timestamp: datetime
    tags: dict[str, str]
    fields: dict[str, Any]


class BaseDataSource(ABC):
    """
    Abstract base class for CSV file data extraction.

    Responsibilities:
        * Continuous monitoring of the inbox directory for new CSVs
        * Structural validation (required columns)
        * DataPoint streaming via iterators
        * Archiving to processed/ after broker confirmation

    Reliability policy:
        * Broker-level Acknowledgment: ensures files are moved only upon broker ACK
        * At-least-once delivery: if confirmation doesn't arrive, file stays in inbox
        * Graceful shutdown: interruptible at any time

    Responsibility split:
        * Parent (BaseDataSource): polling loop, validation, file lifecycle, DataPoint streaming & sanitization
        * Decorator (@register_datasource): injects DEVICE_TYPE
        * Child (Subclass): REQUIRED_COLUMNS, _coerce_and_clean, _map_rows (DataFrame iteration)

    Usage:
        * for file_path, datapoints in datasource.iter_files(): ...
    """

    MEASUREMENT_NAME = "biometrics"
    DEVICE_TYPE: str

    @property
    @abstractmethod
    def REQUIRED_COLUMNS(self) -> tuple[str, ...]:
        """Tuple of mandatory columns in the CSV."""
        pass

    def __init__(
        self,
        data_path: Path | str,
        poll_interval: float,
        stop_requested: threading.Event,
        device_id: str,
    ):
        """
        Initializes a DataSource ready for iter_files().

        Args:
            data_path: Directory containing the CSV files.
            poll_interval: Seconds between each check for new files.
            stop_requested: Shared event for coordinated shutdown.
            device_id: Unique identifier of the sensor instance.
        """
        # Validation
        if not isinstance(poll_interval, (int, float)) or poll_interval <= 0:
            raise ValueError(f"poll_interval must be > 0, received: {poll_interval}")
    
        # Configuration
        self._data_path = Path(data_path)
        self._poll_interval = poll_interval
        self.device_id = device_id

        # Derived
        self._processed_path = self._data_path / "processed"
        self._failed_path = self._data_path / "failed"

        # State
        self._stop_requested = stop_requested
        self._pending_files_count = 0

        self._logger = logging.getLogger(f"{self.__class__.__name__}[{device_id}]")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def iter_files(self) -> Iterator[tuple[Path, Iterator[DataPoint]]]:
        """
        Generates (file_path, datapoints) tuples in continuous mode.

        Behavior:
            1. Directory setup (creates inbox/processed if needed)
            2. Looks for CSV files in inbox
            3. If files found: yields them one at a time
            4. If no files found: waits with polling
            5. Continues until stop is requested

        Yields:
            Tuple (file_path, Iterator[DataPoint]) for each file.

        Note:
            The caller must invoke mark_as_processed(file_path)
            after confirming publication to the broker.
        """
        self._setup_directories()

        self._logger.info(f"Starting monitoring: {self._data_path}")

        try:
            while not self._stop_requested.is_set():
                files = self._get_pending_files()
                self._pending_files_count = len(files)

                if files:
                    for file_path in files:
                        
                        if self._stop_requested.is_set():
                            break
                        
                        self._pending_files_count -= 1
                            
                        try:
                            df = self._load_and_validate(file_path)
                            if df is None:
                                self.mark_as_failed(file_path)
                                continue
                        except pd.errors.ParserError as e:
                            self._logger.error(f"Malformed CSV {file_path.name}: {e}")
                            self.mark_as_failed(file_path)
                            continue
                        except Exception as e:
                            self._logger.error(f"I/O error loading {file_path.name}: {e}")
                            self.mark_as_failed(file_path)
                            continue

                        datapoints = self._iter_datapoints(df, file_path)
                        yield (file_path, datapoints)
                else:
                    self._stop_requested.wait(timeout=self._poll_interval)

        finally:
            self._logger.info("DataSource terminated")

    def mark_as_processed(self, file_path: Path) -> bool:
        """
        Moves a file to the processed/ folder after publish confirmation.

        Args:
            file_path: Path of the file to archive.

        Returns:
            True if moved successfully, False otherwise.
        """
        try:
            dest = self._processed_path / file_path.name

            # Handle duplicates by appending timestamp
            if dest.exists():
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                dest = self._processed_path / f"{file_path.stem}_{timestamp}{file_path.suffix}"

            shutil.move(str(file_path), str(dest))
            self._logger.debug(f"Archived: {file_path.name} -> processed/")
            return True

        except Exception as e:
            self._logger.error(f"Archiving error {file_path.name}: {e}")
            return False
        
    def mark_as_failed(self, file_path: Path) -> bool:
        """Moves a file to the failed/ folder after definitive failure."""
        try:
            dest = self._failed_path / file_path.name
            if dest.exists():
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                dest = self._failed_path / f"{file_path.stem}_{timestamp}{file_path.suffix}"
            shutil.move(str(file_path), str(dest))
            self._logger.warning(f"Failed file: {file_path.name} -> failed/")
            return True
        except Exception as e:
            self._logger.error(f"Error moving {file_path.name}: {e}")
            return False

    def get_info(self) -> dict[str, Any]:
        """
        Returns datasource information for diagnostics.

        Returns:
            Dictionary with datasource info.
        """
        return {
            "device_id": self.device_id,
            "device_type": self.DEVICE_TYPE,
            "data_path": str(self._data_path),
            "measurement": self.MEASUREMENT_NAME,
        }
    
    def is_idle(self) -> bool:
        """
        Indicates whether the current work is finished: confirms that the internal list
        of detected files is exhausted and no new ones have arrived on disk.
        """
        if self._pending_files_count <= 0:
            # If so, do a disk check before declaring idle
            actual_files = self._get_pending_files()
            return len(actual_files) == 0
    
        return False

    # =========================================================================
    # PROCESSING
    # =========================================================================

    def _iter_datapoints(self, df: pd.DataFrame, file_path: Path) -> Iterator[DataPoint]:
        """
        Orchestrates loading and guarantees data quality.

        This function acts as a Gatekeeper:
            1. Handles I/O and parsing errors during CSV loading
            2. Delegates DataPoint extraction to the subclass
            3. Sanitizes results: removes None, forces tags to string
            4. Reports anomalies via log (once per file)

        Args:
            file_path: Path of the file to process.

        Yields:
            Validated and sanitized DataPoints.

        Note:
            Logic bugs in _row_to_datapoint (child) bubble up to the caller.
            Only I/O errors are caught here.
        """
        # Processing with output sanitization
        has_warned_fields = False
        has_warned_tags = False

        for row in self._map_rows(df, file_path):
            ts = row.timestamp
            if pd.isna(ts):
                continue

            if hasattr(ts, "to_pydatetime"):
                clean_timestamp = ts.to_pydatetime()
            else:
                clean_timestamp = ts
            
            # Remove None from fields (InfluxDB does not accept them)
            clean_fields = {}
            for key, value in row.fields.items():
                if not pd.isna(value):
                    clean_fields[key] = value
                elif not has_warned_fields:
                    self._logger.warning(f"None values detected in fields of {file_path.name}, will be removed")
                    has_warned_fields = True

            # Discard points with no useful data
            if not clean_fields:
                continue

            # Remove None from tags and convert to string
            clean_tags = {}
            for key, value in row.tags.items():
                if not pd.isna(value):
                    clean_tags[str(key)] = str(value)
                elif not has_warned_tags:
                    self._logger.warning(f"None values detected in tags of {file_path.name} (key='{key}')")
                    has_warned_tags = True
        
            dp = DataPoint(
                timestamp=clean_timestamp,
                measurement=self.MEASUREMENT_NAME,
                device_type=self.DEVICE_TYPE,
                device_id=self.device_id,
                tags=clean_tags,
                fields=clean_fields,
            )

            yield dp

    # =========================================================================
    # HOOKS FOR SUBCLASSES
    # =========================================================================

    @abstractmethod
    def _map_rows(self, df: pd.DataFrame, file_path: Path) -> Iterator[MappedRow]:
        """
        Transforms the DataFrame into a MappedRow iterator.

        Args:
            df: Already validated and normalized DataFrame.
            file_path: File path (for contextual metadata).

        Yields:
            MappedRow containing the extracted timestamp, tags, and fields.

        Note:
            Do not handle I/O exceptions (handled by the caller).
        """
        pass

    def _coerce_and_clean(self, df: pd.DataFrame, file_path: Path) -> pd.DataFrame | None:
        """
        Hook for type-specific transformations.

        Default implementation: no transformation.
        Override for type conversions, value cleanup, etc.

        Args:
            df: Structurally validated DataFrame.
            file_path: File path (for logging).

        Returns:
            Cleaned DataFrame or None if unrecoverable.
        """
        return df

    # =========================================================================
    # INTERNAL UTILITIES
    # =========================================================================

    def _setup_directories(self) -> None:
        """Creates required directories if they do not exist."""
        if not self._data_path.exists():
            self._data_path.mkdir(parents=True)
            self._logger.info(f"Created inbox directory: {self._data_path}")

        if not self._processed_path.exists():
            self._processed_path.mkdir(parents=True)
            self._logger.info(f"Created processed directory: {self._processed_path}")
        
        if not self._failed_path.exists():
            self._failed_path.mkdir(parents=True)
            self._logger.info(f"Created failed directory: {self._failed_path}")

    def _load_and_validate(self, file_path: Path) -> pd.DataFrame | None:
        """
        Pipeline: load CSV -> validate columns -> apply cleanup.

        Args:
            file_path: Path of the CSV file.

        Returns:
            Validated DataFrame or None if invalid.
        """
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower().str.strip()

        if df.empty:
            self._logger.warning(f"{file_path.name}: empty file")
            return None

        if self.REQUIRED_COLUMNS:
            missing = set(self.REQUIRED_COLUMNS) - set(df.columns)
            if missing:
                self._logger.warning(f"{file_path.name}: missing columns {missing}")
                return None

        return self._coerce_and_clean(df, file_path)

    def _get_pending_files(self) -> list[Path]:
        """
        Retrieves pending CSV files, excluding processed/.

        Returns:
            List of Path objects for files to process.

        Note:
            To avoid race conditions, files should be created with a temporary
            extension and renamed to .csv once writing is complete.
        """
        if not self._data_path.is_dir():
            return []

        files = [
            f for f in sorted(self._data_path.rglob("*.csv"))
            if not f.is_relative_to(self._processed_path)
            and not f.is_relative_to(self._failed_path)
        ]

        return files