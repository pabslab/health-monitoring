from pathlib import Path
from collections.abc import Iterator

import pandas as pd

from datasources import BaseDataSource, MappedRow
from datasources.registry import register_datasource

@register_datasource("colmi_r02_ring")
class ColmiR02RingDataSource(BaseDataSource):
    """
    DataSource for Smart Ring (Colmi R02) signals.

    This class processes high-frequency sensor data from wearable rings by:
        1. Parsing CSV files containing absolute timestamps and multi-modal signals.
        2. Converting localized or "mixed" format timestamps into absolute UTC.
        3. Extracting tri-axial accelerometer data, PPG, and optional SpO2 levels.
        4. Associating records with device metadata and session identifiers.

    Data Structure:
        * measurement: "biometrics" (from Base Class).
        * device_type: "colmi_r02_ring".
        * tags: includes 'patient_id' and 'session_id'.
        * fields: includes 'accX', 'accY', 'accZ', 'ppg' (float) and optional 'spO2' (float).
    """
    DEVICE_TYPE: str # Injected from decorator (could also delete this line but EIBTI)

    DEFAULT_PATIENT_ID = "lab_test_user"

    # CSV column names
    COL_TIMESTAMP = "timestamp"
    COL_ACC_X = "accx"
    COL_ACC_Y = "accy"
    COL_ACC_Z = "accz"
    COL_PPG = "ppg"
    COL_SPO2 = "spo2"
    
    @property
    def REQUIRED_COLUMNS(self) -> tuple[str, ...]:
        return (self.COL_TIMESTAMP, self.COL_ACC_X, self.COL_ACC_Y, 
                self.COL_ACC_Z, self.COL_PPG, self.COL_SPO2)

    # Python uses __init__ from base_data_source

    def _map_rows(self, df: pd.DataFrame, file_path: Path) -> Iterator[MappedRow]:
        """
        Performs the row mapping specific to the "colmi_r02_ring" sensor.

        Converts the absolute timestamp to a UTC datetime and maps all signals
        (accelerometer, ppg, spO2) into the MappedRow fields.
        """

        # Session ID from filename (e.g. "ring_data_20251210_172000.csv" -> "ring_data_20251210_172000")
        session_id = file_path.stem

        tags = {
            "patient_id": self.DEFAULT_PATIENT_ID,
            "session_id": session_id
        }

        # Vectorized timestamp pre-computation (O(1) instead of O(n)).
        # "mixed" format because sensors are not always consistent in how they write files
        # (e.g. "2025-12-10 17:20:21" for the first 10 rows, then once load increases they
        # start writing "2025-12-10 17:20:21.960"). floor("us") drops nanoseconds.
        df["calc_timestamp"] = pd.to_datetime(
            df[self.COL_TIMESTAMP],
            format="mixed",
            errors="coerce",
            utc=True
        ).dt.floor("us")

        df = df.dropna(subset=["calc_timestamp"])

        # itertuples is 10-100x faster than iterrows
        for row in df.itertuples(index=False):
            try:
                fields = {
                    "accX": float(getattr(row, self.COL_ACC_X)),
                    "accY": float(getattr(row, self.COL_ACC_Y)),
                    "accZ": float(getattr(row, self.COL_ACC_Z)),
                    "ppg": float(getattr(row, self.COL_PPG)),
                }
            
                # If spo2 is missing, we don't add it
                spo2_value = getattr(row, self.COL_SPO2)
                if pd.notna(spo2_value):
                    fields["spO2"] = float(spo2_value)
                
                yield MappedRow(
                    timestamp=row.calc_timestamp.to_pydatetime(),
                    tags=tags.copy(),
                    fields=fields,
                )
            except (ValueError, TypeError):
                continue

    def _coerce_and_clean(self, df: pd.DataFrame, file_path: Path) -> pd.DataFrame | None:
        """
        Converts columns to numeric and removes invalid rows.
        
        This sensor requires numeric columns for signals, so we use pd.to_numeric
        with coerce to handle malformed values in a vectorized way.
        The timestamp is handled separately in _process_file.
        """
        initial_count = len(df)
        
        # Vectorized conversion of numeric columns (excluding timestamp)
        numeric_cols = [self.COL_ACC_X, self.COL_ACC_Y, self.COL_ACC_Z, self.COL_PPG, self.COL_SPO2]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Rows with missing data in essential columns must be dropped.
        # Rows with a missing spO2 value are accepted.
        essential_columns = [self.COL_TIMESTAMP, self.COL_ACC_X, 
                               self.COL_ACC_Y, self.COL_ACC_Z, self.COL_PPG]
        df = df.dropna(subset=essential_columns)
        
        dropped = initial_count - len(df)
        if dropped > 0:
            self._logger.warning(
                f"{file_path.name}: dropped {dropped}/{initial_count} invalid rows"
            )
        
        if df.empty:
            self._logger.warning(f"{file_path.name}: no valid rows after cleanup")
            return None
            
        return df
