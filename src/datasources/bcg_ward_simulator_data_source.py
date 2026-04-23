from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Iterator

import pandas as pd

from datasources import BaseDataSource, MappedRow
from datasources.registry import register_datasource

@register_datasource("bcg_ward_simulator")
class BcgWardSimulatorDataSource(BaseDataSource):
    """
    DataSource for simulated Ballistocardiogram (BCG) signals.

    This class simulates a ward monitoring scenario by:
        1. Parsing CSV files where time is expressed as a relative offset (seconds).
        2. Converting offsets into absolute UTC timestamps starting from today's midnight.
        3. Extracting BCG signal values and binary apnea labels (0/1).
        4. Associating data with a specific patient identified by the filename.

    Data Structure:
        * measurement: "biometrics" (from Base Class).
        * device_type: "bcg_ward_simulator".
        * tags: includes 'patient_id' (derived from CSV filename).
        * fields: includes 'bcg' (float) and 'is_apnea' (int).
    """
    DEVICE_TYPE: str # Injected from decorator (could also delete this line but EIBTI)
    
    # CSV column names
    COL_TIME = "time"
    COL_BCG = "bcg"
    COL_APNEA = "label"

    # For converting offset -> datetime (BCG-specific; other sensors have absolute timestamps).
    BASE_TIME = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    @property
    def REQUIRED_COLUMNS(self) -> tuple[str, ...]:
        return (self.COL_TIME, self.COL_BCG, self.COL_APNEA)
    
    # Python uses __init__ from base_data_source

    def _map_rows(self, df: pd.DataFrame, file_path: Path) -> Iterator[MappedRow]:
        """
        Performs the row mapping specific to the "bcg_ward_simulator" sensor.

        Converts the time offset into a UTC timestamp and maps the BCG signal
        and apnea label into the MappedRow fields.
        """
        # Patient ID from filename (e.g. "patient_001.csv" -> "patient_001")
        patient_id = file_path.stem

        tags = {
            "patient_id": patient_id
        }

        # Vectorized timestamp pre-computation (O(1) instead of O(n))
        df["calc_timestamp"] = (
            self.BASE_TIME + pd.to_timedelta(df[self.COL_TIME], unit="s")
        ).dt.floor("us")  # Drop nanoseconds

        df = df.dropna(subset=["calc_timestamp"])

        # itertuples is 10-100x faster than iterrows
        for row in df.itertuples(index=False):
            try:
                fields = {
                    "bcg": float(getattr(row, self.COL_BCG)),
                    "is_apnea": int(getattr(row, self.COL_APNEA)),
                }

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
        
        This sensor requires all numeric columns, so we use pd.to_numeric
        with coerce to handle malformed values in a vectorized way.
        """
        initial_count = len(df)
        
        # Vectorized conversion: more efficient than per-row try/except
        for col in self.REQUIRED_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        df = df.dropna(subset=list(self.REQUIRED_COLUMNS))
        
        dropped = initial_count - len(df)
        if dropped > 0:
            self._logger.warning(
                f"{file_path.name}: dropped {dropped}/{initial_count} non-numeric rows"
            )
        
        if df.empty:
            self._logger.warning(f"{file_path.name}: no valid rows after cleanup")
            return None
            
        return df