from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class DataPoint:
    """
    Data structure for a single point to be published (it is a DTO - Data Transfer Object).

    Represents the "contract" between DataSource and Publisher:
        * The DataSource produces DataPoints
        * The Publisher consumes DataPoints (without knowing where they come from)

    Attributes:
        * timestamp: Moment of measurement (UTC)
        * measurement: Measurement name
        * device_type: Device type, also used as measurement (e.g. "bcg_ward_simulator", "colmi_r02_ring").
        * device_id: Unique identifier of the sensor instance (e.g. "SIM-EDGE-GATEWAY-01", "MOCK-MAC-A1B2C3D4E5F6")
        * tags: Indexed metadata (e.g. patient_id)
        * fields: Numeric values of the measurement

    Note:
        The dataclass decorator saves us the assignments in __init__
    """
    timestamp: datetime
    measurement: str
    device_type: str
    device_id: str
    tags: dict[str, str]
    fields: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the DataPoint into a dictionary for serialization.
        Optimized for MessagePack: timestamp as float (epoch).

        Returns:
            Dictionary ready for serialization
        """
        return {
            "ts": self.timestamp.timestamp(),
            "m": self.measurement,
            "dt": self.device_type,
            "di": self.device_id,
            "tags": self.tags,
            "fields": self.fields
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataPoint":
        """
        Reconstructs a DataPoint from a dictionary (subscriber side).

        Note: @staticmethod would also work as long as we only use DataPoint,
        but if we were to create a subclass (e.g. BcgDataPoint) it would return
        an object of the wrong type. We use @classmethod because, by receiving
        'cls' as a dynamic reference, it guarantees the creation of the correct
        instance even for any child classes.
        """
        return cls(
            timestamp=datetime.fromtimestamp(data["ts"], tz=timezone.utc),
            measurement=data["m"],
            device_type=data["dt"],
            device_id=data["di"],
            tags=data["tags"],
            fields=data["fields"]
        )
    
    def __repr__(self) -> str:
        """
        Concise textual representation for debugging and logging.

        Instead of printing the entire object (which could be huge), it shows
        the measurement, a preview of the first 3 fields, and the timestamp
        formatted in a readable way, so we don't clutter the terminal during tests.
        """
        fields_preview = list(self.fields.keys())[:3]
        return (
            f"DataPoint(measurement={self.measurement}, "
            f"fields={fields_preview}, "
            f"timestamp={self.timestamp.isoformat(sep=' ', timespec='milliseconds')})"
        )