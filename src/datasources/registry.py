from typing import Type

# Dynamic registry containing the classes related to devices
SUPPORTED_SENSORS: dict[str, Type] = {}

def register_datasource(sensor_type: str):
    """
    Decorator to automatically register a Data Source class.
    """
    def decorator(cls):
        if sensor_type in SUPPORTED_SENSORS:
            raise ValueError(f"Sensore '{sensor_type}' già registrato con {SUPPORTED_SENSORS[sensor_type].__name__}")
        
        cls.DEVICE_TYPE = sensor_type
        SUPPORTED_SENSORS[sensor_type] = cls
        return cls
    return decorator