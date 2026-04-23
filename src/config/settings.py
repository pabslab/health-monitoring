"""
Configuration settings for the project using Pydantic.
"""
import logging
from pathlib import Path

from pydantic import BaseModel, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# DATA SOURCES CONFIGURATION
# =============================================================================
class DataSourceConfig(BaseModel):
    """
    Schema for individual data source validation.
    """
    type: str
    enabled: bool = True
    data_path: Path
    poll_interval: int = Field(default=2, gt=0)

# Define the base directory for data storage within the container
DATA_DIR = Path("/app/data")

# Dictionary containing specific configurations for each gateway/device
DATASOURCES = {
    "SIM-EDGE-GATEWAY-01": DataSourceConfig(
        type="bcg_ward_simulator",
        enabled=True,
        data_path=DATA_DIR / "SIM-EDGE-GATEWAY-01",
        poll_interval=2,
    ),
    "MOCK-MAC-A1:B2:C3:D4:E5:F6": DataSourceConfig(
        type="colmi_r02_ring",
        enabled=True,
        data_path=DATA_DIR / "MOCK-MAC-A1B2C3D4E5F6",
        poll_interval=2,
    )
}

# =============================================================================
# VALIDATION LAYER (Pydantic Settings)
# =============================================================================
class BaseAppConfig(BaseSettings):
    """
    Base configuration class containing shared settings (like logging) 
    and the core Pydantic model configuration.
    """
    log_level: str = Field(default="INFO")

    model_config = SettingsConfigDict(
        env_file=".env",           # Automatically looks for and loads a .env file
        env_file_encoding="utf-8",
        extra="ignore"             # Ignores extra variables in .env not defined here
    )

class MQTTSettings(BaseAppConfig):
    """
    Automatically defines and validates MQTT environment variables.
    If a variable is missing or improperly typed, Pydantic blocks execution.
    """
    mqtt_host: str = Field(default="localhost")
    mqtt_port: int = Field(default=1883)
    mqtt_batch_size: int = Field(default=3000)
    mqtt_topic_base: str = Field(default="healthcare")

class InfluxSettings(BaseAppConfig):
    """
    Automatically defines and validates InfluxDB environment variables.
    If a variable is missing or improperly typed, Pydantic blocks execution.
    """
    influx_host: str
    influx_port: int
    influx_token: str
    influx_org: str
    influx_bucket: str

    @computed_field
    def influx_url(self) -> str:
        """Constructs the InfluxDB URL dynamically from host and port."""
        return f"http://{self.influx_host}:{self.influx_port}"

# =============================================================================
# LOGGING SETUP
# =============================================================================
def setup_logging(log_level: str = "INFO"):
    """
    Configures the global logging system.
    
    Args:
        log_level (str): The logging level to use, usually retrieved 
                         from a Pydantic settings instance.
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )