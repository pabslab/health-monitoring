"""
FastAPI - API for data extraction and inference from InfluxDB.

Endpoints:
    GET  /health                 - Health check
    GET  /data/bcg               - Extract BCG data (with optional filters)
    GET  /stats/bcg              - Aggregate BCG statistics
    POST /predict/patient-status - Real-time inference on patient status (Variance)
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from pydantic import BaseModel, Field

from config import InfluxSettings, setup_logging

settings = InfluxSettings()
setup_logging(settings.log_level)
logger = logging.getLogger("FastAPI")

# =============================================================================
# ANNOTATED TYPE DEFINITIONS
# =============================================================================

PatientID = Annotated[
    str, 
    Field(
        description="Unique patient ID (letters, numbers, '-' or '_')",
        pattern="^[a-zA-Z0-9_-]+$" # Pydantic validates the regex
    )
]

Limit = Annotated[
    int, 
    Query(description="Number of MOST RECENT samples to extract", ge=1, le=10000)
]

NumSamples = Annotated[
    int, 
    Field(description="Number of samples for inference", ge=10, le=10000)
]

# =============================================================================
# LIFESPAN
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs when the server starts up
    client = InfluxDBClientAsync(url=settings.influx_url, token=settings.influx_token, org=settings.influx_org)

    # Verify environment variables
    try:
        # Verify host and port
        if not await client.ping():
            raise ConnectionError("InfluxDB host/port unreachable.")

        # Verify token, org and bucket via a minimal query
        # If any of these is wrong, query_api.query() will raise an exception.
        query_api = client.query_api()
        
        # Look for only the last second of data (very fast)
        # If the bucket/org does not exist -> 404 Error
        # If the token is wrong -> 401 Error
        check_query = f'from(bucket: "{settings.influx_bucket}") |> range(start: -1s) |> limit(n: 1)'
        
        await query_api.query(query=check_query)
        logger.info("Connection to InfluxDB established (Pool opened)")

    except Exception as e:
        await client.close()
        logger.critical(f"CONFIGURATION ERROR: {e}", exc_info=True)
        raise SystemExit(1)

    app.state.influx = client 
    
    yield # Here the API "breathes" and waits for requests
    
    # Runs when the server shuts down (CTRL+C)
    await client.close()
    logger.info("Connection to InfluxDB closed correctly")

# =============================================================================
# FASTAPI APP
# =============================================================================
app = FastAPI(
    title="Healthcare Monitoring API",
    description="API for data extraction and inference from physiological sensors",
    version="1.0",
    lifespan = lifespan
)

# CORS to allow calls from Grafana or external frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    influxdb: str
    timestamp: str

class BcgDataPoint(BaseModel):
    timestamp: str
    field: str
    value: float

class BcgDataResponse(BaseModel):
    count: int
    patient_id: str
    data: list[BcgDataPoint]

class StatsResponse(BaseModel):
    patient_id: str
    total_measurements: int
    bcg_mean: float
    bcg_std: float
    bcg_min: float
    bcg_max: float
    apnea_count: int
    apnea_percentage: float

class StatusRequest(BaseModel):
    """Request to analyze the current patient status."""
    patient_id: str = Field(
        default="patient_001",
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Unique patient ID"
    )
    num_samples: NumSamples = 300

class StatusResponse(BaseModel):
    """Inference response on patient status."""
    patient_id: str
    status_predicted: str      # "NORMAL", "APNEA", "MOVEMENT"
    signal_std: float          # The computed value (standard deviation)
    threshold_used: float      # Alarm threshold exceeded (0.0 if NORMAL)
    timestamp: str

# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/", tags=["root"])
async def root():
    """Root endpoint - API information."""
    return {
        "message": "Healthcare Monitoring API",
        "version": "1.0",
        "endpoints": {
            "health": "/health",
            "bcg_data": "/data/bcg",
            "bcg_stats": "/stats/bcg",
            "patient_status": "/predict/patient-status"
        }
    }

@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health_check(request: Request):
    """Verify InfluxDB connection."""
    logger.info("System health check requested")

    client = request.app.state.influx
    try:
        is_alive = await client.ping()
        influx_status = "healthy" if is_alive else "unhealthy"
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        influx_status = f"error: {str(e)}"
        
    return HealthResponse(
        status="ok",
        influxdb=influx_status,
        timestamp=datetime.now(timezone.utc).isoformat()
    )

@app.get("/data/bcg", response_model=BcgDataResponse, tags=["data"])
async def get_bcg_data(
    request: Request,
    patient_id: PatientID = "patient_001",
    limit: Limit = 100
):
    """Extract raw BCG data from InfluxDB."""
    logger.info(f"Retrieving raw BCG data for patient: {patient_id}")
    client = request.app.state.influx
    query_api = client.query_api()

    # Fetch the latest available data (last 7 days)
    flux_query = '''
    from(bucket: bucket_name)
    |> range(start: -7d)
    |> filter(fn: (r) => r["_measurement"] == "biometrics")
    |> filter(fn: (r) => r["device_type"] == "bcg_ward_simulator")
    |> filter(fn: (r) => r["patient_id"] == p_id)
    |> filter(fn: (r) => r["_field"] == "bcg")
    |> group()
    |> sort(columns: ["_time"], desc: true)
    |> limit(n: p_limit)
    '''

    try:
        result = await query_api.query(
            query=flux_query, 
            params={
                "p_id": patient_id, 
                "p_limit": limit,
                "bucket_name": settings.influx_bucket
            }
        )
 
        if not result:
            raise HTTPException(status_code=404, detail="Patient not found or no data available")

        data_points: list[BcgDataPoint] = []
        for table in result:
            for record in table.records:
                data_points.append(BcgDataPoint(
                    timestamp = record.get_time().isoformat(),
                    field = record.get_field(),
                    value = record.get_value()
                ))
        
        # Handle possible empty tables from InfluxDB
        if not data_points:
            raise HTTPException(status_code=404, detail="No data found")
        
        return BcgDataResponse(
            count = len(data_points),
            patient_id = patient_id,
            data = data_points
        ) 
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Retrieval error for {patient_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")

@app.get("/stats/bcg", response_model=StatsResponse, tags=["statistics"])
async def get_bcg_stats(
    request: Request,
    patient_id: PatientID = "patient_001"
):
    """Aggregate statistics on BCG data computed entirely by InfluxDB."""
    logger.info(f"Calculating aggregate BCG statistics for patient: {patient_id}")
    client = request.app.state.influx
    query_api = client.query_api()

    flux_query = '''
    base = from(bucket: bucket_name)
        |> range(start: -7d)
        |> filter(fn: (r) => r["_measurement"] == "biometrics")
        |> filter(fn: (r) => r["device_type"] == "bcg_ward_simulator")
        |> filter(fn: (r) => r.patient_id == p_id)    

    // Isolate BCG data (Float)
    bcg_base = base 
        |> filter(fn: (r) => r._field == "bcg")
        |> group()   

    bcg_base |> mean()   |> yield(name: "mean")
    bcg_base |> stddev() |> yield(name: "std")
    bcg_base |> min()    |> yield(name: "min")
    bcg_base |> max()    |> yield(name: "max")
    bcg_base |> count()  |> yield(name: "count")

    // Isolate apnea data (Integer)
    base 
        |> filter(fn: (r) => r._field == "is_apnea")
        |> filter(fn: (r) => r._value == 1)
        |> group()
        |> count()
        |> yield(name: "apnea_count")
    '''

    try:        
        result = await query_api.query(
            query=flux_query, 
            params={"p_id": patient_id, "bucket_name": settings.influx_bucket}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Patient not found or no data available")

        # Initialize a dictionary to collect results from the various yields
        stats = {
            "mean": None, "std": None, "min": None, "max": None, "count": None, "apnea_count": None
        }

        # Iterate over all returned tables.
        # The yield(name: "x") instruction populates the "result" column with name "x"
        for table in result:
            for record in table.records:
                yield_name = record.values.get("result")
                val = record.get_value()
                if yield_name in stats and val is not None:
                    stats[yield_name] = val
        
        # Define which fields are MANDATORY for BCG statistics
        mandatory_fields = ["mean", "std", "min", "max", "count"]

        if any(stats[field] is None for field in mandatory_fields):
            raise HTTPException(
                status_code=404, 
                detail="Insufficient BCG data to compute statistics"
            )

        apnea_count = int(stats.get("apnea_count") or 0)
        total = int(stats["count"])

        return StatsResponse(
            patient_id=patient_id,
            total_measurements=total,
            bcg_mean=float(stats["mean"]),
            bcg_std=float(stats["std"]),
            bcg_min=float(stats["min"]),
            bcg_max=float(stats["max"]),
            apnea_count=apnea_count,
            apnea_percentage=round((apnea_count / total) * 100, 2)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stats error for {patient_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error calculating statistics: {str(e)}")

# =============================================================================
# PATIENT STATUS INFERENCE (VARIANCE)
# =============================================================================

@app.post("/predict/patient-status", response_model=StatusResponse, tags=["inference"])
async def predict_patient_status(http_request: Request, request: StatusRequest):
    """
    Heuristic inference on patient status (Thresholds calibrated on CSV file data).
    
    Analyzes the standard deviation of the BCG signal over the last N samples:
    - APNEA: < 1.4 (Weak signal, only residual heartbeat)
    - MOVEMENT: > 3.0 (Artifacts from sudden movement)
    - NORMAL: between 1.4 and 3.0 (Regular breathing)
    """
    logger.info(f"Executing status inference for patient: {request.patient_id}")

    APNEA_THRESHOLD = 1.4
    MOVEMENT_THRESHOLD = 3.0

    client = http_request.app.state.influx
    query_api = client.query_api()

    # Fetch the last N samples (independent of sampling frequency)
    flux_query = '''
    from(bucket: bucket_name)
        |> range(start: -7d)
        |> filter(fn: (r) => r["_measurement"] == "biometrics")
        |> filter(fn: (r) => r["device_type"] == "bcg_ward_simulator")
        |> filter(fn: (r) => r["patient_id"] == p_id)
        |> filter(fn: (r) => r["_field"] == "bcg")
        |> group()
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: p_limit)
    '''

    try:        
        result = await query_api.query(
            query=flux_query, 
            params={
                "p_id": request.patient_id, 
                "p_limit": request.num_samples,
                "bucket_name": settings.influx_bucket
            }
        )

        if not result:
            raise HTTPException(status_code=404, detail="Patient not found or no data available")
        
        # Extract only numeric values
        values = []
        for table in result:
            for record in table.records:
                values.append(record.get_value())
        
        if not values:
            raise HTTPException(status_code=404, detail="No data found for this patient")
        if len(values) < 10:
            raise HTTPException(status_code=400, detail="Insufficient data for inference")

        current_std = np.std(values)
        
        status: str = "NORMAL"
        active_threshold: float = 0.0
        
        if current_std < APNEA_THRESHOLD:
            status = "APNEA"
            active_threshold = APNEA_THRESHOLD
        elif current_std > MOVEMENT_THRESHOLD:
            status = "MOVEMENT"
            active_threshold = MOVEMENT_THRESHOLD

        return StatusResponse(
            patient_id=request.patient_id,
            status_predicted=status,
            signal_std=float(current_std),
            threshold_used=active_threshold,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
    except HTTPException:
        # Re-raise already handled 400 or 404 errors
        raise
    except Exception as e:
        # Handle unexpected errors (e.g. InfluxDB unreachable)
        logger.error(f"Inference error for patient {request.patient_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")
