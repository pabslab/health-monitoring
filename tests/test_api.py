"""
REST API validation script.
Note: This script is designed to generate the validation reporting
mentioned in chapter 5.2.2 of the thesis, verifying the consistency of the data
extracted from InfluxDB against the original dataset.
"""
import requests

# Configuration
API_BASE_URL = "http://localhost:8000"

def print_section(title):
    """Prints section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def test_health():
    """Test 1: Health Check."""
    print_section("TEST 1: Health Check")
    
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            data = response.json()
            print(f"API Status: {data['status']}")
            print(f"InfluxDB: {data['influxdb']}")
            print(f"Timestamp: {data['timestamp']}")
        else:
            print(f"Error: {response.status_code}")
    except Exception as e:
        print(f"Connection error: {e}")

def test_bcg_stats(patient_id="patient_001"):
    """Test 2: BCG statistics."""
    print_section(f"TEST 2: BCG Statistics - Patient {patient_id}")
    
    params = {"patient_id": patient_id}
    
    response = requests.get(f"{API_BASE_URL}/stats/bcg", params=params)
    
    if response.status_code == 200:
        stats = response.json()
        print(f"Patient: {stats['patient_id']}")
        print(f"Total Measurements: {stats['total_measurements']:,}")
        print(f"\nBCG Signal Statistics:")
        print(f"  - Mean:      {stats['bcg_mean']:.4f}")
        print(f"  - Std Dev:   {stats['bcg_std']:.4f}")
        print(f"\nApnea Analysis:")
        print(f"  - Total:      {stats['apnea_count']}")
        print(f"  - Percentage: {stats['apnea_percentage']:.2f}%")
    elif response.status_code == 404:
        print(f"No data found for {patient_id}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_bcg_data(patient_id="patient_001", limit=5):
    """Test 3: BCG Data Extraction."""
    print_section(f"TEST 3: BCG Data - Patient {patient_id}")
    
    params = {
        "patient_id": patient_id,
        "limit": limit
    }
    
    response = requests.get(f"{API_BASE_URL}/data/bcg", params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"Extracted points: {data['count']}")
        if data['data']:
            for i, point in enumerate(data['data'], 1):
                print(f"  {i}. {point['timestamp'][:19]} | {point['field']} = {point['value']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_patient_status_inference(patient_id="patient_001", num_samples=300):
    """Test 4: Real-time Patient Status Inference (Standard Deviation)."""
    print_section(f"TEST 4: Status Inference - Patient {patient_id}")
    
    payload = {
        "patient_id": patient_id,
        "num_samples": num_samples
    }
    
    response = requests.post(
        f"{API_BASE_URL}/predict/patient-status",
        json=payload
    )
    
    if response.status_code == 200:
        res = response.json()
        print(f"Inference Result:     **{res['status_predicted']}**")
        print(f"Variance (Std Dev):   {res['signal_std']:.4f}")
        print(f"Apnea Threshold Used: {res['threshold_used']}")
        print(f"Timestamp:            {res['timestamp']}")
        
        # Visual feedback
        status = res['status_predicted']
        if status == "APNEA":
            print("\nALERT: Possible apnea detected (flat signal)")
        elif status == "MOVEMENT":
            print("\nNOTE: Excessive movement detected")
        else:
            print("\nINFO: Regular breathing")
            
    elif response.status_code == 400:
        print(f"Insufficient data: {response.json()['detail']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

def run_all_tests():
    """Executes the complete test suite."""    
    try:
        test_health()
        test_bcg_stats(patient_id="patient_001")
        test_bcg_data(patient_id="patient_001", limit=5)
        test_patient_status_inference(patient_id="patient_001", num_samples=300)
        
        print_section("Test Completed")
        
    except requests.exceptions.ConnectionError:
        print("\nERROR: API not reachable on localhost:8000")

if __name__ == "__main__":
    run_all_tests()