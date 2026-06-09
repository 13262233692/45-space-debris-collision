import os

EARTH_RADIUS_KM = 6371.0
J2000_EPOCH = 2451545.0

DEFAULT_PROPAGATION_HOURS = 72
DEFAULT_TIME_STEP_MINUTES = 10
DEFAULT_PARTITION_SIZE = 10000
DEFAULT_CHUNK_SIZE = 10000
DEFAULT_DASK_WORKERS = max(1, os.cpu_count() - 1 or 1)
DEFAULT_DASK_THREADS_PER_WORKER = 1
MAX_VIS_POINTS = 50000

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TLE_CACHE_DIR = os.path.join(DATA_DIR, "tle_cache")

DASH_HOST = "0.0.0.0"
DASH_PORT = 8050
DASH_DEBUG = True

DEBRIS_CATEGORIES = {
    "LEO": {"altitude_range": (160, 2000), "color": "#ff4444", "opacity": 0.6},
    "MEO": {"altitude_range": (2000, 35786), "color": "#ffaa00", "opacity": 0.5},
    "GEO": {"altitude_range": (35786 - 500, 35786 + 500), "color": "#44aaff", "opacity": 0.7},
    "HEO": {"altitude_range": (0, 60000), "color": "#aa44ff", "opacity": 0.4},
}

CA_POC_RED_THRESHOLD = 1e-4
CA_POC_YELLOW_THRESHOLD = 1e-6
CA_POC_GREEN_THRESHOLD = 1e-8

CA_DEFAULT_COVARIANCE_LEO = [0.2, 0.2, 0.2]
CA_DEFAULT_COVARIANCE_MEO = [1.0, 1.0, 1.0]
CA_DEFAULT_COVARIANCE_GEO = [5.0, 5.0, 5.0]

CA_DEFAULT_HARD_BODY_RADIUS = 0.05

CA_TCA_SEARCH_REFINEMENT_STEPS = 3

CA_MAX_CONJUNCTIONS = 200

CA_SCREEN_DISTANCE_KM = 500.0

CA_PRIMARY_TARGETS = ["25544", "ISS (ZARYA)", "48274", "54032"]

PROTECTED_OBJECTS_IDS = ["25544", "48274", "54032"]
