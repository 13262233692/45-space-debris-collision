import os

EARTH_RADIUS_KM = 6371.0
J2000_EPOCH = 2451545.0

DEFAULT_PROPAGATION_HOURS = 72
DEFAULT_TIME_STEP_MINUTES = 10
DEFAULT_CHUNK_SIZE = 500
DEFAULT_DASK_WORKERS = max(1, os.cpu_count() - 1 or 1)
DEFAULT_DASK_THREADS_PER_WORKER = 1

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
