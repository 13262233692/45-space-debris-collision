import re
import datetime
import numpy as np
import pandas as pd
from sgp4.api import Satrec, WGS72
from sgp4.io import twoline2rv
from sgp4.earth_gravity import wgs72
from config import DEFAULT_PROPAGATION_HOURS, DEFAULT_TIME_STEP_MINUTES, EARTH_RADIUS_KM


def parse_tle_lines(lines):
    results = []
    i = 0
    while i < len(lines) - 1:
        line1 = lines[i].strip()
        line2 = lines[i + 1].strip()
        if line1.startswith("1 ") and line2.startswith("2 "):
            results.append((line1, line2))
            i += 2
        elif line1.startswith("0 ") or (not line1.startswith("1 ") and not line1.startswith("2 ")):
            line0 = line1
            line1 = lines[i + 1].strip() if i + 1 < len(lines) else ""
            line2 = lines[i + 2].strip() if i + 2 < len(lines) else ""
            if line1.startswith("1 ") and line2.startswith("2 "):
                results.append((line0, line1, line2))
                i += 3
            else:
                i += 1
        else:
            i += 1
    return results


def parse_tle_file(filepath):
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return parse_tle_lines(lines)


def extract_satellite_id(line1, line2):
    match = re.search(r"^1\s+(\d+)", line1)
    if match:
        return match.group(1)
    match = re.search(r"^2\s+(\d+)", line2)
    if match:
        return match.group(1)
    return f"unknown_{hash(line1 + line2) % 1000000:06d}"


def extract_orbital_params(line2):
    try:
        inclination = float(line2[8:16].strip())
        raan = float(line2[17:25].strip())
        eccentricity = float("0." + line2[26:33].strip())
        arg_perigee = float(line2[34:42].strip())
        mean_anomaly = float(line2[43:51].strip())
        mean_motion = float(line2[52:63].strip())
        mu = 398600.4418
        a = (mu / (mean_motion * 2 * np.pi / 86400.0) ** 2) ** (1.0 / 3.0)
        altitude_km = a - EARTH_RADIUS_KM
        return {
            "inclination": inclination,
            "raan": raan,
            "eccentricity": eccentricity,
            "arg_perigee": arg_perigee,
            "mean_anomaly": mean_anomaly,
            "mean_motion": mean_motion,
            "semi_major_axis_km": a,
            "altitude_km": altitude_km,
        }
    except (ValueError, IndexError):
        return None


def classify_orbit(altitude_km, eccentricity):
    if eccentricity > 0.1:
        return "HEO"
    if altitude_km < 2000:
        return "LEO"
    if 35286 < altitude_km < 36286:
        return "GEO"
    return "MEO"


def build_satrec_from_tle(tle_entry):
    if len(tle_entry) == 3:
        _, line1, line2 = tle_entry
    else:
        line1, line2 = tle_entry

    try:
        satrec = Satrec.twoline2rv(line1, line2, WGS72)
        return satrec
    except Exception:
        try:
            satrec = twoline2rv(line1, line2, wgs72)
            return satrec
        except Exception:
            return None


def propagate_single_j2000(satrec, jd_start, jd_end, step_minutes=DEFAULT_TIME_STEP_MINUTES):
    if satrec is None:
        return None

    step_days = step_minutes / 1440.0
    total_days = jd_end - jd_start
    n_steps = int(total_days / step_days) + 1

    positions = np.empty((n_steps, 3), dtype=np.float64)
    velocities = np.empty((n_steps, 3), dtype=np.float64)
    times_jd = np.empty(n_steps, dtype=np.float64)
    valid = 0

    for i in range(n_steps):
        jd = jd_start + i * step_days
        fr = 0.0
        try:
            e, r, v = satrec.sgp4(jd, fr)
            if e == 0 and r is not None and v is not None:
                positions[valid] = r
                velocities[valid] = v
                times_jd[valid] = jd
                valid += 1
        except Exception:
            continue

    if valid == 0:
        return None

    return {
        "times_jd": times_jd[:valid],
        "positions_km": positions[:valid],
        "velocities_kms": velocities[:valid],
    }


def propagate_chunk(tle_chunk, jd_start, jd_end, step_minutes=DEFAULT_TIME_STEP_MINUTES):
    chunk_results = []

    for tle_entry in tle_chunk:
        if len(tle_entry) == 3:
            name, line1, line2 = tle_entry
            sat_id = name.strip() if name.strip() else extract_satellite_id(line1, line2)
        else:
            line1, line2 = tle_entry
            sat_id = extract_satellite_id(line1, line2)

        satrec = build_satrec_from_tle(tle_entry)
        if satrec is None:
            continue

        prop = propagate_single_j2000(satrec, jd_start, jd_end, step_minutes)
        if prop is None:
            continue

        orb_params = extract_orbital_params(line2)
        altitude_km = orb_params["altitude_km"] if orb_params else 0.0
        eccentricity = orb_params["eccentricity"] if orb_params else 0.0
        category = classify_orbit(altitude_km, eccentricity)

        chunk_results.append(
            {
                "satellite_id": sat_id,
                "category": category,
                "altitude_km": altitude_km,
                "orbital_params": orb_params,
                "times_jd": prop["times_jd"],
                "positions_km": prop["positions_km"],
                "velocities_kms": prop["velocities_kms"],
            }
        )

    return chunk_results


def propagate_partition_to_dataframe(tle_partition, jd_start, jd_end, step_minutes=DEFAULT_TIME_STEP_MINUTES):
    rows = []

    for tle_entry in tle_partition:
        if len(tle_entry) == 3:
            name, line1, line2 = tle_entry
            sat_id = name.strip() if name.strip() else extract_satellite_id(line1, line2)
        else:
            line1, line2 = tle_entry
            sat_id = extract_satellite_id(line1, line2)

        satrec = build_satrec_from_tle(tle_entry)
        if satrec is None:
            continue

        prop = propagate_single_j2000(satrec, jd_start, jd_end, step_minutes)
        if prop is None:
            continue

        orb_params = extract_orbital_params(line2)
        altitude_km = orb_params["altitude_km"] if orb_params else 0.0
        eccentricity = orb_params["eccentricity"] if orb_params else 0.0
        category = classify_orbit(altitude_km, eccentricity)

        n_pts = len(prop["positions_km"])
        for t in range(n_pts):
            rows.append(
                {
                    "satellite_id": sat_id,
                    "category": category,
                    "altitude_km": altitude_km,
                    "time_step": t,
                    "time_jd": prop["times_jd"][t],
                    "x_km": prop["positions_km"][t, 0],
                    "y_km": prop["positions_km"][t, 1],
                    "z_km": prop["positions_km"][t, 2],
                    "vx_kms": prop["velocities_kms"][t, 0],
                    "vy_kms": prop["velocities_kms"][t, 1],
                    "vz_kms": prop["velocities_kms"][t, 2],
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "satellite_id", "category", "altitude_km", "time_step",
                "time_jd", "x_km", "y_km", "z_km", "vx_kms", "vy_kms", "vz_kms",
            ]
        )

    return pd.DataFrame(rows)


def propagate_partition_to_summary(tle_partition, jd_start, jd_end, step_minutes=DEFAULT_TIME_STEP_MINUTES):
    rows = []

    for tle_entry in tle_partition:
        if len(tle_entry) == 3:
            name, line1, line2 = tle_entry
            sat_id = name.strip() if name.strip() else extract_satellite_id(line1, line2)
        else:
            line1, line2 = tle_entry
            sat_id = extract_satellite_id(line1, line2)

        satrec = build_satrec_from_tle(tle_entry)
        if satrec is None:
            continue

        prop = propagate_single_j2000(satrec, jd_start, jd_end, step_minutes)
        if prop is None:
            continue

        orb_params = extract_orbital_params(line2)
        altitude_km = orb_params["altitude_km"] if orb_params else 0.0
        eccentricity = orb_params["eccentricity"] if orb_params else 0.0
        category = classify_orbit(altitude_km, eccentricity)

        rows.append(
            {
                "satellite_id": sat_id,
                "category": category,
                "altitude_km": altitude_km,
                "eccentricity": eccentricity,
                "n_trajectory_points": len(prop["positions_km"]),
                "inclination": orb_params["inclination"] if orb_params else 0.0,
                "raan": orb_params["raan"] if orb_params else 0.0,
                "mean_motion": orb_params["mean_motion"] if orb_params else 0.0,
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "satellite_id", "category", "altitude_km", "eccentricity",
                "n_trajectory_points", "inclination", "raan", "mean_motion",
            ]
        )

    return pd.DataFrame(rows)


def compute_jd_from_datetime(dt=None):
    if dt is None:
        dt = datetime.datetime.utcnow()
    year = dt.year
    month = dt.month
    day = dt.day
    hour = dt.hour
    minute = dt.minute
    second = dt.second + dt.microsecond / 1e6

    if month <= 2:
        year -= 1
        month += 12

    A = int(year / 100)
    B = 2 - A + int(A / 4)
    jd = int(365.25 * (year + 4716)) + int(30.6001 * (month + 1)) + day + B - 1524.5
    jd += (hour + minute / 60.0 + second / 3600.0) / 24.0
    return jd


def compute_propagation_window(hours=DEFAULT_PROPAGATION_HOURS):
    jd_start = compute_jd_from_datetime()
    jd_end = jd_start + hours / 24.0
    return jd_start, jd_end
