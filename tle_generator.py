import os
import random
import math
from config import DATA_DIR


LEO_TEMPLATES = [
    (
        "1 25544U 98067A   24001.50000000  .00016717  00000-0  30200-3 0  9993",
        "2 25544  51.6412 200.2345 0006703  50.4518 309.7468 15.49560520 12345",
    ),
    (
        "1 48274U 21035A   24001.50000000  .00007000  00000-0  25000-3 0  9990",
        "2 48274  53.0540 249.0320 0001256  90.4320 269.7880 15.06038000 12340",
    ),
    (
        "1 54032U 22057A   24001.50000000  .00012000  00000-0  28000-3 0  9991",
        "2 54032  97.4500 100.0000 0012000  90.0000 270.0000 14.80000000 12341",
    ),
]

MEO_TEMPLATES = [
    (
        "1 41019U 15052A   24001.50000000  .00000050  00000-0  00000+0 0  9992",
        "2 41019  55.0500 250.0600 0050000  45.0000 315.0000  2.00560000 12342",
    ),
]

GEO_TEMPLATES = [
    (
        "1 32273U 07046A   24001.50000000  .00000010  00000-0  00000+0 0  9994",
        "2 32273   0.0200 100.0000 0001000 270.0000  90.0000  1.00270000 12344",
    ),
]

HEO_TEMPLATES = [
    (
        "1 29245U 06032A   24001.50000000  .00000100  00000-0  00000+0 0  9995",
        "2 29245  63.4000 270.0000 6800000 270.0000  90.0000  2.00560000 12345",
    ),
]


def _compute_checksum(line):
    checksum = 0
    for c in line[:-1]:
        if c.isdigit():
            checksum += int(c)
        elif c == "-":
            checksum += 1
    return str(checksum % 10)


def _generate_tle_from_params(sat_num, epoch_day, inclination, raan, eccentricity, arg_perigee, mean_anomaly, mean_motion, rev_num):
    line1_field = f"1 {sat_num:05d}U 24001A   {epoch_day:012.8f}  .00010000  00000-0  15000-3 0  999"
    line1_field = line1_field.ljust(68)
    line1 = line1_field + _compute_checksum(line1_field)

    ecc_str = f"{eccentricity:.7f}"[2:].ljust(7)
    mm_str = f"{mean_motion:12.8f}"[-12:]
    line2_field = f"2 {sat_num:05d} {inclination:8.4f} {raan:8.4f} {ecc_str}{arg_perigee:8.4f} {mean_anomaly:8.4f} {mm_str}{rev_num:5d}"
    line2_field = line2_field.ljust(68)
    line2 = line2_field + _compute_checksum(line2_field)

    return line1, line2


def generate_leo_debris(n, start_id=80001):
    entries = []
    for i in range(n):
        sat_num = start_id + i
        epoch_day = 24001.5 + random.uniform(-1, 1)
        inclination = random.gauss(51.6, 15)
        inclination = max(0, min(180, inclination))
        raan = random.uniform(0, 360)
        eccentricity = 10 ** random.uniform(-5, -2)
        arg_perigee = random.uniform(0, 360)
        mean_anomaly = random.uniform(0, 360)
        mean_motion = random.uniform(12.5, 16.5)
        rev_num = random.randint(1000, 99999)
        line1, line2 = _generate_tle_from_params(
            sat_num, epoch_day, inclination, raan, eccentricity, arg_perigee, mean_anomaly, mean_motion, rev_num
        )
        entries.append((f"DEBRIS-{sat_num}", line1, line2))
    return entries


def generate_meo_debris(n, start_id=70001):
    entries = []
    for i in range(n):
        sat_num = start_id + i
        epoch_day = 24001.5 + random.uniform(-1, 1)
        inclination = random.gauss(55, 10)
        inclination = max(0, min(180, inclination))
        raan = random.uniform(0, 360)
        eccentricity = 10 ** random.uniform(-5, -2)
        arg_perigee = random.uniform(0, 360)
        mean_anomaly = random.uniform(0, 360)
        mean_motion = random.uniform(1.5, 4.0)
        rev_num = random.randint(100, 9999)
        line1, line2 = _generate_tle_from_params(
            sat_num, epoch_day, inclination, raan, eccentricity, arg_perigee, mean_anomaly, mean_motion, rev_num
        )
        entries.append((f"MEO-{sat_num}", line1, line2))
    return entries


def generate_geo_debris(n, start_id=60001):
    entries = []
    for i in range(n):
        sat_num = start_id + i
        epoch_day = 24001.5 + random.uniform(-1, 1)
        inclination = random.gauss(0.5, 2.0)
        inclination = max(0, min(10, inclination))
        raan = random.uniform(0, 360)
        eccentricity = 10 ** random.uniform(-5, -3)
        arg_perigee = random.uniform(0, 360)
        mean_anomaly = random.uniform(0, 360)
        mean_motion = random.uniform(0.95, 1.05)
        rev_num = random.randint(10, 999)
        line1, line2 = _generate_tle_from_params(
            sat_num, epoch_day, inclination, raan, eccentricity, arg_perigee, mean_anomaly, mean_motion, rev_num
        )
        entries.append((f"GEO-{sat_num}", line1, line2))
    return entries


def generate_heo_debris(n, start_id=50001):
    entries = []
    for i in range(n):
        sat_num = start_id + i
        epoch_day = 24001.5 + random.uniform(-1, 1)
        inclination = random.gauss(63.4, 5)
        inclination = max(20, min(120, inclination))
        raan = random.uniform(0, 360)
        eccentricity = random.uniform(0.3, 0.75)
        arg_perigee = random.gauss(270, 30) % 360
        mean_anomaly = random.uniform(0, 360)
        mean_motion = random.uniform(1.5, 4.0)
        rev_num = random.randint(100, 9999)
        line1, line2 = _generate_tle_from_params(
            sat_num, epoch_day, inclination, raan, eccentricity, arg_perigee, mean_anomaly, mean_motion, rev_num
        )
        entries.append((f"HEO-{sat_num}", line1, line2))
    return entries


def generate_conjunction_debris(n, start_id=90001):
    entries = []
    iss_l1 = "1 25544U 98067A   24001.50000000  .00016717  00000-0  30200-3 0  9993"
    iss_l2 = "2 25544  51.6412 200.2345 0006703  50.4518 309.7468 15.4956052012345"

    for i in range(n):
        sat_num = start_id + i
        rev_num = random.randint(1000, 99999)

        if i < n // 3:
            delta_ma = random.uniform(-0.001, 0.001)
        elif i < 2 * n // 3:
            delta_ma = random.uniform(-0.01, 0.01)
        else:
            delta_ma = random.uniform(-0.05, 0.05)

        delta_raan = random.gauss(0, 0.0001)
        delta_inc = random.gauss(0, 0.0001)

        inc = 51.6412 + delta_inc
        raan = 200.2345 + delta_raan
        ma = 309.7468 + delta_ma

        l1 = "1 {:05d}U".format(sat_num) + iss_l1[8:]
        l2 = "2 {:05d}".format(sat_num) + " {:8.4f}".format(inc) + " {:8.4f}".format(raan) + " 0006703  {:8.4f}".format(50.4518) + " {:8.4f}".format(ma) + " 15.49560520{:5d}".format(rev_num)

        l1_field = l1[:68].ljust(68)
        l1 = l1_field + _compute_checksum(l1_field)
        l2_field = l2[:68].ljust(68)
        l2 = l2_field + _compute_checksum(l2_field)

        entries.append((f"CONJ-{sat_num}", l1, l2))
    return entries


def generate_sample_tle_dataset(
    n_leo=2000,
    n_meo=300,
    n_geo=200,
    n_heo=100,
    n_conjunction=20,
    output_path=None,
):
    random.seed(42)

    entries = []
    entries.extend(generate_leo_debris(n_leo, start_id=80001))
    entries.extend(generate_meo_debris(n_meo, start_id=70001))
    entries.extend(generate_geo_debris(n_geo, start_id=60001))
    entries.extend(generate_heo_debris(n_heo, start_id=50001))
    entries.extend(generate_conjunction_debris(n_conjunction, start_id=90001))

    random.shuffle(entries)

    iss_entry = (
        "ISS (ZARYA)",
        "1 25544U 98067A   24001.50000000  .00016717  00000-0  30200-3 0  9993",
        "2 25544  51.6412 200.2345 0006703  50.4518 309.7468 15.4956052012345",
    )
    entries.insert(0, iss_entry)

    if output_path is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        output_path = os.path.join(DATA_DIR, "sample_tle.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        for entry in entries:
            if len(entry) == 3:
                name, line1, line2 = entry
                f.write(f"{name}\n{line1}\n{line2}\n")
            else:
                line1, line2 = entry
                f.write(f"{line1}\n{line2}\n")

    print(f"Generated {len(entries)} TLE entries -> {output_path}")
    return output_path


if __name__ == "__main__":
    path = generate_sample_tle_dataset()
    print(f"Sample TLE data ready at: {path}")
