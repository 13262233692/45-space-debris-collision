import os
import time
import shutil
import logging
import functools
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
from dask import delayed
from dask.distributed import Client, LocalCluster

from sgp4_orbit_parser import (
    parse_tle_file,
    propagate_partition_to_dataframe,
    propagate_partition_to_summary,
    propagate_chunk,
    compute_propagation_window,
)
from conjunction_engine import (
    run_conjunction_assessment,
    conjunctions_to_dataframe,
)
from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DASK_WORKERS,
    DEFAULT_DASK_THREADS_PER_WORKER,
    DEFAULT_PROPAGATION_HOURS,
    DEFAULT_TIME_STEP_MINUTES,
    DATA_DIR,
    CA_SCREEN_DISTANCE_KM,
    CA_MAX_CONJUNCTIONS,
)

logger = logging.getLogger(__name__)


def _compute_optimal_partitions(n_objects, n_workers):
    n_parts = max(n_workers * 2, n_objects // 10000)
    n_parts = min(n_parts, 64)
    n_parts = max(n_parts, 2)
    return n_parts


TRAJECTORY_META = {
    "satellite_id": str,
    "category": str,
    "altitude_km": float,
    "time_step": int,
    "time_jd": float,
    "x_km": float,
    "y_km": float,
    "z_km": float,
    "vx_kms": float,
    "vy_kms": float,
    "vz_kms": float,
}

SUMMARY_META = {
    "satellite_id": str,
    "category": str,
    "altitude_km": float,
    "eccentricity": float,
    "n_trajectory_points": int,
    "inclination": float,
    "raan": float,
    "mean_motion": float,
}


class DaskOrbitScheduler:
    def __init__(
        self,
        n_workers=DEFAULT_DASK_WORKERS,
        threads_per_worker=DEFAULT_DASK_THREADS_PER_WORKER,
        chunk_size=DEFAULT_CHUNK_SIZE,
    ):
        self.n_workers = max(1, n_workers)
        self.threads_per_worker = threads_per_worker
        self.chunk_size = chunk_size
        self.client = None
        self.cluster = None

        self._trajectory_ddf = None
        self._summary_df = None
        self._parquet_dir = os.path.join(DATA_DIR, "trajectory_parquet")
        self._summary_path = os.path.join(DATA_DIR, "summary_parquet")
        self._total_objects = 0
        self._n_time_steps = 0
        self._propagation_done = False
        self._propagation_config = {}
        self._snapshot_cache = {}

        self._tle_entries = None
        self._propagation_results = None
        self._conjunctions = None
        self._conjunctions_df = None
        self._ca_done = False

    def start_cluster(self):
        logger.info(f"Starting Dask LocalCluster: workers={self.n_workers}, threads/worker={self.threads_per_worker}")
        self.cluster = LocalCluster(
            n_workers=self.n_workers,
            threads_per_worker=self.threads_per_worker,
            processes=True,
            silence_logs=logging.WARNING,
            memory_limit="4GB",
        )
        self.client = Client(self.cluster)
        logger.info(f"Dask dashboard: {self.client.dashboard_link}")
        return self.client

    def shutdown_cluster(self):
        if self.client:
            self.client.close()
        if self.cluster:
            self.cluster.close()
        logger.info("Dask cluster shut down.")

    def _manual_partition(self, tle_entries, n_partitions):
        partitions = []
        total = len(tle_entries)
        part_size = total // n_partitions
        remainder = total % n_partitions
        start = 0
        for i in range(n_partitions):
            end = start + part_size + (1 if i < remainder else 0)
            partitions.append(tle_entries[start:end])
            start = end
        return partitions

    def submit_propagation(
        self,
        tle_filepath,
        hours=DEFAULT_PROPAGATION_HOURS,
        step_minutes=DEFAULT_TIME_STEP_MINUTES,
        use_distributed=False,
        run_ca=True,
    ):
        t0 = time.time()
        tle_entries = parse_tle_file(tle_filepath)
        n_objects = len(tle_entries)
        logger.info(f"Parsed {n_objects} TLE entries from {tle_filepath}")

        self._tle_entries = tle_entries

        n_partitions = _compute_optimal_partitions(n_objects, self.n_workers)
        jd_start, jd_end = compute_propagation_window(hours)
        logger.info(
            f"Propagation window: JD {jd_start:.4f} -> JD {jd_end:.4f} "
            f"({hours}h, step={step_minutes}min)"
        )
        logger.info(
            f"Coarse-grained partitioning: {n_objects} objects -> {n_partitions} partitions "
            f"(~{n_objects // n_partitions} objects/partition)"
        )

        self._propagation_config = {
            "hours": hours,
            "step_minutes": step_minutes,
            "jd_start": jd_start,
            "jd_end": jd_end,
            "n_objects": n_objects,
            "n_partitions": n_partitions,
        }

        partitions = self._manual_partition(tle_entries, n_partitions)

        scheduler = "distributed" if use_distributed else "threads"
        if use_distributed and self.client is None:
            self.start_cluster()

        logger.info(f"Phase 1: Building task graph with {n_partitions} delayed tasks -> Parquet [{scheduler}]")

        delayed_trajs = []
        delayed_summaries = []
        for part in partitions:
            d_traj = delayed(propagate_partition_to_dataframe)(
                part, jd_start, jd_end, step_minutes
            )
            d_summ = delayed(propagate_partition_to_summary)(
                part, jd_start, jd_end, step_minutes
            )
            delayed_trajs.append(d_traj)
            delayed_summaries.append(d_summ)

        logger.info(f"Task graph: {len(delayed_trajs)} trajectory + {len(delayed_summaries)} summary = {len(delayed_trajs) + len(delayed_summaries)} total tasks")

        with dask.config.set({"scheduler": scheduler}):
            if os.path.exists(self._parquet_dir):
                shutil.rmtree(self._parquet_dir, ignore_errors=True)
            os.makedirs(self._parquet_dir, exist_ok=True)

            traj_ddf = dd.from_delayed(delayed_trajs, meta=TRAJECTORY_META)
            traj_ddf.to_parquet(self._parquet_dir, engine="pyarrow", compression="snappy")
            logger.info(f"Trajectory Parquet written to {self._parquet_dir}")

        logger.info("Phase 2: Writing summary Parquet")

        with dask.config.set({"scheduler": scheduler}):
            if os.path.exists(self._summary_path):
                shutil.rmtree(self._summary_path, ignore_errors=True)
            os.makedirs(self._summary_path, exist_ok=True)

            summ_ddf = dd.from_delayed(delayed_summaries, meta=SUMMARY_META)
            summ_ddf = summ_ddf.repartition(npartitions=max(1, n_partitions // 4))
            summ_ddf.to_parquet(self._summary_path, engine="pyarrow", compression="snappy")
            logger.info(f"Summary Parquet written to {self._summary_path}")

        self._trajectory_ddf = dd.read_parquet(self._parquet_dir, engine="pyarrow")
        self._summary_df = dd.read_parquet(self._summary_path, engine="pyarrow").compute()

        self._total_objects = len(self._summary_df)
        if self._total_objects > 0:
            self._n_time_steps = int(self._summary_df["n_trajectory_points"].max())
        self._propagation_done = True
        self._snapshot_cache.clear()

        if run_ca:
            logger.info("Phase 3: Running Conjunction Assessment (CA) engine")
            self._run_conjunction_assessment(jd_start, jd_end, step_minutes)

        elapsed = time.time() - t0
        logger.info(
            f"Full pipeline complete in {elapsed:.1f}s: "
            f"{self._total_objects} objects, {self._n_time_steps} time steps, "
            f"{n_partitions} partitions"
        )

        return self._summary_df

    def submit_propagation_local(
        self,
        tle_filepath,
        hours=DEFAULT_PROPAGATION_HOURS,
        step_minutes=DEFAULT_TIME_STEP_MINUTES,
    ):
        return self.submit_propagation(
            tle_filepath,
            hours=hours,
            step_minutes=step_minutes,
            use_distributed=False,
        )

    def _run_conjunction_assessment(self, jd_start, jd_end, step_minutes):
        if self._propagation_results is not None:
            all_objects = self._propagation_results
        else:
            logger.info("CA: Re-propagating TLE data to obtain full trajectory objects for CA...")
            all_objects = []
            chunk_size = 500
            for i in range(0, len(self._tle_entries), chunk_size):
                chunk = self._tle_entries[i:i + chunk_size]
                result = propagate_chunk(chunk, jd_start, jd_end, step_minutes)
                if result:
                    all_objects.extend(result)
            self._propagation_results = all_objects
            logger.info(f"CA: {len(all_objects)} objects available for conjunction screening")

        if not all_objects:
            logger.warning("CA: No objects available for conjunction assessment")
            self._ca_done = True
            return

        primary_objects = []
        protected_ids = set()
        for obj in all_objects:
            sat_id = obj["satellite_id"]
            for pid in ["25544", "48274", "54032", "ISS"]:
                if pid in sat_id:
                    primary_objects.append(obj)
                    protected_ids.add(sat_id)
                    break

        if not primary_objects:
            logger.info("CA: No protected objects found, selecting first LEO object as primary")
            for obj in all_objects:
                if obj["category"] == "LEO":
                    primary_objects = [obj]
                    protected_ids.add(obj["satellite_id"])
                    break

            if not primary_objects and all_objects:
                primary_objects = [all_objects[0]]
                protected_ids.add(all_objects[0]["satellite_id"])

        logger.info(f"CA: {len(primary_objects)} primary (protected) objects identified")

        from sgp4_orbit_parser import build_satrec_from_tle, propagate_single_j2000

        fine_step_min = 1
        logger.info(f"CA: Re-propagating {len(primary_objects)} primary objects at {fine_step_min}-min resolution for fine CA screening")
        fine_primary_objects = []
        for p in primary_objects:
            p_satrec = None
            for tle in self._tle_entries:
                if len(tle) == 3:
                    name, l1, l2 = tle
                else:
                    l1, l2 = tle
                from sgp4_orbit_parser import extract_satellite_id
                sid = name.strip() if len(tle) == 3 else extract_satellite_id(l1, l2)
                if sid == p["satellite_id"]:
                    p_satrec = build_satrec_from_tle(tle)
                    break
            if p_satrec is not None:
                fine_prop = propagate_single_j2000(p_satrec, jd_start, jd_end, fine_step_min)
                if fine_prop is not None:
                    fine_p = dict(p)
                    fine_p["positions_km"] = fine_prop["positions_km"]
                    fine_p["velocities_kms"] = fine_prop["velocities_kms"]
                    fine_p["times_jd"] = fine_prop["times_jd"]
                    fine_primary_objects.append(fine_p)
                else:
                    fine_primary_objects.append(p)
            else:
                fine_primary_objects.append(p)

        coarse_max_dist = CA_SCREEN_DISTANCE_KM * 10
        conjunction_candidates = []
        for p in fine_primary_objects:
            p_pos = p["positions_km"]
            if len(p_pos) == 0:
                continue
            for d in all_objects:
                if d["satellite_id"] in protected_ids:
                    continue
                d_pos = d["positions_km"]
                if len(d_pos) == 0:
                    continue

                n_steps = min(len(p_pos), len(d_pos))
                if n_steps == 0:
                    continue
                coarse_diffs = p_pos[:n_steps] - d_pos[:n_steps]
                coarse_min = np.min(np.linalg.norm(coarse_diffs, axis=1))

                if coarse_min > coarse_max_dist:
                    continue

                d_satrec = None
                for tle in self._tle_entries:
                    if len(tle) == 3:
                        name, l1, l2 = tle
                    else:
                        l1, l2 = tle
                    from sgp4_orbit_parser import extract_satellite_id as _esid
                    sid = name.strip() if len(tle) == 3 else _esid(l1, l2)
                    if sid == d["satellite_id"]:
                        d_satrec = build_satrec_from_tle(tle)
                        break

                if d_satrec is not None:
                    fine_d_prop = propagate_single_j2000(d_satrec, jd_start, jd_end, fine_step_min)
                    if fine_d_prop is not None:
                        fine_d_pos = fine_d_prop["positions_km"]
                        n = min(len(p_pos), len(fine_d_pos))
                        fine_diffs = p_pos[:n] - fine_d_pos[:n]
                        fine_min = np.min(np.linalg.norm(fine_diffs, axis=1))
                        if fine_min <= CA_SCREEN_DISTANCE_KM:
                            fine_d = dict(d)
                            fine_d["positions_km"] = fine_d_prop["positions_km"]
                            fine_d["velocities_kms"] = fine_d_prop["velocities_kms"]
                            fine_d["times_jd"] = fine_d_prop["times_jd"]
                            conjunction_candidates.append(fine_d)
                    else:
                        if coarse_min <= CA_SCREEN_DISTANCE_KM:
                            conjunction_candidates.append(d)
                else:
                    if coarse_min <= CA_SCREEN_DISTANCE_KM:
                        conjunction_candidates.append(d)

        logger.info(f"CA: {len(conjunction_candidates)} conjunction candidates after fine screening")

        t0 = time.time()
        self._conjunctions = run_conjunction_assessment(
            fine_primary_objects,
            all_objects if not conjunction_candidates else conjunction_candidates,
            max_screen_distance_km=CA_SCREEN_DISTANCE_KM,
            max_conjunctions=CA_MAX_CONJUNCTIONS,
            tle_entries=self._tle_entries,
            skip_coarse_screen=bool(conjunction_candidates),
        )
        self._conjunctions_df = conjunctions_to_dataframe(self._conjunctions)
        self._ca_done = True

        elapsed = time.time() - t0
        n_red = sum(1 for c in self._conjunctions if c["alert_level"] == "RED")
        n_yellow = sum(1 for c in self._conjunctions if c["alert_level"] == "YELLOW")
        logger.info(
            f"CA complete in {elapsed:.1f}s: {len(self._conjunctions)} conjunctions "
            f"(RED={n_red}, YELLOW={n_yellow})"
        )

    def get_conjunctions(self):
        return self._conjunctions if self._conjunctions else []

    def get_conjunctions_dataframe(self):
        return self._conjunctions_df

    def get_conjunctions_at_time(self, time_index, tolerance=2):
        if not self._conjunctions:
            return []

        nearby = []
        for conj in self._conjunctions:
            if abs(conj["tca_index"] - time_index) <= tolerance:
                nearby.append(conj)
        return nearby

    def get_snapshot_at_time(self, time_index=0):
        if self._trajectory_ddf is None:
            return None

        if time_index in self._snapshot_cache:
            return self._snapshot_cache[time_index]

        t0 = time.time()
        try:
            cols = ["satellite_id", "category", "altitude_km", "x_km", "y_km", "z_km", "vx_kms", "vy_kms", "vz_kms"]
            snapshot = self._trajectory_ddf.loc[self._trajectory_ddf["time_step"] == time_index, cols].compute()
            elapsed = time.time() - t0
            logger.info(f"Snapshot at time_step={time_index}: {len(snapshot)} rows in {elapsed:.2f}s")

            if len(snapshot) < 5000:
                self._snapshot_cache[time_index] = snapshot
                if len(self._snapshot_cache) > 50:
                    oldest_key = next(iter(self._snapshot_cache))
                    del self._snapshot_cache[oldest_key]

            return snapshot
        except Exception as e:
            logger.error(f"Failed to read snapshot: {e}")
            return None

    def get_snapshot_at_time_sampled(self, time_index=0, max_points=50000, random_seed=42):
        snapshot = self.get_snapshot_at_time(time_index)
        if snapshot is None or snapshot.empty:
            return None

        if len(snapshot) > max_points:
            rng = np.random.RandomState(random_seed)
            sample_idx = rng.choice(len(snapshot), max_points, replace=False)
            snapshot = snapshot.iloc[sample_idx].reset_index(drop=True)

        return snapshot

    def get_summary_dataframe(self):
        if self._summary_df is not None:
            return self._summary_df
        if os.path.exists(self._summary_path):
            self._summary_df = dd.read_parquet(self._summary_path, engine="pyarrow").compute()
            return self._summary_df
        return None

    def get_category_counts(self):
        summary = self.get_summary_dataframe()
        if summary is None or summary.empty:
            return {"LEO": 0, "MEO": 0, "GEO": 0, "HEO": 0}
        return summary["category"].value_counts().to_dict()

    def get_trajectory_data_for_visualization(self, time_index=0, max_points=50000):
        snapshot = self.get_snapshot_at_time_sampled(time_index, max_points)
        if snapshot is None:
            return {}

        data_by_category = {}
        for category in snapshot["category"].unique():
            cat_data = snapshot[snapshot["category"] == category]
            data_by_category[category] = {
                "x": cat_data["x_km"].values.astype(np.float32),
                "y": cat_data["y_km"].values.astype(np.float32),
                "z": cat_data["z_km"].values.astype(np.float32),
                "ids": cat_data["satellite_id"].values.tolist(),
            }

        return data_by_category

    @property
    def results(self):
        return self._summary_df

    @property
    def summary(self):
        return self._summary_df

    @property
    def total_objects(self):
        return self._total_objects

    @property
    def n_time_steps(self):
        return self._n_time_steps

    @property
    def propagation_done(self):
        return self._propagation_done

    @property
    def ca_done(self):
        return self._ca_done

    @property
    def trajectory_ddf(self):
        return self._trajectory_ddf

    @property
    def parquet_dir(self):
        return self._parquet_dir
