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
    compute_propagation_window,
)
from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DASK_WORKERS,
    DEFAULT_DASK_THREADS_PER_WORKER,
    DEFAULT_PROPAGATION_HOURS,
    DEFAULT_TIME_STEP_MINUTES,
    DATA_DIR,
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
    ):
        t0 = time.time()
        tle_entries = parse_tle_file(tle_filepath)
        n_objects = len(tle_entries)
        logger.info(f"Parsed {n_objects} TLE entries from {tle_filepath}")

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
    def trajectory_ddf(self):
        return self._trajectory_ddf

    @property
    def parquet_dir(self):
        return self._parquet_dir
