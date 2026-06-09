import time
import logging
import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed, compute
from dask.distributed import Client, LocalCluster

from sgp4_orbit_parser import parse_tle_file, propagate_chunk, compute_propagation_window
from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DASK_WORKERS,
    DEFAULT_DASK_THREADS_PER_WORKER,
    DEFAULT_PROPAGATION_HOURS,
    DEFAULT_TIME_STEP_MINUTES,
)

logger = logging.getLogger(__name__)


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
        self._propagation_results = None
        self._summary_df = None

    def start_cluster(self):
        logger.info(f"Starting Dask LocalCluster: workers={self.n_workers}, threads/worker={self.threads_per_worker}")
        self.cluster = LocalCluster(
            n_workers=self.n_workers,
            threads_per_worker=self.threads_per_worker,
            processes=True,
            silence_logs=logging.WARNING,
            memory_limit="2GB",
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

    def _split_into_chunks(self, tle_entries):
        chunks = []
        for i in range(0, len(tle_entries), self.chunk_size):
            chunks.append(tle_entries[i : i + self.chunk_size])
        logger.info(f"Split {len(tle_entries)} TLE entries into {len(chunks)} chunks (chunk_size={self.chunk_size})")
        return chunks

    def submit_propagation(
        self,
        tle_filepath,
        hours=DEFAULT_PROPAGATION_HOURS,
        step_minutes=DEFAULT_TIME_STEP_MINUTES,
    ):
        if self.client is None:
            self.start_cluster()

        t0 = time.time()
        tle_entries = parse_tle_file(tle_filepath)
        logger.info(f"Parsed {len(tle_entries)} TLE entries from {tle_filepath}")

        chunks = self._split_into_chunks(tle_entries)
        jd_start, jd_end = compute_propagation_window(hours)

        logger.info(f"Propagation window: JD {jd_start:.4f} -> JD {jd_end:.4f} ({hours}h, step={step_minutes}min)")
        logger.info(f"Submitting {len(chunks)} delayed tasks to Dask...")

        delayed_results = []
        for chunk_idx, chunk in enumerate(chunks):
            task = delayed(propagate_chunk)(chunk, jd_start, jd_end, step_minutes)
            delayed_results.append(task)

        all_results = compute(*delayed_results, scheduler="distributed")

        elapsed = time.time() - t0
        logger.info(f"Propagation complete in {elapsed:.1f}s")

        flat_results = []
        for chunk_result in all_results:
            if chunk_result:
                flat_results.extend(chunk_result)

        self._propagation_results = flat_results
        self._summary_df = self._build_summary_dataframe(flat_results)

        return flat_results

    def submit_propagation_local(
        self,
        tle_filepath,
        hours=DEFAULT_PROPAGATION_HOURS,
        step_minutes=DEFAULT_TIME_STEP_MINUTES,
    ):
        t0 = time.time()
        tle_entries = parse_tle_file(tle_filepath)
        logger.info(f"Parsed {len(tle_entries)} TLE entries from {tle_filepath}")

        chunks = self._split_into_chunks(tle_entries)
        jd_start, jd_end = compute_propagation_window(hours)

        logger.info(f"Propagation window: JD {jd_start:.4f} -> JD {jd_end:.4f} ({hours}h)")
        logger.info(f"Computing {len(chunks)} chunks locally with Dask threaded scheduler...")

        delayed_results = []
        for chunk in chunks:
            task = delayed(propagate_chunk)(chunk, jd_start, jd_end, step_minutes)
            delayed_results.append(task)

        all_results = compute(*delayed_results, scheduler="threads")

        elapsed = time.time() - t0
        logger.info(f"Local propagation complete in {elapsed:.1f}s")

        flat_results = []
        for chunk_result in all_results:
            if chunk_result:
                flat_results.extend(chunk_result)

        self._propagation_results = flat_results
        self._summary_df = self._build_summary_dataframe(flat_results)

        return flat_results

    def _build_summary_dataframe(self, results):
        rows = []
        for r in results:
            pos = r["positions_km"]
            rows.append(
                {
                    "satellite_id": r["satellite_id"],
                    "category": r["category"],
                    "altitude_km": r["altitude_km"],
                    "n_trajectory_points": len(pos),
                    "pos_x_initial_km": pos[0, 0] if len(pos) > 0 else np.nan,
                    "pos_y_initial_km": pos[0, 1] if len(pos) > 0 else np.nan,
                    "pos_z_initial_km": pos[0, 2] if len(pos) > 0 else np.nan,
                }
            )
        return pd.DataFrame(rows)

    def get_summary_dataframe(self):
        return self._summary_df

    def build_xarray_dataset(self):
        if not self._propagation_results:
            return None

        all_ids = []
        all_cats = []
        max_len = 0
        for r in self._propagation_results:
            all_ids.append(r["satellite_id"])
            all_cats.append(r["category"])
            max_len = max(max_len, len(r["positions_km"]))

        n_sats = len(all_ids)
        n_times = max_len

        pos_x = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        pos_y = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        pos_z = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        vel_x = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        vel_y = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        vel_z = np.full((n_sats, n_times), np.nan, dtype=np.float32)
        times_jd = np.full((n_sats, n_times), np.nan, dtype=np.float64)

        for i, r in enumerate(self._propagation_results):
            n = len(r["positions_km"])
            pos_x[i, :n] = r["positions_km"][:, 0]
            pos_y[i, :n] = r["positions_km"][:, 1]
            pos_z[i, :n] = r["positions_km"][:, 2]
            vel_x[i, :n] = r["velocities_kms"][:, 0]
            vel_y[i, :n] = r["velocities_kms"][:, 1]
            vel_z[i, :n] = r["velocities_kms"][:, 2]
            times_jd[i, :n] = r["times_jd"]

        ds = xr.Dataset(
            {
                "pos_x": (["object", "time_step"], pos_x),
                "pos_y": (["object", "time_step"], pos_y),
                "pos_z": (["object", "time_step"], pos_z),
                "vel_x": (["object", "time_step"], vel_x),
                "vel_y": (["object", "time_step"], vel_y),
                "vel_z": (["object", "time_step"], vel_z),
                "time_jd": (["object", "time_step"], times_jd),
            },
            coords={
                "object": np.arange(n_sats),
                "time_step": np.arange(n_times),
            },
            attrs={
                "satellite_ids": all_ids,
                "categories": all_cats,
                "description": "Space debris J2000 trajectory dataset",
                "propagation_hours": DEFAULT_PROPAGATION_HOURS,
            },
        )

        return ds

    def get_trajectory_data_for_visualization(self):
        if not self._propagation_results:
            return {}

        data_by_category = {}
        for r in self._propagation_results:
            cat = r["category"]
            if cat not in data_by_category:
                data_by_category[cat] = {"x": [], "y": [], "z": [], "ids": []}

            pos = r["positions_km"]
            data_by_category[cat]["x"].extend(pos[:, 0].tolist())
            data_by_category[cat]["y"].extend(pos[:, 1].tolist())
            data_by_category[cat]["z"].extend(pos[:, 2].tolist())
            data_by_category[cat]["ids"].extend([r["satellite_id"]] * len(pos))

        for cat in data_by_category:
            data_by_category[cat]["x"] = np.array(data_by_category[cat]["x"], dtype=np.float32)
            data_by_category[cat]["y"] = np.array(data_by_category[cat]["y"], dtype=np.float32)
            data_by_category[cat]["z"] = np.array(data_by_category[cat]["z"], dtype=np.float32)

        return data_by_category

    def get_snapshot_at_time(self, time_index=0):
        if not self._propagation_results:
            return None

        rows = []
        for r in self._propagation_results:
            pos = r["positions_km"]
            vel = r["velocities_kms"]
            idx = min(time_index, len(pos) - 1)
            if idx >= 0:
                rows.append(
                    {
                        "satellite_id": r["satellite_id"],
                        "category": r["category"],
                        "altitude_km": r["altitude_km"],
                        "x_km": pos[idx, 0],
                        "y_km": pos[idx, 1],
                        "z_km": pos[idx, 2],
                        "vx_kms": vel[idx, 0],
                        "vy_kms": vel[idx, 1],
                        "vz_kms": vel[idx, 2],
                    }
                )

        return pd.DataFrame(rows)

    @property
    def results(self):
        return self._propagation_results

    @property
    def summary(self):
        return self._summary_df
