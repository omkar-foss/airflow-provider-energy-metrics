import json
import logging
import threading
from typing import Any

import airflow
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from codecarbon import EmissionsTracker
from packaging.version import Version


LOGGER = logging.getLogger(__name__)
AIRFLOW_VERSION = Version(airflow.__version__)
IS_AIRFLOW_3 = AIRFLOW_VERSION.major >= 3


def _get_str(obj: Any, key: str) -> str | None:
    val = getattr(obj, key, None)
    return str(val) if val is not None else None


def _get_float(obj: Any, key: str) -> float | None:
    val = getattr(obj, key, None)
    return float(val) if val is not None else None


def _get_int(obj: Any, key: str) -> int | None:
    val = getattr(obj, key, None)
    return int(val) if val is not None else None


class EnergyMetricsListener:
    def __init__(self):
        self._log_label = "[Energy Metrics]"
        self._active_trackers: dict = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock: threading.Lock = threading.Lock()

    def _ti_key(self, task_instance):
        return "_".join(
            [task_instance.dag_id or "", task_instance.task_id or "", task_instance.run_id or ""]
        )

    def _get_lock_for_key(self, ti_key):
        with self._locks_lock:
            lock = self._locks.get(ti_key)
            if lock is None:
                lock = threading.Lock()
                self._locks[ti_key] = lock
            return lock

    def _remove_lock_for_key(self, ti_key):
        with self._locks_lock:
            self._locks.pop(ti_key, None)

    def _set_tracker(self, task_instance, tracker):
        ti_key = self._ti_key(task_instance)
        lock = self._get_lock_for_key(ti_key)
        with lock:
            self._active_trackers[ti_key] = tracker

    def _get_tracker(self, task_instance):
        ti_key = self._ti_key(task_instance)
        lock = self._get_lock_for_key(ti_key)
        with lock:
            tracker = self._active_trackers.pop(ti_key, None)
        self._remove_lock_for_key(ti_key)
        return tracker

    def _stop_codecarbon_tracking(self, task_instance):
        tracker = self._get_tracker(task_instance)
        if not tracker:
            return

        try:
            # Halt tracker thread; yields the total estimated kg CO2eq
            total_co2_kg_returned = tracker.stop()
            data = getattr(tracker, "final_emissions_data", None)

            if data is not None:
                energy_metrics = {
                    "timestamp": _get_str(data, "timestamp"),
                    "project_name": _get_str(data, "project_name"),
                    "run_id": _get_str(data, "run_id"),
                    "duration_secs": _get_float(data, "duration"),
                    "emissions_kgCO2eq": _get_float(data, "emissions"),
                    "emissions_rate_kg_per_sec": _get_float(data, "emissions_rate"),
                    "cpu_power_watts": _get_float(data, "cpu_power"),
                    "gpu_power_watts": _get_float(data, "gpu_power"),
                    "ram_power_watts": _get_float(data, "ram_power"),
                    "cpu_energy_kwh": _get_float(data, "cpu_energy"),
                    "gpu_energy_kwh": _get_float(data, "gpu_energy"),
                    "ram_energy_kwh": _get_float(data, "ram_energy"),
                    "energy_consumed_kwh": _get_float(data, "energy_consumed"),
                    "country_name": _get_str(data, "country_name"),
                    "country_iso_code": _get_str(data, "country_iso_code"),
                    "region": _get_str(data, "region"),
                    "on_cloud": _get_str(data, "on_cloud"),
                    "cloud_provider": _get_str(data, "cloud_provider"),
                    "cloud_region": _get_str(data, "cloud_region"),
                    "os": _get_str(data, "os"),
                    "python_version": _get_str(data, "python_version"),
                    "codecarbon_version": _get_str(data, "codecarbon_version"),
                    "cpu_count": _get_int(data, "cpu_count"),
                    "cpu_model": _get_str(data, "cpu_model"),
                    "gpu_count": _get_int(data, "gpu_count"),
                    "gpu_model": _get_str(data, "gpu_model"),
                    "longitude": _get_float(data, "longitude"),
                    "latitude": _get_float(data, "latitude"),
                    "ram_total_size_gb": _get_float(data, "ram_total_size"),
                    "tracking_mode": _get_str(data, "tracking_mode"),
                    "cpu_utilization_percent": _get_float(data, "cpu_utilization_percent"),
                    "gpu_utilization_percent": _get_float(data, "gpu_utilization_percent"),
                    "ram_utilization_percent": _get_float(data, "ram_utilization_percent"),
                    "ram_used_gb": _get_float(data, "ram_used_gb"),
                }
            else:
                # Fallback path if the dataclass wrapper fails to compile on this OS profile
                fallback_co2 = (
                    float(total_co2_kg_returned or 0.0)
                    if total_co2_kg_returned is not None
                    else None
                )
                energy_metrics = {"emissions_kgCO2eq": fallback_co2}
                LOGGER.warning(
                    f"{self._log_label} final_emissions_data unavailable. Pushed fallback metric."
                )
            task_instance.xcom_push(key="codecarbon_energy_metrics", value=energy_metrics)
            LOGGER.info(
                f"{self._log_label} Metrics for Task Instance ID {task_instance.task_id}: "
                f"{json.dumps(energy_metrics, indent=2)}\n"
            )

        except Exception as e:
            LOGGER.error(
                f"{self._log_label} Failed to safely compile or push CodeCarbon metrics: {e}"
            )


class EnergyMetricsListenerAirflow2(EnergyMetricsListener):
    """Strictly targets Airflow 2.x hooks."""

    @hookimpl
    def on_task_instance_running(self, task_instance, *args, **kwargs):
        """Fires natively when an Airflow 2 task enters the running state."""
        try:
            tracker = EmissionsTracker(measure_power_secs=15, save_to_file=False)
            tracker.start()

            self._set_tracker(task_instance, tracker)

            LOGGER.info(f"{self._log_label} Profiling started for task: {task_instance.task_id}")
        except Exception as e:
            LOGGER.error(f"{self._log_label} Failed to start automated CodeCarbon profiling: {e}")

    @hookimpl
    def on_task_instance_success(self, task_instance, *args, **kwargs):
        """Fires natively on standard Airflow 2 task completion."""
        self._stop_codecarbon_tracking(task_instance)

    @hookimpl
    def on_task_instance_failed(self, task_instance, error=None, *args, **kwargs):
        """Fires natively on Airflow 2 task exceptions, safely capturing the error payload."""
        self._stop_codecarbon_tracking(task_instance)


class EnergyMetricsListenerAirflow3(EnergyMetricsListener):
    """Strictly targets Airflow 2.x hooks."""

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, **kwargs):
        """Starts and inializes CodeCarbon tracker on task instance."""
        try:
            tracker = EmissionsTracker(measure_power_secs=15, save_to_file=False)
            tracker.start()

            self._set_tracker(task_instance, tracker)

            LOGGER.info(f"{self._log_label} Profiling started for task: {task_instance.task_id}")
        except Exception as e:
            LOGGER.error(f"{self._log_label} Failed to start automated CodeCarbon profiling: {e}")

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, **kwargs):
        """Stops CodeCarbon tracking on task instance success."""

        self._stop_codecarbon_tracking(task_instance)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error, **kwargs):
        """Stops CodeCarbon tracking on task instance failure."""

        self._stop_codecarbon_tracking(task_instance)


class EnergyMetricsPlugin(AirflowPlugin):
    """Airflow plugin class to capture energy metrics of a dag using CodeCarbon."""

    name = "energy_metrics_plugin"
    listeners = [
        EnergyMetricsListenerAirflow3() if IS_AIRFLOW_3 else EnergyMetricsListenerAirflow2()
    ]
