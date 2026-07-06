import inspect
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState
from helpers import get_dagrun

from energy_metrics.listener import EnergyMetricsListener, EnergyMetricsPlugin


@patch("energy_metrics.listener.EmissionsTracker")
def test_listener_automatically_initializes_codecarbon_on_all_tasks(
    mock_codecarbon_tracker, mock_task_instance
):
    """Verifies that any task running on the worker automatically
    instantiates and activates CodeCarbon without requiring parameters.
    """

    listener = EnergyMetricsPlugin().listeners[0]
    listener.on_task_instance_running(
        previous_state=TaskInstanceState.QUEUED,
        task_instance=mock_task_instance,
    )

    assert listener._get_tracker(mock_task_instance) is not None
    mock_codecarbon_tracker.return_value.start.assert_called_once()


def test_stop_tracking_bypasses_safely_if_no_tracker_is_bound_to_task(
    mock_task_instance,
):
    """Ensures that if _stop_codecarbon_tracking runs against an untracked or
    skipped task context, it returns cleanly without side effects.
    """
    listener = EnergyMetricsPlugin().listeners[0]
    listener._stop_codecarbon_tracking(mock_task_instance)
    mock_task_instance.xcom_push.assert_not_called()


@pytest.mark.usefixtures("airflow_test_db")
@patch("energy_metrics.listener.EmissionsTracker")
@provide_session
def test_stop_tracking_pushes_valid_metrics_xcom(mock_codecarbon_tracker, session=None):
    """Check if listener pushes valid energy metrics data to xcom."""

    from airflow.models import DAG, TaskInstance
    from airflow.operators.empty import EmptyOperator  # type: ignore

    mock_codecarbon_tracker.stop.return_value = 0.042157
    time_now = timezone.utcnow()
    mock_final_data = MagicMock()
    mock_final_data.timestamp = time_now
    mock_final_data.project_name = "airflow"
    mock_final_data.run_id = "run-123"
    mock_final_data.duration = 45.0
    mock_final_data.emissions = 0.042157
    mock_final_data.emissions_rate = 345.2
    mock_final_data.cpu_power = 65.0
    mock_final_data.gpu_power = 0.0
    mock_final_data.ram_power = 16.0
    mock_final_data.cpu_energy = 0.075
    mock_final_data.gpu_energy = 0.000
    mock_final_data.ram_energy = 0.050
    mock_final_data.energy_consumed = 0.125
    mock_final_data.country_name = "United States"
    mock_final_data.country_iso_code = "USA"
    mock_final_data.region = "virginia"
    mock_final_data.on_cloud = "Y"
    mock_final_data.cloud_provider = "aws"
    mock_final_data.cloud_region = "us-east-1"
    mock_final_data.os = "Windows-10-10.0.19044-SP0"
    mock_final_data.python_version = "3.10.12"
    mock_final_data.codecarbon_version = "2.3.0"
    mock_final_data.cpu_count = 4
    mock_final_data.cpu_model = "Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz"
    mock_final_data.gpu_count = 2
    mock_final_data.gpu_model = "1 x NVIDIA GeForce GTX 1080 Ti"
    mock_final_data.longitude = 3.456
    mock_final_data.latitude = 1.234
    mock_final_data.ram_total_size = 32.0
    mock_final_data.tracking_mode = "machine"
    mock_final_data.cpu_utilization_percent = 20.5
    mock_final_data.gpu_utilization_percent = 0
    mock_final_data.ram_utilization_percent = 45.6
    mock_final_data.ram_used_gb = 4.2
    mock_codecarbon_tracker.final_emissions_data = mock_final_data
    mock_codecarbon_tracker.return_value.final_emissions_data = mock_final_data

    target_task_id = "test-task"
    dag_id = "energy_metrics_dag"
    dag = DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
    )
    empty_operator = EmptyOperator(
        task_id=target_task_id,
        dag=dag,
    )
    plugin = EnergyMetricsPlugin()
    listener = plugin.listeners[0]

    dagrun = get_dagrun(session, dag, empty_operator)

    task_instance = (
        session.query(TaskInstance)
        .filter_by(dag_id=dag_id, task_id=target_task_id, run_id=dagrun.run_id)
        .first()
    )
    assert task_instance is not None, (
        "Airflow failed to automatically generate the TaskInstance row stub."
    )

    task_instance.state = "success"
    session.commit()
    mock_tracker_instance = mock_codecarbon_tracker.return_value
    listener._set_tracker(task_instance, mock_tracker_instance)
    context = {
        "dag": dag,
        "task": empty_operator,
        "task_instance": task_instance,
        "ti": task_instance,
        "session": session,
    }
    on_task_instance_success_method = getattr(listener, "on_task_instance_success")
    sig = inspect.signature(on_task_instance_success_method)
    if "previous_state" in sig.parameters:
        try:
            prev_state = TaskInstanceState.QUEUED
        except ImportError:
            prev_state = "queued"
        on_task_instance_success_method(
            previous_state=prev_state, task_instance=task_instance, session=session
        )
    else:
        on_task_instance_success_method(task_instance, context=context, session=session)

    session.commit()
    task_instance.refresh_from_db(session=session)
    metrics = task_instance.xcom_pull(
        key="codecarbon_energy_metrics",
        task_ids=target_task_id,
        session=session,
    )
    assert metrics is not None, "Listener failed to trigger or drop XCom metric payloads."
    assert metrics["timestamp"] == str(time_now)
    assert metrics["project_name"] == "airflow"
    assert metrics["run_id"] == "run-123"
    assert metrics["duration_secs"] > 0  # duration_secs updates after stopping tracker
    assert metrics["emissions_kgCO2eq"] == 0.042157
    assert metrics["emissions_rate_kg_per_sec"] == 345.2
    assert metrics["cpu_power_watts"] == 65.0
    assert metrics["gpu_power_watts"] == 0.0
    assert metrics["ram_power_watts"] == 16.0
    assert metrics["cpu_energy_kwh"] == 0.075
    assert metrics["gpu_energy_kwh"] == 0.000
    assert metrics["ram_energy_kwh"] == 0.050
    assert metrics["energy_consumed_kwh"] == 0.125
    assert metrics["country_name"] == "United States"
    assert metrics["country_iso_code"] == "USA"
    assert metrics["region"] == "virginia"
    assert metrics["on_cloud"] == "Y"
    assert metrics["cloud_provider"] == "aws"
    assert metrics["cloud_region"] == "us-east-1"
    assert metrics["os"] == "Windows-10-10.0.19044-SP0"
    assert metrics["python_version"] == "3.10.12"
    assert metrics["codecarbon_version"] == "2.3.0"
    assert metrics["cpu_count"] == 4
    assert metrics["cpu_model"] == "Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz"
    assert metrics["gpu_count"] == 2
    assert metrics["gpu_model"] == "1 x NVIDIA GeForce GTX 1080 Ti"
    assert metrics["latitude"] == 1.234
    assert metrics["longitude"] == 3.456
    assert metrics["ram_total_size_gb"] == 32.0
    assert metrics["tracking_mode"] == "machine"
    assert metrics["cpu_utilization_percent"] == 20.5
    assert metrics["gpu_utilization_percent"] == 0
    assert metrics["ram_utilization_percent"] == 45.6
    assert metrics["ram_used_gb"] == 4.2


def test_plugin_metadata_and_registration():
    """Validates that the custom plugin registers with the correct name
    and hooks the listener module."""

    plugin_instance = EnergyMetricsPlugin()
    assert isinstance(plugin_instance, AirflowPlugin)
    assert plugin_instance.name == "energy_metrics_plugin"
    assert len(plugin_instance.listeners) == 1
    assert isinstance(plugin_instance.listeners[0], EnergyMetricsListener)


def test_plugin_entry_point_discovery():
    """Simulates Airflow's entry_points setup to ensure the class is
    discoverable dynamically on start."""

    import importlib.metadata

    discovered_plugins = importlib.metadata.entry_points(group="airflow.plugins")
    plugin_entry = next(
        (ep for ep in discovered_plugins if ep.name == "energy_metrics_plugin"), None
    )
    if plugin_entry:
        loaded_plugin_class = plugin_entry.load()
        assert loaded_plugin_class == EnergyMetricsPlugin
