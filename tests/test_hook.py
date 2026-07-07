from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow.utils.session import provide_session
from ecologits.impacts.modeling import Impacts
from helpers import get_dagrun

from energy_metrics.hook import EnergyMetricsHook


@pytest.mark.usefixtures("airflow_test_db")
@patch("energy_metrics.hook.EcoLogits")
@provide_session
def test_hook_context_lifecycle_and_xcom_push(mock_ecologits, session=None):
    """Check if hook pushes valid metrics to xcom as expected."""

    from airflow.models import DAG, TaskInstance
    from airflow.operators.empty import EmptyOperator  # type: ignore

    mock_llm_response = MagicMock()
    impacts_payload = {
        "energy": {"value": 0.004},
        "gwp": {"value": 0.002},
        "adpe": {"value": 3e-08},
        "pe": {"value": 0.04},
        "wcf": {"value": 0.004},
        "usage": {
            "wcf": {"value": 0.001},
            "energy": {"value": 0.004},
            "gwp": {"value": 0.0016},
            "adpe": {"value": 0.0},
            "pe": {"value": 0.02},
        },
        "embodied": {
            "gwp": {"value": 0.0004},
            "adpe": {"value": 3e-08},
            "pe": {"value": 0.02},
        },
        "warnings": None,
        "errors": None,
    }
    mock_llm_response.impacts = Impacts.model_validate(impacts_payload)

    target_task_id = "test-hook-task"
    dag_id = "energy_metrics_hook_dag"
    dag = DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
    )
    empty_operator = EmptyOperator(
        task_id=target_task_id,
        dag=dag,
    )

    dagrun = get_dagrun(session, dag, empty_operator)

    task_instance = (
        session.query(TaskInstance)
        .filter_by(dag_id=dag_id, task_id=target_task_id, run_id=dagrun.run_id)
        .first()
    )
    assert task_instance is not None, "Airflow database failed to generate the TaskInstance record."

    task_instance.state = "running"
    session.commit()

    with EnergyMetricsHook(task_instance=task_instance) as hook:
        mock_ecologits.init.assert_called_once()
        assert hook.task_instance == task_instance

        # User will need to call push_energy_metrics_xcom once they're done
        # calling their respective LLM API.
        hook.push_energy_metrics_xcom(mock_llm_response)

    session.commit()
    task_instance.refresh_from_db(session=session)

    metrics = task_instance.xcom_pull(
        key="ecologits_energy_metrics",
        task_ids=target_task_id,
        session=session,
    )
    assert metrics is not None, (
        "Hook failed to push metrics payload into Airflow XCom database backend."
    )

    assert metrics == {
        "energy": {"type": "energy", "name": "Energy", "value": 0.004, "unit": "kWh"},
        "gwp": {
            "type": "GWP",
            "name": "Global Warming Potential",
            "value": 0.002,
            "unit": "kgCO2eq",
        },
        "adpe": {
            "type": "ADPe",
            "name": "Abiotic Depletion Potential (elements)",
            "value": 3e-08,
            "unit": "kgSbeq",
        },
        "pe": {"type": "PE", "name": "Primary Energy", "value": 0.04, "unit": "MJ"},
        "wcf": {"type": "WCF", "name": "Water Consumption Footprint", "value": 0.004, "unit": "L"},
        "usage": {
            "type": "usage",
            "name": "Usage",
            "energy": {"type": "energy", "name": "Energy", "value": 0.004, "unit": "kWh"},
            "gwp": {
                "type": "GWP",
                "name": "Global Warming Potential",
                "value": 0.0016,
                "unit": "kgCO2eq",
            },
            "adpe": {
                "type": "ADPe",
                "name": "Abiotic Depletion Potential (elements)",
                "value": 0.0,
                "unit": "kgSbeq",
            },
            "pe": {"type": "PE", "name": "Primary Energy", "value": 0.02, "unit": "MJ"},
            "wcf": {
                "type": "WCF",
                "name": "Water Consumption Footprint",
                "value": 0.001,
                "unit": "L",
            },
        },
        "embodied": {
            "type": "embodied",
            "name": "Embodied",
            "gwp": {
                "type": "GWP",
                "name": "Global Warming Potential",
                "value": 0.0004,
                "unit": "kgCO2eq",
            },
            "adpe": {
                "type": "ADPe",
                "name": "Abiotic Depletion Potential (elements)",
                "value": 3e-08,
                "unit": "kgSbeq",
            },
            "pe": {"type": "PE", "name": "Primary Energy", "value": 0.02, "unit": "MJ"},
        },
    }
