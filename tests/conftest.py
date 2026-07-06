import os
from datetime import datetime
from unittest.mock import MagicMock, PropertyMock

import pytest
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator  # type: ignore  # type: ignore
from airflow.utils import timezone
from helpers import resolve_dagrun_kwargs

from energy_metrics.listener import AIRFLOW_VERSION


# 1. Force Airflow's home directory into a temporary folder inside your test workspace
# This stops Airflow from trying to touch or create ~/airflow/ on Linux
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ["AIRFLOW_HOME"] = os.path.join(TEST_DIR, ".airflow_home")

# 2. Use a raw IN-MEMORY SQLite connection string to bypass all file-system write blocks
SQL_CONN = "sqlite:///:memory:"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = SQL_CONN
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"


@pytest.fixture(scope="session", autouse=True)
def airflow_test_db():
    """
    Spins up a 100% volatile, in-memory Airflow database.
    Bypasses Linux permission and file path blocks entirely.
    """
    # Create the isolated home directory structure explicitly
    os.makedirs(os.environ["AIRFLOW_HOME"], exist_ok=True)

    from airflow.configuration import conf
    from airflow.utils import db

    # Force programmatic binding override
    if not conf.has_section("database"):
        conf.add_section("database")
    conf.set("database", "sql_alchemy_conn", SQL_CONN)

    print("\n[CI] Bootstrapping clean in-memory Airflow metadata schemas...")
    db.initdb()

    yield

    print("\n[CI] Wiping memory space...")
    try:
        db.resetdb()
    except Exception:
        pass


@pytest.fixture
def mock_task_instance():
    """Generates a modern Airflow Task Instance with a mocked read-only 'key' property.

    Insulated to support direct task instance attribute binding and to mock out database
    mutation footprints.
    """
    from uuid import uuid4

    dag = DAG(dag_id="test_sustainability_dag", start_date=datetime(2026, 1, 1))
    task = EmptyOperator(task_id="test_compute_task", dag=dag)
    now = timezone.utcnow()
    run_id = "test_run_sustainability_123"
    session = MagicMock()

    mock_version_id = uuid4().hex

    if AIRFLOW_VERSION.major == 3 and AIRFLOW_VERSION.minor >= 1:
        from airflow.models.dagrun import DagRun

        dagrun_kwargs = resolve_dagrun_kwargs(DagRun, now, session)
        dag_run = DagRun(
            run_id=run_id,
            state="running",
            start_date=now,
            **dagrun_kwargs,
        )

        ti = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=mock_version_id)
    else:
        dagrun_kwargs = resolve_dagrun_kwargs(dag.create_dagrun, now, session)
        dag_run = dag.create_dagrun(
            run_id=run_id,
            state="running",
            start_date=now,
            **dagrun_kwargs,
        )
        ti = TaskInstance(task=task, run_id=dag_run.run_id)

    ti.dag_run = dag_run
    type(ti).key = PropertyMock(return_value="test_ti_unique_key_123")
    ti.xcom_push = MagicMock()
    ti._check_and_change_state_before_execution = MagicMock(return_value=True)
    ti.set_state = MagicMock()

    return ti
