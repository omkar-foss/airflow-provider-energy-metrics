import inspect
from uuid import uuid4

from airflow.utils import timezone
from sqlalchemy import text

from energy_metrics.listener import AIRFLOW_VERSION


def resolve_dagrun_kwargs(target_callable, execution_now, session=None):
    sig = inspect.signature(target_callable)
    kwargs = {}
    if "logical_date" in sig.parameters:
        kwargs["logical_date"] = execution_now
    elif "execution_date" in sig.parameters:
        kwargs["execution_date"] = execution_now

    if "run_after" in sig.parameters:
        kwargs["run_after"] = execution_now

    if "session" in sig.parameters:
        kwargs["session"] = session

    run_type, triggered_by = None, None
    try:
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        run_type = DagRunType.MANUAL
        triggered_by = DagRunTriggeredByType.TEST
    except (TypeError, ImportError):
        try:
            from airflow.utils.types import DagRunType

            run_type = DagRunType.MANUAL
        except ImportError:
            run_type = "manual"

    kwargs["run_type"] = run_type

    if "triggered_by" in sig.parameters:
        kwargs["triggered_by"] = triggered_by

    return kwargs


def get_dagrun(session, dag, operator):
    utc_now = timezone.utcnow()

    if AIRFLOW_VERSION.major == 3 and AIRFLOW_VERSION.minor >= 1:
        from airflow.models.dag_version import DagVersion
        from airflow.models.dagrun import DagRun

        session.execute(text("PRAGMA foreign_keys = OFF;"))

        try:
            from airflow.models.dag_bundle import DagBundleModel  # type: ignore

            bundle = session.query(DagBundleModel).filter_by(name="default_bundle").first()
            if not bundle:
                bundle = DagBundleModel(
                    name="default_bundle",
                    active=True,
                )
                session.add(bundle)
                session.commit()
        except ImportError:
            pass

        try:
            dag.sync_to_db(session=session)
            session.flush()
        except AttributeError:
            from airflow.models.dag import DagModel

            dag_model = DagModel(dag_id=dag.dag_id)

            if hasattr(dag_model, "bundle_name"):
                setattr(dag_model, "bundle_name", "default_bundle")
            if hasattr(dag_model, "is_active"):
                setattr(dag_model, "is_active", True)
            if hasattr(dag_model, "is_paused"):
                setattr(dag_model, "is_paused", False)

            session.merge(dag_model)
            session.flush()

        dag_version = session.query(DagVersion).filter_by(dag_id=dag.dag_id).first()
        if not dag_version:
            dag_version = DagVersion(
                id=uuid4().hex,
                dag_id=dag.dag_id,
                version_number=1,
                bundle_name="default_bundle",
            )
            session.add(dag_version)
            session.commit()

        dagrun_kwargs = resolve_dagrun_kwargs(DagRun, utc_now, session)
        dagrun = DagRun(
            dag_id=dag.dag_id,
            run_id="test_run_id",
            state="running",
            start_date=utc_now,
            **dagrun_kwargs,
        )
        session.add(dagrun)
        session.flush()

        from airflow.models import TaskInstance

        task_instance = TaskInstance(
            task=operator,
            run_id=dagrun.run_id,
            state="running",
            dag_version_id=dag_version.id,
        )
        setattr(task_instance, "dag_id", dag.dag_id)
        setattr(task_instance, "task_id", operator.task_id)
        session.add(task_instance)
        session.commit()
        session.execute(text("PRAGMA foreign_keys = ON;"))
    else:
        dagrun_kwargs = resolve_dagrun_kwargs(dag.create_dagrun, utc_now, session)
        dagrun = dag.create_dagrun(
            run_id="test_run_id",
            state="running",
            start_date=utc_now,
            **dagrun_kwargs,
        )

    return dagrun
