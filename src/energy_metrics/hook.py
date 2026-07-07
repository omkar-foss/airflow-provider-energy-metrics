import json
import logging

from airflow.hooks.base import BaseHook
from ecologits import EcoLogits
from ecologits.impacts.modeling import Impacts


LOGGER = logging.getLogger(__name__)


class EnergyMetricsHook(BaseHook):
    """Airflow hook to capture energy metrics of remote LLM APIs
    using Ecologits."""

    def __init__(self, task_instance):
        super().__init__()
        self.task_instance = task_instance
        self._log_label = "[Energy Metrics]"

    def __enter__(self):
        """Entry handler for context window. Initializes EcoLogits."""

        LOGGER.info(f"{self._log_label} Starting energy consumption monitoring with EcoLogits.")
        try:
            EcoLogits.init()
        except Exception as e:
            LOGGER.error(f"Failed to initialize localized EcoLogits tracking: {e}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit handler for context window."""

        LOGGER.info(f"{self._log_label} EcoLogits hook exiting.")
        return False

    def push_energy_metrics_xcom(self, llm_api_response):
        """Extracts all tracked remote execution impacts and pushes to xcom."""
        try:
            if not llm_api_response:
                raise ValueError(
                    f"{self._log_label} Response is empty, so cannot get energy impact metrics."
                )

            impacts: Impacts = llm_api_response.impacts
            if not impacts:
                raise ValueError(
                    f"{self._log_label} Ecologits impacts payload not found in response."
                )
            energy_metrics = (
                impacts.model_dump() if hasattr(impacts, "model_dump") else impacts.dict()
            )
            self.task_instance.xcom_push(key="ecologits_energy_metrics", value=energy_metrics)
            LOGGER.info(
                f"{self._log_label} Metrics for Task Instance ID {self.task_instance.task_id}: "
                f"{json.dumps(energy_metrics, indent=2)}\n"
            )

        except Exception as e:
            LOGGER.error(
                f"{self._log_label} Failed to safely compile or push Ecologits metrics: {e}"
            )
