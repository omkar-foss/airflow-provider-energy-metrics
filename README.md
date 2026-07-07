# ⚡ Airflow Provider Energy Metrics [![Build Status](https://github.com/omkar-foss/airflow-provider-energy-metrics/actions/workflows/run_tests.yml/badge.svg)](https://github.com/omkar-foss/airflow-provider-energy-metrics/actions/workflows/run_tests.yml)

Apache Airflow plugin that tracks the energy utilization and carbon footprints of data workflows.

It provides a Airflow Listener (`EnergyMetricsListener`) to capture energy consumption metrics of
your dag tasks instances, and a dedicated Airflow Hook (`EnergyMetricsHook`) to capture energy
consumption metrics of API calls to LLM providers like OpenAI, Anthropic and others. Refer
[examples below](#implementation-examples) for more info.

---

## Features

- CodeCarbon-based listener profiles hardware power (CPU, GPU, RAM) transparently across task
  lifecycles.
- Ecologits-based context-manager hook tracks, third-party generative AI/LLM network queries.
- Aggregates and pushes structuralized metrics payloads to native Airflow XCom variables.

## Supported Versions

Current supported and tested versions are Airflow 2.8.1 to 3.0.0.

## Installation

Install this provider either by baking this install command into your Airflow docker image:

```bash
RUN uv pip install git+https://github.com/omkar-foss/airflow-provider-energy-metrics \
    --constraints https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt
```

Replace the constraints file link above with your Airflow and Python version, format of Airflow's
constraints files is `https://raw.githubusercontent.com/apache/airflow/constraints-<YOUR_AIRFLOW_VERSION>/constraints-<YOUR_PYTHON_VERSION>.txt`.

## Verification

After installation is complete, confirm the plugin is running successfully on your instance:

1. Log in to your Airflow Web UI.
2. Navigate to the top navigation bar and select **Admin** then click **Plugins**.
3. Verify that `energy_metrics_plugin` is registered under the active plugin catalog table and shows
   its associated lifecycle listener hooks running.

Or you can list the plugins using Airflow in CLI:

```bash
airflow plugins
```

## How to Use

Upon installing, listener runs automatically on all tasks across your DAG execution
environments. You may need to restart the Airflow scheduler once though for it to load the
listener. By default, it sends the output metrics as a dict to XCom for use in downstream tasks.

Energy Metrics Listener captures the energy consumption of what's running within your task instances
on your provisioned hardware using CodeCarbon. See
[example 1](#example-1-automated-lifecycle-listener-codecarbon) below.

To explicitly capture energy metrics for remote API calls to LLM providers, use `EnergyMetricsHook`.
See [example 2](#example-2-explicit-context-hooks-ecologits--remote-llm-apis) below.

### Implementation Examples

#### Example 1: Automated Lifecycle Listener (CodeCarbon)

This approach automatically instruments your steps without altering your core processing python
execution functions or requiring parameter flags.

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# CodeCarbon listener automatically captures current runner machine core hardware metrics
# Separate config isn't required to get this running, since the listener loads automatically
with DAG(
    dag_id="sustainability_automated_listener",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    compute_task = PythonOperator(
        task_id="local_heavy_compute",
        python_callable=transform_large_dataset
    )
```

#### Example 2: Explicit Context Hooks (EcoLogits & Remote LLM APIs)

Use this method to track energy consumption of third-party generative AI/LLM provider requests.

```python
import openai
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from energy_metrics.hook import EnergyMetricsHook

def llm_processing_pipeline(**context):
    """Executes target external cloud operations safely mapped to the current task instance."""
    ti = context["ti"]

    # Open the thread-safe remote tracking window
    with EnergyMetricsHook(task_instance=ti) as energy_metrics_hook:
        print("Starting cloud model processing step...")
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Calculate the universe"}]
        )
        print(response.choices.message.content)

        # Use this function to capture and push the energy metrics to XCom
        energy_metrics_hook.push_energy_metrics_xcom(response)


with DAG(
    dag_id="sustainability_explicit_hooks",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    hybrid_task = PythonOperator(
        task_id="run_api_workload_inline",
        python_callable=llm_processing_pipeline,
    )
```

## Exported Metrics Reference (XCom)

When a tracked task completes, the plugin extracts all telemetry data from the active
tracking context and exports it to Airflow XCom metadata storage layer under explicit keys.

### Listener Metrics (`key="codecarbon_energy_metrics"`)

Automated backend outputs pushing comprehensive system telemetry dictionaries (with standard
fallbacks to standalone `emissions_kgCO2eq` targets if hardware blocks reporting):

```json
{
  "timestamp": "2026-07-07T14:19:53",
  "project_name": "codecarbon",
  "run_id": "87fc3ed9-f705-4fc2-b0df-5774d68c5511",
  "duration_secs": 1.6877790899998217,
  "emissions_kgCO2eq": 2.045025742685986e-5,
  "emissions_rate_kg_per_sec": 1.2116667132581919e-5,
  "cpu_power_watts": 77.0,
  "gpu_power_watts": 0.0,
  "ram_power_watts": 10.0,
  "cpu_energy_kwh": 2.53807784999996e-5,
  "gpu_energy_kwh": 0.0,
  "ram_energy_kwh": 3.2834802500019576e-6,
  "energy_consumed_kwh": 2.8664258750001557e-5,
  "country_name": "France",
  "country_iso_code": "FR",
  "region": "paris",
  "on_cloud": "Y",
  "cloud_provider": "azure",
  "cloud_region": "francecentral",
  "os": "Linux-5.4.0",
  "python_version": "3.12.10",
  "codecarbon_version": "3.2.8",
  "cpu_count": 4,
  "cpu_model": "Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz",
  "gpu_count": 0,
  "gpu_model": "1 x NVIDIA GeForce GTX 1080 Ti",
  "longitude": 2.3,
  "latitude": 48.6,
  "ram_total_size_gb": 8.0,
  "tracking_mode": "machine",
  "cpu_utilization_percent": 24.5,
  "gpu_utilization_percent": 2.5,
  "ram_utilization_percent": 41.2,
  "ram_used_gb": 13.18
}
```

### Hook Metrics (`key="ecologits_energy_metrics"`)

Structured payloads pushed directly via TaskInstance context abstractions:

```json
{
  "energy": {
    "type": "energy",
    "name": "Energy",
    "value": 0.004,
    "unit": "kWh"
  },
  "gwp": {
    "type": "GWP",
    "name": "Global Warming Potential",
    "value": 0.002,
    "unit": "kgCO2eq"
  },
  "adpe": {
    "type": "ADPe",
    "name": "Abiotic Depletion Potential (elements)",
    "value": 3e-8,
    "unit": "kgSbeq"
  },
  "pe": { "type": "PE", "name": "Primary Energy", "value": 0.04, "unit": "MJ" },
  "wcf": {
    "type": "WCF",
    "name": "Water Consumption Footprint",
    "value": 0.004,
    "unit": "L"
  },
  "usage": {
    "type": "usage",
    "name": "Usage",
    "energy": {
      "type": "energy",
      "name": "Energy",
      "value": 0.004,
      "unit": "kWh"
    },
    "gwp": {
      "type": "GWP",
      "name": "Global Warming Potential",
      "value": 0.0016,
      "unit": "kgCO2eq"
    },
    "adpe": {
      "type": "ADPe",
      "name": "Abiotic Depletion Potential (elements)",
      "value": 0.0,
      "unit": "kgSbeq"
    },
    "pe": {
      "type": "PE",
      "name": "Primary Energy",
      "value": 0.02,
      "unit": "MJ"
    },
    "wcf": {
      "type": "WCF",
      "name": "Water Consumption Footprint",
      "value": 0.001,
      "unit": "L"
    }
  },
  "embodied": {
    "type": "embodied",
    "name": "Embodied",
    "gwp": {
      "type": "GWP",
      "name": "Global Warming Potential",
      "value": 0.0004,
      "unit": "kgCO2eq"
    },
    "adpe": {
      "type": "ADPe",
      "name": "Abiotic Depletion Potential (elements)",
      "value": 3e-8,
      "unit": "kgSbeq"
    },
    "pe": {
      "type": "PE",
      "name": "Primary Energy",
      "value": 0.02,
      "unit": "MJ"
    }
  }
}
```

## License

This project is licensed under [Apache License 2.0](LICENSE).
