# Airflow Provider Energy Metrics

An Apache Airflow plugin that tracks the energy utilization and carbon footprints of data workflows.

It includes an Airflow Listener (`EnergyMetricsListener`) to monitor energy consumption of your
tasks, and a dedicated Airflow Hook (`EnergyMetricsHook`) to monitor energy consumption of API
calls to LLM providers like OpenAI, Anthropic and others. Refer
[examples below](#implementation-examples) for more info.

---

## Features

- CodeCarbon event listener profiles hardware power (CPU, GPU, RAM) transparently across task
  lifecycles.
- Ecologits-based Context-manager hook isolates, tracks, and safely unpatches third-party generative
  AI/LLM network queries.
- Aggregates and pushes structuralized telemetry datasets into native Airflow XCom variables.
- Built to capture execution parameters cleanly across Airflow versions 2.8 to 3.1.

## Installation

Install the package directly inside your Airflow environment or append it to your project
requirements file:

```bash
pip install git+https://github.com/omkar-foss/airflow-provider-energy-metrics
```

## Verification

After installation is complete, confirm the plugin is running successfully on your instance:

1. Log in to your Airflow Web UI.
2. Navigate to the top navigation bar and select **Admin** then click **Plugins**.
3. Verify that `energy_metrics_plugin` is registered under the active plugin catalog table and shows
   its associated lifecycle listener hooks running.

## How to Use

Upon installing, **listener runs automatically** on all tasks across your DAG execution
environments . You may need to restart the Airflow scheduler once though for it to load the
listener. By default, it sends the output metrics as a dict to XCom for use in downstream tasks.

Energy Metrics Listener captures the energy consumption of what's running within your task instances
on your provisioned hardware using CodeCarbon. See
[example 1](#example-1-automated-lifecycle-listener-codecarbon) below.

To explicitly capture energy metrics for remote API calls to LLM providers, use `EnergyMetricsHook`.
See [example 2](#example-2-explicit-context-hooks-ecologits--remote-apis) below.

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

#### Example 2: Explicit Context Hooks (EcoLogits & Remote APIs)

Use this method to safely isolate, log, and un-patch monkey-wrapped execution scopes for external
network calls running within an operator.

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
    with EnergyMetricsHook(task_instance=ti):
        print("Starting cloud model processing step...")
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Calculate the universe"}]
        )
        print(response.choices.message.content)

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

When a tracked task completes execution, the plugin extracts all telemetry data from the active
tracking context and exports it to the Airflow XCom metadata storage layer under explicit keys.

### Listener Metrics (`key="codecarbon_energy_metrics"`)

Automated backend outputs pushing comprehensive system telemetry dictionaries (with standard
fallbacks to standalone `emissions_kgCO2eq` targets if hardware blocks reporting):

```json
{
  "timestamp": "2026-06-18T21:59:00",
  "project_name": "codecarbon",
  "run_id": "uuid-string",
  "duration_secs": 45.2,
  "emissions_kgCO2eq": 0.00123,
  "emissions_rate_kg_per_sec": 0.000027,
  "cpu_power_watts": 45.0,
  "gpu_power_watts": 0.0,
  "ram_power_watts": 12.0,
  "cpu_energy_kwh": 0.00056,
  "gpu_energy_kwh": 0.0,
  "ram_energy_kwh": 0.00015,
  "energy_consumed_kwh": 0.00071,
  "country_name": "United States",
  "country_iso_code": "USA",
  "region": "eastus",
  "on_cloud": "Y",
  "cloud_provider": "azure",
  "cloud_region": "eastus",
  "os": "Linux-5.4.0",
  "python_version": "3.11.5",
  "codecarbon_version": "2.8.0",
  "cpu_count": 8,
  "cpu_model": "Intel Xeon",
  "gpu_count": 0,
  "gpu_model": null,
  "longitude": -77.0369,
  "latitude": 38.8951,
  "ram_total_size_gb": 32.0,
  "tracking_mode": "machine",
  "cpu_utilization_percent": 24.5,
  "gpu_utilization_percent": 0.0,
  "ram_utilization_percent": 41.2,
  "ram_used_gb": 13.18
}
```

### Hook Metrics (`key="ecologits_energy_metrics"`)

Structured payloads pushed directly via TaskInstance context abstractions:

```json
{
  "remote_api_co2_kg": 0.000412,
  "remote_api_energy_wh": 0.85,
  "remote_api_input_tokens": 1250,
  "remote_api_output_tokens": 420
}
```
