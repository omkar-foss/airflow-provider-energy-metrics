def get_provider_info():
    return {
        "package-name": "airflow-provider-energy-metrics",
        "name": "Energy Metrics Provider",
        "description": "Captures and reports DAG energy consumption metrics.",
        "versions": ["0.1.0"],
        "connection-types": [
            {
                "connection-type": "energy_metrics",
                "hook-class-name": "energy_metrics.hook.EnergyMetricsHook",
            }
        ],
        "plugins": [
            {
                "name": "energy_metrics_plugin",
                "plugin-class": "energy_metrics.listener.EnergyMetricsPlugin",
            }
        ],
    }
