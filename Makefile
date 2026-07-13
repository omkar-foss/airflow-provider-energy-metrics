.ONESHELL:
SHELL := /bin/bash
.PHONY: help test test-matrix build lock clean
.DEFAULT_GOAL := help

PYTHON_VERSION ?= 3.10
AIRFLOW_VERSION ?= 3.1.6
PYTHON_VERSIONS := 3.10 3.11 3.12 3.13
AIRFLOW_VERSIONS := 2.8.2 2.9.0 2.9.3 2.10.1 2.10.5 2.11.0 3.0.0 3.0.6 3.1.2 3.1.6

define is_below_version
printf "$(1)" | awk -F. '{ \
    if ($$1 < $(2) || ($$1 == $(2) && $$2 < $(3))) \
        print "true"; \
    else \
        print "false"; \
}'
endef
IS_BELOW_2_11_0 := $(shell $(call is_below_version,$(AIRFLOW_VERSION),2,11))

help:
	@printf "Available commands:\n"
	@printf "  make lock         Lock dependencies and update uv.lock\n"
	@printf "  make build        Build package with synced virtual environment\n"
	@printf "  make test         Run pytest suite\n"
	@printf "  make test-matrix  Run tests for different python-airflow version combos\n"
	@printf "  make clean        Remove build artifacts and temporary cache\n"

format:
	uv run ruff format .
	uv run ruff check . --fix

lock:
	@printf "Locking dependency updates...\n"
	rm -rf .venv
	uv venv .venv
	uv sync
	uv lock

build:
	@printf "Building distribution packages...\n"
	uv sync
	uv build

test:
	@printf "Running pytest execution suite with Airflow $(AIRFLOW_VERSION) and Python $(PYTHON_VERSION)...\n"
	@export AIRFLOW_HOME="$(shell pwd)/tests/.airflow_home"
	printf "AIRFLOW_HOME set to: $$AIRFLOW_HOME\n"
	rm -rf "$$AIRFLOW_HOME"
	rm -rf .venv
	uv venv --python "$(PYTHON_VERSION)" .venv
	uv pip install -e . \
		--python "$(PYTHON_VERSION)" \
		--override <(echo "apache-airflow==$(AIRFLOW_VERSION)") \
		--constraints "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt" || exit 1
	uv run --python "$(PYTHON_VERSION)" \
		--with "apache-airflow==$(AIRFLOW_VERSION)" \
		pytest -v tests/

test-matrix:
	@printf "Starting Python with Airflow matrix tests suite\n"
	failed=0
	declare -A results
	for py in $(PYTHON_VERSIONS); do
		for airflow in $(AIRFLOW_VERSIONS); do
			# Airflow 2.8.x fails for Python versions above 3.11
			if [[ "$$airflow" == 2.8.* ]]; then
				if [ $$(echo "$$py > 3.11" | bc) -eq 1 ]; then
					results["Python $$py | Airflow $$airflow"]="SKIPPED"
					continue
				fi
			fi

			# Only Airflow 3.1.x supports Python versions above 3.12
			if [ $$(echo "$$py > 3.12" | bc) -eq 1 ]; then
				if [[ "$$airflow" != 3.1.* ]]; then
					results["Python $$py | Airflow $$airflow"]="SKIPPED";
					continue;
				fi;
			fi;

			printf "Testing: Python $$py | Airflow $$airflow\n"
			if $(MAKE) test PYTHON_VERSION=$$py AIRFLOW_VERSION=$$airflow; then
				results["Python $$py | Airflow $$airflow"]="PASSED"
			else
				results["Python $$py | Airflow $$airflow"]="FAILED"
				failed=1
			fi
		done
	done

	printf "\n==================================================\n"
	printf "               MATRIX TEST REPORT                 \n"
	printf "==================================================\n"

	while read -r config; do
		# Skip empty iterations
		[ -z "$$config" ] && continue

		status="$${results[$$config]}"
		if [ "$$status" = "PASSED" ]; then
			printf "$$config: ✅\n"
		elif [ "$$status" = "FAILED" ]; then
			printf "$$config: ❌\n"
		else
			printf "$$config: ➖\n"
		fi
	done < <(printf '%s\n' "$${!results[@]}" | sort -V)

	printf "==================================================\n"

	if [ $$failed -eq 1 ]; then
		printf "Matrix test run failed. One or more configs threw errors.\n"
		exit 1
	else
		printf "All compatible Matrix test configs passed successfully.\n"
	fi


clean:
	@export AIRFLOW_HOME="$(shell pwd)/tests/.airflow_home"
	rm -rf .venv/ dist/ src/*.egg-info/ .pytest_cache/ .ruff_cache/ "$$AIRFLOW_HOME"
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@printf "Purged build outputs and environment cache.\n"
