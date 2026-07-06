.ONESHELL:
SHELL := /bin/bash
.PHONY: help test test-matrix build lock clean
.DEFAULT_GOAL := help

PYTHON_VERSION ?= 3.10
AIRFLOW_VERSION ?= 3.0.6
PYTHON_VERSIONS := 3.10 3.11 3.12 3.13 3.14
AIRFLOW_VERSIONS := 2.8.1 2.9.2 2.10.1 2.11.1 3.0.6 3.1.2

define is_below_version
printf "$(1)" | awk -F. '{ \
    if ($$1 < $(2) || ($$1 == $(2) && $$2 < $(3))) \
        print "true"; \
    else \
        print "false"; \
}'
endef
IS_BELOW_2_11_0 := $(shell $(call is_below_version,$(AIRFLOW_VERSION),2,11))
EXTRA_DEPS :=
ifeq ($(IS_BELOW_2_11_0),true)
    EXTRA_DEPS := --with "flask-session==0.5.0"
endif

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
	uv sync
	uv lock

build:
	@printf "Building distribution packages...\n"
	uv sync
	uv build

test:
	@printf "Running pytest execution suite...\n"
	@export AIRFLOW_HOME="$(shell pwd)/tests/.airflow_home"
	printf "AIRFLOW_HOME set to: $$AIRFLOW_HOME\n"
	rm -rf "$$AIRFLOW_HOME"
	uv run --python "$(PYTHON_VERSION)" \
		--with "apache-airflow==$(AIRFLOW_VERSION)" $(EXTRA_DEPS) \
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

			# Airflow 2.9.x and 2.10.x fail for Python versions above 3.12
			if [[ "$$airflow" == 2.9.* ]] || [[ "$$airflow" == 2.10.* ]]; then
				if [ $$(echo "$$py > 3.12" | bc) -eq 1 ]; then
					results["Python $$py | Airflow $$airflow"]="SKIPPED"
					continue
				fi
			fi

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
			printf "$$config: $$status\n"
		elif [ "$$status" = "FAILED" ]; then
			printf "$$config: $$status\n"
		else
			printf "$$config: $$status\n"
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
	rm -rf dist/ src/*.egg-info/ .pytest_cache/ .ruff_cache/ "$$AIRFLOW_HOME"
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@printf "Purged build outputs and environment cache.\n"
