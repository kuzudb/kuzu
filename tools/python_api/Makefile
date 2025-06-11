.DEFAULT_GOAL := help
# Explicit targets to avoid conflict with files of the same name.
.PHONY: \
	requirements \
	lint check format \
	build test \
	help

PYTHONPATH=
SHELL=/usr/bin/env bash
VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.venv:  ## Set up a Python virtual environment and install dev packages
	python3 -m venv $(VENV)

requirements: .venv ## Install/update Python dev packages
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install -U uv \
	&& $(VENV_BIN)/uv pip install --upgrade -r requirements_dev.txt

pytest: requirements
ifeq ($(OS),Windows_NT)
	set PYTHONPATH=./build
else
	export PYTHONPATH=./build
endif
	$(VENV_BIN)/python -m pytest -vv ./test

lint: requirements  ## Apply autoformatting and linting rules
	$(VENV_BIN)/ruff check src_py test
	$(VENV_BIN)/ruff format src_py test
	-$(VENV_BIN)/mypy src_py test

check: requirements
	$(VENV_BIN)/ruff check src_py test --verbose

format: requirements
	$(VENV_BIN)/ruff format src_py test

build:  ## Compile kuzu (and install in 'build') for Python
	$(MAKE) -C ../../ python
	cp src_py/*.py build/kuzu/

test: requirements  ## Run the Python unit tests
	cp src_py/*.py build/kuzu/ && cd build
	$(VENV_BIN)/pytest test

help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
