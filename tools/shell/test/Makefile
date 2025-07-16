.DEFAULT_GOAL := help
# Explicit targets to avoid conflict with files of the same name.
.PHONY: \
	requirements historyfile \
	test test-venv \
	check format

PYTHONPATH=
SHELL=/usr/bin/env bash
VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.venv:
	python3 -m venv $(VENV)

requirements: .venv
	$(VENV_BIN)/python -m pip install -U uv \
	&& $(VENV_BIN)/uv pip install pytest pexpect ruff

# Create a history file for use in the tests. Otherwise test queries will be written to ~/.kuzu/history.txt
historyfile:
	rm -f ./history.txt && touch ./history.txt

test: historyfile
	python3 -m pytest -v .

test-venv: requirements historyfile
	$(VENV_BIN)/python3 -m pytest -v .

check: requirements
	$(VENV_BIN)/uv run ruff check

format: requirements
	$(VENV_BIN)/uv run ruff format
