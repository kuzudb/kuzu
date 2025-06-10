.DEFAULT_GOAL := help
# Explicit targets to avoid conflict with files of the same name.
.PHONY: requirements test

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
	$(MAKE) requirements

requirements:
	$(VENV_BIN)/python -m pip install -U uv \
	&& $(VENV_BIN)/uv pip install pytest pexpect

test: .venv
	$(VENV_BIN)/python3 -m pytest -v .
