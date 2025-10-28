#!/bin/sh
set -eux

# Install test deps
pip install -r requirements-dev.txt

# Install package in editable mode so ws_de is importable
pip install -e src

# Enable integration tests flag and run the adapter unit test
export RUN_INTEGRATION=1
pytest -q tests/test_adapter.py -k resolve
