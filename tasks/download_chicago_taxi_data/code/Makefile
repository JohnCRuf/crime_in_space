SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/chicago_taxi_trips.csv
include ../../generic.make

../output/chicago_taxi_trips.csv: download_chicago_taxi.py | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python download_chicago_taxi.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
