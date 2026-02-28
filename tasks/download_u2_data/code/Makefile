SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/hvfhv_trips.parquet ../output/taxi_zone_lookup.csv ../output/taxi_zones.geojson
include ../../generic.make

../output/hvfhv_trips.parquet ../output/taxi_zone_lookup.csv ../output/taxi_zones.geojson &: \
		download_u2.py | $(PYTHON_OOPR) ../output
	$(PYTHON) download_u2.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
