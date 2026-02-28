SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/trips_with_routes.parquet
include ../../generic.make

../output/trips_with_routes.parquet: get_routes.py \
		../input/hvfhv_trips.parquet ../input/taxi_zones.geojson \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) get_routes.py

../input/hvfhv_trips.parquet: ../../download_u2_data/output/hvfhv_trips.parquet | ../input
	ln -sf $(abspath $<) $@

../input/taxi_zones.geojson: ../../download_u2_data/output/taxi_zones.geojson | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
