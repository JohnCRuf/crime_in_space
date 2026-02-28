SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/analysis_data.parquet
include ../../generic.make

../output/analysis_data.parquet: clean_data.py \
		../input/trips_with_routes.parquet \
		../input/nyc_crime.csv \
		../input/taxi_zones.geojson \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) clean_data.py

../input/trips_with_routes.parquet: ../../get_routes/output/trips_with_routes.parquet | ../input
	ln -sf $(abspath $<) $@

../input/nyc_crime.csv: ../../download_crime_data/output/nyc_crime.csv | ../input
	ln -sf $(abspath $<) $@

../input/taxi_zones.geojson: ../../download_u2_data/output/taxi_zones.geojson | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
