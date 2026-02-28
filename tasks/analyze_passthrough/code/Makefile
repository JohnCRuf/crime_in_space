SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/passthrough_comparison.png
include ../../generic.make

../output/passthrough_comparison.png: analyze_passthrough.py \
		../input/analysis_data.parquet \
		../input/taxi_zones.geojson \
		../input/nyc_crime.csv \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) analyze_passthrough.py

../input/analysis_data.parquet: ../../clean_data/output/analysis_data.parquet | ../input
	ln -sf $(abspath $<) $@

../input/taxi_zones.geojson: ../../download_u2_data/output/taxi_zones.geojson | ../input
	ln -sf $(abspath $<) $@

../input/nyc_crime.csv: ../../download_crime_data/output/nyc_crime.csv | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
