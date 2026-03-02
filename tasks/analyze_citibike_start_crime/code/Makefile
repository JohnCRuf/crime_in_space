SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/citibike_start_comparison.png \
     ../output/citibike_start_binned_scatter.png
include ../../generic.make

../output/citibike_start_comparison.png \
../output/citibike_start_binned_scatter.png &: analyze_citibike_start_crime.py \
		../input/citibike_passthrough_analysis.parquet \
		../input/nyc_crime.csv \
		../input/taxi_zones.geojson \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) analyze_citibike_start_crime.py

../input/citibike_passthrough_analysis.parquet: ../../analyze_citibike_passthrough/output/citibike_passthrough_analysis.parquet | ../input
	ln -sf $(abspath $<) $@

../input/nyc_crime.csv: ../../download_crime_data/output/nyc_crime.csv | ../input
	ln -sf $(abspath $<) $@

../input/taxi_zones.geojson: ../../download_u2_data/output/taxi_zones.geojson | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
