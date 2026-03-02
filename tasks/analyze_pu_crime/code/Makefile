SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/pu_crime_comparison.png \
     ../output/pu_crime_binned_scatter.png
include ../../generic.make

../output/pu_crime_comparison.png \
../output/pu_crime_binned_scatter.png &: analyze_pu_crime.py \
		../input/analysis_data.parquet \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) analyze_pu_crime.py

../input/analysis_data.parquet: ../../clean_data/output/analysis_data.parquet | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
