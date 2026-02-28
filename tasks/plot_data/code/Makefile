SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/binned_scatter_crime_decile.png \
     ../output/binned_scatter_do_crime_decile.png \
     ../output/binned_scatter_pu_do_comparison.png
include ../../generic.make

../output/binned_scatter_crime_decile.png \
../output/binned_scatter_do_crime_decile.png \
../output/binned_scatter_pu_do_comparison.png &: plot_data.py \
		../input/analysis_data.parquet \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) plot_data.py

../input/analysis_data.parquet: ../../clean_data/output/analysis_data.parquet | ../input
	ln -sf $(abspath $<) $@

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
