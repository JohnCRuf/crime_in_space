SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/citibike_trips.parquet
include ../../generic.make

../output/citibike_trips.parquet: download_citibike.py \
		| $(PYTHON_OOPR) ../output
	$(PYTHON) download_citibike.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
