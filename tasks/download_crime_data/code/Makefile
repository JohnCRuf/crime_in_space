SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/nyc_crime.csv
include ../../generic.make

../output/nyc_crime.csv: download_crime.py | $(PYTHON_OOPR) ../output
	$(PYTHON) download_crime.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
