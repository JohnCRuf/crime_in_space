SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/chicago_crime_raw.csv ../output/chicago_crime_by_tract.csv
include ../../generic.make

../output/chicago_crime_raw.csv ../output/chicago_crime_by_tract.csv &: \
		download_chicago_crime_tract.py | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python download_chicago_crime_tract.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
