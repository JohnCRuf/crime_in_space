SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/ctpp_chicago_all_modes_matrix.csv ../output/ctpp_chicago_drive_matrix.csv ../output/ctpp_chicago_transit_matrix.csv
include ../../generic.make

../output/ctpp_chicago_all_modes_matrix.csv ../output/ctpp_chicago_drive_matrix.csv ../output/ctpp_chicago_transit_matrix.csv &: \
		download_ctpp_chicago.py | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python download_ctpp_chicago.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
