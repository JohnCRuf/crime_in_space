SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml

all: ../output/strava_metro_download_manifest.csv
include ../../generic.make

../output/strava_metro_download_manifest.csv: download_chicago_strava.py | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python download_chicago_strava.py

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
