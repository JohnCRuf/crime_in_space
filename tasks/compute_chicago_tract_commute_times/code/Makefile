SHELL=bash
include ../../shell_functions.make

PYTHON_OOPR = ../output/pyproject.toml
SHARDS ?= 8
MODE ?= driving
ORIGIN_BATCH_SIZE ?= 20
DESTINATION_BATCH_SIZE ?= 80

SHARD_FILES = $(shell for i in $$(seq 0 $$(($(SHARDS)-1))); do printf "../output/shards/commute_times_shard_%s.csv " $$i; done)

all: ../output/chicago_tract_commute_times.csv
include ../../generic.make

../output/chicago_tract_commute_times.csv: merge_shards.py $(SHARD_FILES) | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python merge_shards.py \
		--shard-dir ../output/shards \
		--out $@

../output/chicago_tract_centroids.csv: prepare_centroids.py | $(PYTHON_OOPR) ../output ../temp
	uv --cache-dir ../temp/.uvcache --project ../output run python prepare_centroids.py

../output/shards/commute_times_shard_%.csv: compute_commute_times.py ../output/chicago_tract_centroids.csv | $(PYTHON_OOPR) ../output ../temp ../output/shards
	uv --cache-dir ../temp/.uvcache --project ../output run python compute_commute_times.py \
		--centroids ../output/chicago_tract_centroids.csv \
		--out $@ \
		--shard-index $* \
		--num-shards $(SHARDS) \
		--mode $(MODE) \
		--origin-batch-size $(ORIGIN_BATCH_SIZE) \
		--destination-batch-size $(DESTINATION_BATCH_SIZE)

../output/shards:
	mkdir -p $@

.PHONY: shards
shards: $(SHARD_FILES)

../output/pyproject.toml: ../../setup_environment/output/pyproject.toml | ../output
	ln -sf $(abspath $<) $@
