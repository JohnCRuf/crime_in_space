OOPR = ../output ../temp ../report ../input run.sbatch slurmlogs #Order-only pre-requisites
JULIA_OOPR = ../input/Project.toml ../input/Manifest.toml #Julia pre-requisites
wipeclean: #This deletes all output, input, and logs content
	$(WIPECLEAN) $(CURDIR)

run.sbatch: ../../setup_environment/code/run.sbatch | slurmlogs
	ln -s $< $@
../input/Project.toml: ../../setup_environment/output/Project.toml | ../input/Manifest.toml ../input
	ln -s $< $@
../input/Manifest.toml: ../../setup_environment/output/Manifest.toml | ../input
	ln -s $< $@
slurmlogs ../input ../output ../temp ../report ../metadata:
	mkdir $@

../report/%.csv.log: ../output/%.csv | ../report
ifneq ($(shell command -v md5),)
	cat <(md5 $<) <(echo -n 'Lines:') <(cat $< | wc -l ) <(head -3 $<) <(echo '...') <(tail -2 $<)  > $@
else
	cat <(md5sum $<) <(echo -n 'Lines:') <(cat $< | wc -l ) <(head -3 $<) <(echo '...') <(tail -2 $<) > $@
endif

../report/%.jld2.log: ../input/describe_data_script.jl ../input/describe_data.jl ../output/%.jld2 | $(OOPR) $(JULIA_OOPR)
	$(JULIA) $< ../output/$*.jld2

../input/describe_data.jl ../input/describe_data_script.jl: ../input/%: ../../describe_data/code/% | ../input
	ln -s $< $@

.PRECIOUS: ../../%
../../%: #Generic recipe to produce outputs from upstream tasks
	$(MAKE) -C $(subst output/,code/,$(dir $@)) ../output/$(notdir $@)

../metadata/time.txt: | ../metadata
	-rm -r ../input ../output ../temp
	@! make -n all | grep -q 'make -C' || (echo "Error: Forbidden pattern 'make -C' detected in dry run." >&2; exit 1)
	(time $(MAKE) all) 2> >(grep 'user\|real' > $@) #Redirect stderr to file (record the timing)

../metadata/inputs.txt:  | ../metadata
	-rm -r ../input
	@! make -n all | grep -q 'make -C' || (echo "Error: Forbidden pattern 'make -C' detected in dry run." >&2; exit 1)
	make -n all | grep '^ln' | grep -o '[A-Za-z0-9_]*/output/[A-Za-z0-9_\.]*' | sort | uniq | grep -v 'Project.toml\|Manifest.toml\|describe_data.jl' > $@

../metadata/readme_inputs.txt: ../metadata/inputs.txt #If upstream task has output metadata, use it to build the description of inputs used in this task
	@( \
	  > $@; \
	  while read line; do \
	    top_level_dir=$$(echo $$line | cut -d/ -f1); \
	    filename=$$(basename $$line); \
	    outputs_file=../../$$top_level_dir/metadata/outputs.txt; \
	    if [ -f "$$outputs_file" ]; then \
	      desc=$$(grep "^$$filename:" "$$outputs_file" | cut -d: -f2- | sed 's/^ //'); \
	      echo "\`$$line\`: $$desc" >> $@; \
	    else \
	      echo "\`$$line\`: [No metadata file found]" >> $@; \
	    fi; \
	  done < ../metadata/inputs.txt \
	)