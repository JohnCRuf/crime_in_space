SHELL=bash

all: $(addprefix ../output/,task_flow.png task_flow_branchdiff.png)
branch: $(addprefix ../output/,task_flow.png task_flow_branchdiff.png task_flow_branchdiff_trim.png)
paper: ../output/task_flow_paper_trim.png
sections: $(foreach section, Amazon eventstudy monte_carlo, ../output/task_flow_keyword_$(section)_section.png)
include ../../generic.make

../output/graph.txt: graphmaker.sh $(shell find ../../ -name "Makefile") | ../output
	bash graphmaker.sh
../output/graph_branchdiff.txt: ../output/graph.txt
	cat <(grep -v '}' ../output/graph.txt) <(git diff --name-status main HEAD | grep '^[AM]' | grep -o 'tasks/[A-z0-9_]*' | sed 's/tasks\///' | sort | uniq | sed 's/$$/ [shape=box]/') <(echo }) > $@
../output/graph_branchdiff_trim.txt: graph_box_trim.sh ../output/graph_branchdiff.txt
	bash $^ $@
../output/graph_keyword_%_section.txt: ../output/graph.txt
	(head -n 1 $< && grep "$*" ../output/graph.txt | sed '1d;$d' && tail -n 1 $<) > $@

../output/task_flow.png: ../output/graph.txt
	dot -Grankdir=LR -Tpng $< -o $@
../output/task_flow_%.png: ../output/graph_%.txt
	dot -Grankdir=LR -Tpng $< -o $@

../output/graph_paper.txt: ../output/graph.txt ../../../paper/Makefile
	cat <(grep -v '}' ../output/graph.txt) <(make -n -B -C ../../../paper/ | grep '^ln' | grep -o 'tasks/[A-Za-z0-9_]*' | sed 's/tasks\///' | sort | uniq | sed 's/$$/ -> paper/') <(echo paper [shape=box]) <(echo }) > $@
../output/graph_%_trim.txt: graph_box_trim.sh ../output/graph_%.txt
	bash $^ $@
notinpaper: ../output/graph.txt ../output/graph_paper_trim.txt
	diff --side-by-side --suppress-common-lines <(cat <(awk '{print $$1}' ../output/graph.txt) <(awk '{print $$3}' ../output/graph.txt) | sort | uniq) <(cat <(awk '{print $$1}' ../output/graph_paper_trim.txt) <(awk '{print $$3}' ../output/graph_paper_trim.txt) | sort | uniq | grep -v '^paper$$')
../output/graph_slides.txt: ../output/graph.txt ../../../slides/Makefile
	cat <(grep -v '}' ../output/graph.txt) <(make -n -B -C ../../../slides/ | grep '^ln' | grep -o 'tasks/[A-Za-z0-9_]*' | sed 's/tasks\///' | sort | uniq | sed 's/$$/ -> slides/') <(echo slides [shape=box]) <(echo }) > $@
../output/graph_logbook.txt: ../output/graph.txt ../../../logbook/Makefile
	cat <(grep -v '}' ../output/graph.txt) <(make -n -B -C ../../../logbook/ | grep '^cp' | grep -o 'tasks/[A-Za-z0-9_]*' | sed 's/tasks\///' | sort | uniq | sed 's/$$/ -> logbook/') <(echo logbook [shape=box]) <(echo }) > $@
notused: ../output/graph.txt $(foreach stub, paper slides logbook,../output/graph_$(stub)_trim.txt)
	@echo Left column contains unused tasks, right-column tasks are in paper/slides/logbook but not in graph
	-@diff --side-by-side --suppress-common-lines <(cat <(awk '{print $$1}' ../output/graph.txt) <(awk '{print $$3}' ../output/graph.txt) | sort | uniq) <(cat <(cat <(awk '{print $$1}' ../output/graph_paper_trim.txt) <(awk '{print $$3}' ../output/graph_paper_trim.txt) | sort | uniq | grep -v '^paper$$') <(cat <(awk '{print $$1}' ../output/graph_slides_trim.txt) <(awk '{print $$3}' ../output/graph_slides_trim.txt) | sort | uniq | grep -v '^slides$$') <(cat <(awk '{print $$1}' ../output/graph_logbook_trim.txt) <(awk '{print $$3}' ../output/graph_logbook_trim.txt) | sort | uniq | grep -v '^logbook$$') | sort | uniq)

../output/graph_bidirectional_edges.txt: ../output/graph.txt
	grep -v '{\|}' ../output/graph.txt | awk -F'->' '{if ($$2 < $$1) {print $$2 "," $$1} else {print $$1 "," $$2} }' | sort | uniq -d  > $@