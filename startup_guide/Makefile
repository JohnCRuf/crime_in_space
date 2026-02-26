startup_guideentries := $(shell grep '\\input{.*entries' startup_guide.tex | sed 's/^.*\\input{\(.*\.tex\).*/\1/')

all: startup_guide.pdf

startup_guide.pdf: startup_guide.tex $(startup_guideentries)
	pdflatex -draftmode startup_guide.tex
	pdflatex startup_guide.tex
	rm startup_guide.log startup_guide.out startup_guide.toc startup_guide.aux
