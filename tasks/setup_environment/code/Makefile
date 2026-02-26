SHELL=bash
include ../../shell_functions.make
# DEFINITIONS
PROJECT_NAME ?= pyproject
PYTHON_VER   ?= 3.12

all:  ../output/stata_packages.txt ../output/julia_packages.txt ../output/R_packages.txt ../output/python_packages.txt timeout_check
include ../../generic.make

../output/stata_packages.txt: packages.do | ../output
	$(STATA) $<
../output/julia_packages.txt: packages.jl ../output/Manifest.toml ../output/Project.toml 
	julia $<
../output/R_packages.txt: | ../output
	Rscript packages.R
../output/python_packages.txt: py_requirements.txt ../output/pyproject.toml | ../output
	uv --project ../output sync
	echo "Python Packages Installed" > $@

../output/fortran_packages.txt: fortran_requirements.txt ../output/fortran_project | ../output
	fpm --root ../output build
	echo "Fortran Packages Installed" > $@


# Initialization Recipes
../output/pyproject.toml:
	uv init --project ../output --name pyproject --python 3.12
	echo "Python environment initialized with uv."
# Julia Project Initialization
../output/Manifest.toml ../output/Project.toml &: | ../output
	julia -e 'import Pkg; \
		Pkg.activate("../output"); \
		Pkg.instantiate(); \
		println("Initialized environment in ../output")'
# R Project Initialization
../output/renv.lock: | ../output
	Rscript -e 'if (!"renv" %in% rownames(installed.packages())) install.packages("renv"); if (!file.exists("../output/renv.lock")) renv::init(project="../output", bare=TRUE) else renv::activate(project="../output"); renv::snapshot(project="../output")'
# Fortran Project Initialization
../output/fortran_project: | ../output
	fpm new ../output/fortran_project

# Phony Utilities
.PHONY: pyinit timeout_check julia_add R_add fpm_add

# BASH TOOLING
# Quick check for timeout commands
timeout_check:
	@which gtimeout > /dev/null 2>&1 || which timeout > /dev/null 2>&1 || \
		(echo "Neither 'gtimeout' nor 'timeout' commands are available. Please install coreutils." && exit 1)

# Add packages utilities
py_add: 
	uv --project ../output add $(shell cat py_requirements.txt)
	uv --project ../output add --dev ruff black pytest 
julia_add:
	@test -n "$(PKG)" || (echo "Usage: make add PKG=PackageName"; exit 1)
	@julia -e 'import Pkg; \
		Pkg.activate("../output"); \
		Pkg.add("$(PKG)"); \
		Pkg.instantiate()'
R_add:
	@test -n "$(PKG)" || (echo "Usage: make add PKG=package"; exit 1)
	@Rscript -e 'if (!"renv" %in% rownames(installed.packages())) install.packages("renv"); renv::activate("../output/"); renv::install("$(PKG)"); renv::snapshot()'
fpm_add:
	@test -n "$(PKG)" || { echo "Usage: make fpm_add PKG='name = { git = \"url\" }'"; exit 1; }
	echo "$(PKG)" >> "../output/fortran_project/fpm.toml"
	fpm ../output/fortran_project build