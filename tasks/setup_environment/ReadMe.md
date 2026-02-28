---
title: Setup Environment
tags: [Infrastructure]
---

# Setup Environment

This task handles environmental setup for the project.
Generally I try to keep a very "tight" control over the environment to ensure high reliability but also try to make it easy to use.
For this, I really found that julia's "Project.toml" and "Manifest.toml" files are very useful.
These files allow you to specify the exact versions of packages that your project depends on, ensuring that everyone working on the project has the same environment.
The current setup uses different setups for different languages:

## Language Specifics:
- Julia: Built-in package manager with `Project.toml` and `Manifest.toml`.
- Python: Uses [uv](https://docs.astral.sh/uv/). Works similar to Julia's package manager.
- Stata: Uses an ad-hoc solution with `packages.do` to install. Does not ensure exact versions, but does ensure that the packages are installed.
- R: Uses `renv` to manage packages. Similar to Julia's package manager.
- Fortran: Currently uses `fpm` (Fortran Package Manager) to manage packages. 
It allows for easy installation and management of Fortran libraries.
Functions similar to Julia. 


## Python Packages

The following packages are installed into the shared `uv` environment and are available
to all Python tasks via the `$(PYTHON)` macro (which runs `uv --project ../output`):

| Package | Purpose |
|---|---|
| `numpy` | Numerical arrays |
| `pandas` | Tabular data and Parquet I/O |
| `requests` | HTTP downloads and API calls (OSRM, Socrata) |
| `pyarrow` | Parquet read/write backend for pandas |
| `geopandas` | Spatial join of crime points to taxi zone polygons |
| `matplotlib` | Plotting |
| `tqdm` | Progress bars for long-running API loops |

Each downstream Python task symlinks this task's `output/pyproject.toml` into its own
`output/` directory so the shared environment is discovered by `uv`.

## To Use:

There are a handful of shortcuts to add certain packages to the do files.
`make` should succeed on instantiating the R, Julia, and Python environments.
To add packages to R and Julia, you can use "make r_add PKG=PackageName" or "make julia_add PKG=PackageName".
For Python, you can add packages to the `py_requirements.txt` file and then run `make py_add` to add them to the environment.

