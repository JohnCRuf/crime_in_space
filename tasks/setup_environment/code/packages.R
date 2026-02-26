renv::restore(project = "../output")
dir.create("../output", recursive = TRUE, showWarnings = FALSE)
write("R Packages Restored", file = "../output/R_packages.txt")