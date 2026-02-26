##This script instantiates the Julia packages used throughout the project.

import Pkg
Pkg.activate("../output")
Pkg.instantiate()
for name in keys(Pkg.project().dependencies)
    try
        @eval using $(Symbol(name))
    catch e
        @warn "Failed to load $name" exception=(e, catch_backtrace())
    end
end

open("../output/julia_packages.txt", "w") do f
    write(f, "Successfully instantiated Project.toml")
end