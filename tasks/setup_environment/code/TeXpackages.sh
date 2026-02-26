#!/bin/bash

#Permissions might necessitate adding the "--usermode" option to the "tlmgr install" command

#slides packages
sudo tlmgr install beamertheme-metropolis appendixnumberbeamer fmtcount datetime  
#paper packages
sudo tlmgr install appendix placeins physics etoc epstopdf footmisc chktex changepage titlesec
#logbook packages
sudo tlmgr install stmaryrd csquotes 
#tasks packages
sudo tlmgr install dvipng #Needed for LaTeXStrings package in Julia
#metadata tools
brew install pandoc