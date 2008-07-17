#!/usr/bin/env python
# This is a short example to submit a Gauss job using 
# the Gaudi Applicaton handler
# Andrew Maier 5/4/05
# run this within ganga via exefile
j1=Job()
j1.application=Gaudi()
j1.application.appname="Gauss"
j1.application.optsfile.name=["/afs/cern.ch/lhcb/software/releases/GAUSS/GAUSS_v19r3/Sim/Gauss/v19r3/options/Gauss.opts"]
j1.backend=LSF()


j2=Job()
j2.application=Gaudi()
j2.application.appname="Boole"
j2.application.optsfile.name=["/afs/cern.ch/lhcb/software/releases/BOOLE/BOOLE_v7r3/Digi/Boole/v7r3/options/v200412.opts"]
j2.application.version="v7r3"
j2.backend=LSF()


j3=Job()
j3.application=Gaudi()
j3.application.appname="Brunel"
j3.application.optsfile.name=["/afs/cern.ch/lhcb/software/releases/BRUNEL/BRUNEL_v25r3/Rec/Brunel/v25r3/options/v200412.opts"]
j3.application.version="v25r3"
j3.backend=LSF()


j4=Job()
j4.application=Gaudi()
j4.application.appname="DaVinci"
j4.application.optsfile.name=["/afs/cern.ch/lhcb/software/releases/DAVINCI/DAVINCI_v12r7/Phys/DaVinciEff/v3r2/options/DVEffBs2PhiEtac.opts"]
j4.application.version="v12r7"
j4.backend=LSF()


