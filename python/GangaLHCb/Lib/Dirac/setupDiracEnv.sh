#!/bin/sh
SetupProjectStatus=1
. SetupProject.sh Dirac $@ > /dev/null
export GANGA_DIRAC_SETUP_STATUS=${SetupProjectStatus}



