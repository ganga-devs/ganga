#!/bin/sh
SetupProjectStatus=1
. ${LHCBSCRIPTS}/SetupProject.sh Dirac $@ > /dev/null
export GANGA_DIRAC_SETUP_STATUS=${SetupProjectStatus}



