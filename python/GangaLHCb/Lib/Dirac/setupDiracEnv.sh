#!/bin/sh

export DIRACROOT=$1 
source ${LHCBSCRIPTS}/GridEnv.sh
export DPLAT=`$DIRACROOT/scripts/platform.py`
export PATH=$DIRACROOT/$DPLAT/bin:$DIRACROOT/scripts:$PATH
export LD_LIBRARY_PATH=$DIRACROOT/$DPLAT/lib:$LD_LIBRARY_PATH
export PYTHONPATH=$DIRACROOT

