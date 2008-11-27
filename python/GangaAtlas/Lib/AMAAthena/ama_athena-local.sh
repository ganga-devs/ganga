#! /bin/sh -x
#
# Run AMAAthena locally
#
# Following environment settings are required
#
# ATLAS_SOFTWARE    ... ATLAS Software installation
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results
# LCG_SETUP         ... LCG UI setup script
#
# ATLAS/NIKHEF - Hurng-Chun.Lee@cern.ch

retcode=0

################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='local'

################################################
# load utility functions 
source athena-utility.sh
source ama_athena-utility.sh

################################################
# load AMAAthena/Ganga job wrapper exitcode
define_exitcode

################################################
## setup grid environment 
if [ -f $LCG_SETUP ]; then
    . $LCG_SETUP
fi

################################################
# resolving and setting TMPDIR env. variable
resolve_tmpdir

################################################
# Save essential library path for later usage
LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
PATH_ORIG=$PATH
PYTHONPATH_ORIG=$PYTHONPATH

################################################
# information for debugging 
print_wn_info
print_ext_wn_info

################################################
# setup CMT 
cmt_setup

################################################
# get remote proxy 
get_remote_proxy

################################################
# setup Athena
time athena_setup

if [ $retcode -ne 0 ]; then
    echo "Athena setup/compilation error." 1>&2
    exit $EC_ATLAS_SOFTWARE_UNAVAILABLE
fi

################################################
# determine PYTHON executable in ATLAS release
# having pybin in environment may cause python
# incompatibility issue. 
#get_pybin

################################################
# prepare/staging input data
#stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
stage_inputs

if [ $retcode -ne 0 ]; then
    echo "Input stage error" 1>&2
    exit $EC_STAGEIN_ERROR
fi
 
################################################
## create configuration job option
ama_make_options

################################################
# run athena
run_athena $ATHENA_OPTIONS input.py

if [ $retcode -ne 0 ]; then
    echo "Athena runtime error" 1>&2
    exit $EC_ATHENA_RUNTIME_ERROR
fi

################################################
# store output
#stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
stage_outputs

if [ $retcode -ne 0 ]; then
    echo "Output stage error" 1>&2
    exit $EC_STAGEOUT_ERROR
fi

exit $retcode
