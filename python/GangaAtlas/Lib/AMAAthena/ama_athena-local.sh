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

################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='local'

################################################
# load utility functions 
source athena-utility.sh
source ama_athena-utility.sh

################################################
# load AMAAthena/Ganga job wrapper exitcode
define_ama_exitcode

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
# setup DQ2Client environment
# !TO BE CHECKED! the dq2client_setup assumes a preinstalled DQ2Clients
# !TO BE CHECKED! which may not be available on the local resource
#dq2client_setup
#
#if [ $? -ne 0 ]; then
#    echo "DQ2 client not available" 1>&2
#
#    # when DATASETTYPE is defined, it means DQ2 client is needed.
#    # in this case, the job should be stopped if dq2client_setup failed
#    if [ ! -z $DATASETTYPE ]; then
#        exit $EC_ATLAS_SOFTWARE_UNAVAILABLE
#    fi
#fi

################################################
# setup CMT 
cmt_setup

################################################
# get remote proxy 
get_remote_proxy

################################################
# setup Athena
athena_setup

if [ $? -ne 0 ]; then
    echo "Athena setup/compilation error." 1>&2
    exit $EC_ATHENA_COMPILATION_ERROR
fi

################################################
# determine PYTHON executable in ATLAS release
# having pybin in environment may cause python
# incompatibility issue. 
#get_pybin

################################################
# prepare/staging input data
if [ n$DATASETTYPE != n'FILE_STAGER' ]; then
    stage_inputs

    if [ $? -ne 0 ]; then
        echo "Input stage error" 1>&2
        exit $EC_STAGEIN_ERROR
    fi
else
    make_filestager_joption $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
fi
 
################################################
## create configuration job option
ama_make_options

ls -lt

#################################################
# run athena with different DATASETTYPE
if [ n$DATASETTYPE == n'DQ2_COPY' ]; then
    ## !TO BE CHECKED! the DQ2_COPY mode can cause the summary root file incorrect.
    ## !TO BE CHECKED! the output merging mechanism in the copy-run loop needs to be checked.
    ama_run_athena_with_dq2_copy
else
    ## at this point, input.py should be properly created; otherwise, something wrong
    if [ ! -f input.py ]; then
        echo "input.py not created" 1>&2
        exit $EC_MAKEOPT_ERROR
    fi

    echo "===== input.py beg. ====="
    cat input.py
    echo "===== input.py end. ====="

    prepare_athena
    ama_run_athena $ATHENA_OPTIONS input.py
fi

if [ $? -ne 0 ]; then
    echo "Athena runtime error" 1>&2
    exit $EC_ATHENA_RUNTIME_ERROR
fi

################################################
# store output
#stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
stage_outputs

if [ $? -ne 0 ]; then
    echo "Output stage error" 1>&2
    exit $EC_STAGEOUT_ERROR
fi

exit 0
