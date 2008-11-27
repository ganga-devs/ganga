#! /bin/sh -x
#
# Run AMAAthena on the Grid
#
# Following environment settings are required
#
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results
#
# ATLAS/NIKHEF - Hurng-Chun.Lee@cern.ch

retcode=0

################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='grid'

################################################
# load utility functions 
source athena-utility.sh
source ama_athena-utility.sh

################################################
# setup grid environment 
if [ ! -z $GANGA_GLITE_UI ] 
then
    source $GANGA_GLITE_UI
fi

################################################
# load AMAAthena/Ganga job wrapper exitcode
define_exitcode

################################################
# for some site doesn't config lcg env. properly
check_lcg_env

################################################
# resolving and setting TMPDIR env. variable
resolve_tmpdir

################################################
# information for debugging
print_wn_info
print_ext_wn_info

################################################
# Save essential library path for later usage 
LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
PATH_ORIG=$PATH
PYTHONPATH_ORIG=$PYTHONPATH

################################################
# set up LFC_HOST
if [ ! -z $ATLASOutputDatasetLFC ]
then
    export LFC_HOST=$ATLASOutputDatasetLFC
else
    export LFC_HOST='prod-lfc-atlas-local.cern.ch'
fi

################################################
# detect ATLAS software
if [ -z $VO_ATLAS_SW_DIR ]
then
   echo "No ATLAS Software found." 1>&2
   # step exits with an error
   # WRAPLCG_WNCHEKC_SWENV
   exit $EC_ATLAS_SOFTWARE_UNAVAILABLE
fi

################################################
# setup DQ2Client environment
dq2client_setup

################################################
# setup CMT environment
cmt_setup

################################################
# fix g2c/gcc issues against ATLAS release 11, 12, 13  
if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]; then
    fix_gcc_issue
fi

#################################################
# setup Athena
time athena_setup

if [ $retcode -ne 0 ]; then
    echo "Athena setup/compilation error." 1>&2
    exit $EC_ATHENA_COMPILATION_ERROR
fi

#################################################
# Special setup for CNAF
if [ -e $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh ]
then
    source $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh
fi

#################################################
# Remove /lib and /usr/lib from LD_LIBRARY_PATH
dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

#################################################
# Remove Grid/globus and Grid/DPM for version 13.0.x
if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]
then 
    dum=`echo $PATH | tr ':' '\n' | egrep -v 'Grid/globus' | egrep -v 'Grid/DPM' | tr '\n' ':' `
    export PATH=$dum
    dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v 'Grid/globus' | egrep -v 'Grid/DPM' | tr '\n' ':' `
    export LD_LIBRARY_PATH=$dum
    dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'Grid/globus' | egrep -v 'Grid/DPM' | tr '\n' ':' `
    export PYTHONPATH=$dum
fi

#################################################
# Determine PYTHON executable in ATLAS release
get_pybin

#################################################
# Determine SE type
detect_setype

#################################################
# Load special DM libraries for dCache/DPM/CASTOR
load_special_dm_libraries

#retcode=0

################################################
# check proxy info 
check_voms_proxy

################################################
# prepare/staging input data
stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

if [ $retcode -ne 0 ]; then
    echo "Input stage error" 1>&2
    exit $EC_STAGEIN_ERROR
fi

#if [ -e input_files ] && [ n$DATASETTYPE != n'DQ2_COPY' ]; then
#    stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
#else
#    # Unpack dq2info.tar.gz
#    if [ -e dq2info.tar.gz ]; then
#        tar xzf dq2info.tar.gz
#    fi
#fi

#################################################
## create AMA-specific internal option files 
ama_make_options

#################################################
# run athena without DQ2_COPY 
if [ $retcode -eq 0 ] && [ n$DATASETTYPE != n'DQ2_COPY' ]; then
    prepare_athena
    run_athena $ATHENA_OPTIONS input.py
fi

#################################################
# run athena with DQ2_COPY 
if [ n$DATASETTYPE = n'DQ2_COPY' ] || ( [ $retcode -ne 0 ] && [ ! -z $DATASETFAILOVER ] ); then
    run_athena_with_dq2_copy
fi

if [ $retcode -ne 0 ]; then
    echo "Athena runtime error" 1>&2
    exit $EC_ATHENA_RUNTIME_ERROR
fi

#################################################
# store output
stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

if [ $retcode -ne 0 ]; then
    echo "Output stage error" 1>&2
    exit $EC_STAGEOUT_ERROR
fi

exit 0
