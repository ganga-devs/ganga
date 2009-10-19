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


GANGATIME1=`date +'%s'`

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
define_ama_exitcode

################################################
# for some site doesn't config lcg env. properly
check_lcg_env

################################################
# check proxy info
check_voms_proxy

################################################
# resolving and setting TMPDIR env. variable
resolve_tmpdir

################################################
# WN information/specification
print_wn_info
print_ext_wn_info

################################################
# cache the essential library path for later usage
# and make them globally available in child processes
export LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
export PATH_ORIG=$PATH
export PYTHONPATH_ORIG=$PYTHONPATH

################################################
# set up LFC_HOST for staging-out the job's outputs
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
   exit $EC_ATLAS_SOFTWARE_UNAVAILABLE
fi

################################################
# setup DQ2Client environment
dq2client_setup

if [ $? -ne 0 ]; then
    echo "DQ2 client not available" 1>&2

    # when DATASETTYPE is defined, it means DQ2 client is definitely needed.
    # in this case, the job should be stopped if dq2client_setup failed
    if [ ! -z $DATASETTYPE ]; then
        exit $EC_ATLAS_SOFTWARE_UNAVAILABLE
    fi
fi

#################################################
# Special lcgutils setup for some sites (e.g. CNAF)
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

GANGATIME2=`date +'%s'`
################################################
# prepare/staging input data
if [ n$DATASETTYPE != n'FILE_STAGER' ]; then
    stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

    if [ $? -ne 0 ]; then
        echo "Input stage error" 1>&2
        exit $EC_STAGEIN_ERROR
    fi
else
    make_filestager_joption $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
fi

#################################################
## create AMA-specific internal option files
ama_make_options

ls -lt

################################################
# setup CMT environment
cmt_setup

################################################
# fix g2c/gcc issues against ATLAS release 11, 12, 13
if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]; then
    fix_gcc_issue
fi

#################################################
# run athena with different DATASETTYPE
if [ n$DATASETTYPE == n'DQ2_COPY' ]; then

    #################################################
    # setup Athena
    athena_setup
    if [ $? -ne 0 ]; then
        echo "Athena setup/compilation error." 1>&2
        exit $EC_ATHENA_COMPILATION_ERROR
    fi

    ## copy SQLite files locally to avoid SQLite+NFS lock 
    get_sqlite_files

    GANGATIME3=`date +'%s'`

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

    #################################################
    # setup Athena
    athena_setup
    if [ $? -ne 0 ]; then
        echo "Athena setup/compilation error." 1>&2
        exit $EC_ATHENA_COMPILATION_ERROR
    fi

    ## copy SQLite files locally to avoid SQLite+NFS lock 
    get_sqlite_files

    GANGATIME3=`date +'%s'`

    prepare_athena

    ## network RX status
    get_net_rx
	echo NET_ETH_RX_PREATHENA=$NET_RX_BYTE

    ## run athena process 
    ama_run_athena $ATHENA_OPTIONS AMAConfigFile.py input.py

    ## network RX status
    get_net_rx
    echo NET_ETH_RX_AFTERATHENA=$NET_RX_BYTE
fi

if [ $? -ne 0 ]; then
    echo "Athena runtime error" 1>&2
    exit $EC_ATHENA_RUNTIME_ERROR
fi


GANGATIME4=`date +'%s'`
#################################################
# store output
stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

GANGATIME5=`date +'%s'`

echo "GANGATIME1=$GANGATIME1"
echo "GANGATIME2=$GANGATIME2"
echo "GANGATIME3=$GANGATIME3"
echo "GANGATIME4=$GANGATIME4"
echo "GANGATIME5=$GANGATIME5"

if [ $? -ne 0 ]; then
    echo "Output stage error" 1>&2
    exit $EC_STAGEOUT_ERROR
fi

#################################################
# collecting runtime statistics
chmod +x ama_getstats.py
./ama_getstats.py

exit 0
