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

################################################
# setup grid environment 
if [ ! -z $GANGA_GLITE_UI ] 
then
    source $GANGA_GLITE_UI
fi

################################################
# for some site doesn't config lcg env. properly
check_lcg_env

################################################
# resolving and setting TMPDIR env. variable
resolve_tmpdir

################################################
# information for debugging
print_wn_info

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
# check for GangaTnt subcollection and rename
#ls | grep 'sub_collection_*' > tmp
#if [ $? -eq 0 ]
#then
#mv sub_collection_* tag.pool.root
#tar -c tag.pool.root > tag.tar
#gzip tag.tar
#fi

################################################
# detect ATLAS software
if [ -z $VO_ATLAS_SW_DIR ]
then
   echo "No ATLAS Software found." 1>&2
   # step exits with an error
   # WRAPLCG_WNCHEKC_SWENV
   exit 410103
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
athena_setup

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
# Fix of broken DCache ROOT access in 12.0.x
if [ -e libDCache.so ] && [ n$GANGA_SETYPE = n'DCACHE' ] &&  [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] 
then
    echo 'Fixing broken DCache ROOT access in athena 12.0.x'
    chmod +x libDCache.so
fi

#################################################
# Fix of broken DPM ROOT access in 12.0.x
if [ -e libRFIO.so ] && [ n$GANGA_SETYPE = n'DPM' ] && ( [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] )
then
    echo 'Fixing broken DPM ROOT access in athena 12.0.x'
    chmod +x libRFIO.so
fi
if [ -e libRFIO.so ] && [ n$GANGA_SETYPE = n'DPM' ] && [ ! -z `echo $ATLAS_RELEASE | grep 13.2.0` ]
then
    echo 'Remove libRFIO.so in athena 13.2.0'
    rm libRFIO.so
fi
if [ n$GANGA_SETYPE = n'DPM' ] 
then
    echo 'Creating soft link to fix broken DPM ROOT access in athena'
    ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
fi

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

retcode=0

################################################
# check proxy info 
check_voms_proxy

################################################
# prepare/staging input data
stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
#stage_inputs

#################################################
## create AMA-specific internal option files 
ama_make_options

#################################################
# Work around for glite WMS spaced environement variable problem
if [ -e athena_options ]; then 
    ATHENA_OPTIONS_NEW=`cat athena_options`
    if [ ! "$ATHENA_OPTIONS_NEW" = "$ATHENA_OPTIONS" ]
    then
        export ATHENA_OPTIONS=$ATHENA_OPTIONS_NEW	
    fi
fi

#################################################
# run athena
run_athena $ATHENA_OPTIONS input.py

#################################################
# store output
stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
#stage_outputs

exit $retcode
