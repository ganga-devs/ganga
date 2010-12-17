#! /bin/sh -x
#
# Run Tag preparation on the Grid
#
# Following environment settings are required
#
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results

retcode=0

GANGATIME1=`date +'%s'`
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
# resolving and setting TMPDIR env. variable
resolve_tmpdir

################################################
# information for debugging
print_wn_info

################################################
# Save LD_LIBRARY_PATH
export LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
export PATH_ORIG=$PATH
export PYTHONPATH_ORIG=$PYTHONPATH

################################################
# set up LFC_HOST
if [ ! -z $ATLASOutputDatasetLFC ]
then
    export LFC_HOST=$ATLASOutputDatasetLFC
else
    export LFC_HOST='prod-lfc-atlas-local.cern.ch'
fi

################################################
# get some machine infos
DATE=`date +'%D %T'`
MACH=`uname -srm`
MHZ=`cat /proc/cpuinfo | grep -i 'cpu mhz' | tail -1 | cut -d':' -f2 | tr -s ' ' `
MODEL=`cat /proc/cpuinfo | grep -i 'model name' | tail -1 | cut -d':' -f2 | tr -s ' '`
CACHE=`cat /proc/cpuinfo | grep -i 'cache size' | tail -1 | cut -d':' -f2 | tr -s ' '`
MEMORY=`cat /proc/meminfo | grep -i memtotal | cut -d':' -f2 | tr -s ' '`
HNAME=`hostname -f`
echo "### node info:   $DATE , $MHZ , $MODEL , $MEMORY , $CACHE , $MACH , $HNAME"
#
echo '### checking tmpdirs'
printenv | grep -i tmp

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
# setup CMT 
cmt_setup

################################################
# fix g2c/gcc issues against ATLAS release 11, 12, 13  
if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]; then
    fix_gcc_issue
fi

################################################
# fix g2c/gcc issues against SLC5
fix_gcc_issue_sl5

g++ --version
gcc --version

################################################
# setup ATLAS software

retcode=0

athena_setup; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp

################################################
# Special setup for CNAF
if [ -e $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh ]
then
    source $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh
fi

get_files PDGTABLE.MeV

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

################################################
# Determine lcg-utils version and set commands
get_lcg_util

#################################################
# Determine SE type

if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
    then
    source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
else
    if [ -e dq2info.tar.gz ]; then
	tar xzf dq2info.tar.gz
    fi
fi

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
if [ n$GANGA_SETYPE = n'DPM' ] 
then
    echo 'Creating soft link to fix broken DPM ROOT access in athena'
    ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
fi

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

GANGATIME2=`date +'%s'`

################################################
# Go through each tag file and grab info
printenv

LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
PATH_BACKUP=$PATH
PYTHONPATH_BACKUP=$PYTHONPATH
export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
export PATH=$MY_PATH_ORG:$PATH_BACKUP
export PYTHONPATH=$MY_PYTHONPATH_ORG:$PYTHONPATH_BACKUP

# Remove lib64/python from PYTHONPATH
dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'lib64/python' | tr '\n' ':' `
export PYTHONPATH=$dum
chmod +x ./get_tag_info.py

if [ ! -z $python32bin ]; then
    $python32bin ./get_tag_info.py
else
    if [ -e /usr/bin32/python ]; then
	/usr/bin32/python ./get_tag_info.py
    else
	./get_tag_info.py
    fi
fi

# Fail over
if [ ! -e RunSuccess ]; then
    $pybin ./get_tag_info.py
fi
if [ ! -e RunSuccess ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
    export PATH=$PATH_ORIG
    export PYTHONPATH=$PYTHONPATH_ORIG
    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
	then
	source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
    else
	if [ -e dq2info.tar.gz ]; then
	    tar xzf dq2info.tar.gz
	fi
    fi
    ./get_tag_info.py
fi

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
export PATH=$PATH_BACKUP
export PYTHONPATH=$PYTHONPATH_BACKUP

if [ ! -e taginfo.pkl ]
then
    echo "ERROR: No output produced!"
    exit -1
fi

exit $retcode
