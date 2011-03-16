#! /bin/sh -x
#
# Run Tag preparation locally
#
# Following environment settings are required
#
# ATLAS_SOFTWARE    ... ATLAS Software installation
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results

retcode=0

################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='local'

################################################
# load utility functions 
source athena-utility.sh

################################################
# Save essential library path for later usage
LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
PATH_ORIG=$PATH
PYTHONPATH_ORIG=$PYTHONPATH

################################################
# Setup glite UI 
TEST_CMD=`which voms-proxy-init 2>/dev/null`

if [ ! -z $GANGA_GLITE_UI ] && [ -z $TEST_CMD ] 
then
    source $GANGA_GLITE_UI
fi

if [ -e $PROXY_NAME ]
then
    export X509_USER_PROXY=$PROXY_NAME
fi

################################################
# setup CMT 
cmt_setup


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
# setup Athena

retcode=0

athena_setup; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp


# check the dir list
ls -la

################################################
# get pybin
get_pybin

################################################
# Determine lcg-utils version and set commands
get_lcg_util

#################################################
# Determine SE type

# Set up the DQ2 tools
if [ -e $DQ2_SETUP ]
    then
    source $DQ2_SETUP
else
    if [ -e dq2info.tar.gz ]; then
	tar xzf dq2info.tar.gz
    fi
fi

#if [ -e dq2info.tar.gz ]; then
#    tar xzf dq2info.tar.gz
#fi
detect_setype

# Fix of broken DPM ROOT access
if [ n$GANGA_SETYPE = n'DPM' ] 
then
    echo 'Creating soft link to fix broken DPM ROOT access in athena'
    ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
fi

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

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

if [ n$LCG_PREPARE = n'1' ] 
then
    gettaginfo='./get_tag_info.py'
else
    gettaginfo='./get_tag_info2.py'
fi

chmod +x $gettaginfo

if [ ! -z $python32bin ]; then
    $python32bin $gettaginfo; echo $? > retcode.tmp
else
    if [ -e /usr/bin32/python ]; then
        /usr/bin32/python $gettaginfo; echo $? > retcode.tmp
    else
        $gettaginfo; echo $? > retcode.tmp
    fi
fi
retcode=`cat retcode.tmp`
rm -f retcode.tmp
# Fail over
if [ $retcode -ne 0 ]; then
    $pybin $gettaginfo; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
fi
if [ $retcode -ne 0 ]; then
    export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG
    export PATH=$PATH_ORIG
    export PYTHONPATH=$PYTHONPATH_ORIG
    $gettaginfo; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
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

