#! /bin/sh -x
#
# Run Athena locally
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
# setup CMT 
cmt_setup

################################################
# Setup glite UI 
TEST_CMD=`which voms-proxy-init 2>/dev/null`
if [ ! -z $GANGA_GLITE_UI ] && [ -z $TEST_CMD ] 
then
    source $GANGA_GLITE_UI
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
# get remote proxy 
get_remote_proxy

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

#################################################
# Determine SE type

# Unpack dq2info.tar.gz
if [ -e dq2info.tar.gz ]; then
    tar xzf dq2info.tar.gz
fi
detect_setype

# Fix of broken DPM ROOT access
if [ n$GANGA_SETYPE = n'DPM' ] 
then
    echo 'Creating soft link to fix broken DPM ROOT access in athena'
    ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
fi

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH


################################################
# state the inputs

stage_inputs 

################################################
# create the input.py file
if [ ! -f input.py ] && [ $retcode -eq 0 ] 
then
cat - >input.py <<EOF
ic = []
if os.path.exists('input_files'):
    for lfn in file('input_files'):
        name = os.path.basename(lfn.strip())
        pfn = os.path.join(os.getcwd(),name)
        if (os.path.exists(pfn) and (os.stat(pfn).st_size>0)):
            print 'Input: %s' % name
            ic.append('%s' % name)
        elif (os.path.exists(lfn.strip()) and (os.stat(lfn.strip()).st_size>0)):
            print 'Input: %s' % lfn.strip()
            ic.append('%s' % lfn.strip())
    EventSelector.InputCollections = ic

    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1
EOF
if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 15.` ]
then
  sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
  mv input.py.new input.py
fi

fi

# Set timing command
if [ -x /usr/bin/time ]; then
   timecmd="/usr/bin/time -v"
else
   timecmd=time
fi

# run athena
 
get_files PDGTABLE.MeV   

################################################
# run athena
if [ $retcode -eq 0 ]
then
    prepare_athena
    run_athena $ATHENA_OPTIONS input.py
fi

################################################
# store output
if [ $retcode -eq 0 ]
then
    stage_outputs
fi

exit $retcode

