#! /bin/sh -x
#
# Run SFrameARA locally
#
# Following environment settings are required
#
# ATLAS_SOFTWARE    ... ATLAS Software installation
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results
# SFRAME_ARCHIVE    ... Tarball with SFrame sources
# SFRAME_COMPILE    ... Flag to switch on compilation
# SFRAME_XML        ... Options to run SFrame

retcode=0

################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='local'

################################################
# load utility functions 
source athena-utility.sh
source sframe-utility.sh

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
# setup runtime
runtime_setup

################################################
# compile SFrame
compile_SFrame

################################################
# compile Athena
athena_compile

# check the dir list
ls -la

################################################
# get pybin
get_pybin

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
if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
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

################################################
# prepare the XML file
make_XML

retcode=`cat retcode.tmp`
rm -f retcode.tmp

################################################
# run SFrame ARA
run_SFrame

retcode=`cat retcode.tmp`
rm -f retcode.tmp

################################################
# store output
if [ $retcode -eq 0 ]
then
    stage_outputs
fi

exit $retcode

