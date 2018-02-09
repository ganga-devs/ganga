#!/usr/bin/env bash 

if [ $GANGA_LOG_DEBUG -eq 1 ]; then
    set -x 
fi

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

GANGATIME1=`date +'%s'`
################################################
# set the wrapper type
export GANGA_ATHENA_WRAPPER_MODE='local'

#################################################                                                                                                                      
# make sure some output files are present just in case                                                                                                                 
echo "Create output files to keep Condor happy..."
touch  output_location
touch output_guids
touch output_data

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

# Save glite UI environment
LD_LIBRARY_PATH_GLITE=$LD_LIBRARY_PATH
PATH_GLITE=$PATH
PYTHONPATH_GLITE=$PYTHONPATH

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

################################################
# Determine lcg-utils version and set commands
get_lcg_util

#################################################
# Determine SE type

# Unpack dq2info.tar.gz
if [ -e dq2info.tar.gz ]; then
    tar xzf dq2info.tar.gz
    export PYTHONPATH=$PWD:$PYTHONPATH
    export DQ2_HOME=$PWD/opt/dq2
fi
detect_setype

# Fix of broken DPM ROOT access
if [ n$GANGA_SETYPE = n'DPM' ] 
then
    echo 'Creating soft link to fix broken DPM ROOT access in athena'
    ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
fi

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

GANGATIME2=`date +'%s'`
################################################
# state the inputs

if [ $retcode -eq 0 ] 
then
    if [ n$DATASETTYPE == n'FILE_STAGER' ]; then

        filestager_setup

        make_filestager_joption $LD_LIBRARY_PATH_GLITE $PATH_GLITE $PYTHONPATH_GLITE
	echo 'input.txt start ----------'
	cat input.txt
	echo 'input.txt end ----------'
    else
        stage_inputs $LD_LIBRARY_PATH_GLITE $PATH_GLITE $PYTHONPATH_GLITE
    fi
fi

################################################
# create the input.py file
if [ ! -f input.py ] && [ $retcode -eq 0 ] 
then
cat - >preJobO.py <<EOF
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

try:
    from EventSelectorAthenaPool.EventSelectorAthenaPoolConf import EventSelectorAthenaPool
    orig_ESAP__getattribute =  EventSelectorAthenaPool.__getattribute__

    def _dummy(self,attr):
        if attr == 'InputCollections':
            return ic
        else:
            return orig_ESAP__getattribute(self,attr)

    EventSelectorAthenaPool.__getattribute__ = _dummy
    print 'Overwrite InputCollections'
    print EventSelectorAthenaPool.InputCollections
except:
    try:
        EventSelectorAthenaPool.__getattribute__ = orig_ESAP__getattribute
    except:
        pass
      
try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyFilesInput(*argv):
        return ic

    AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except:
    pass

try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyGet_Value(*argv):
        return ic

    for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
        import re
        if re.search('^(Pool|BS).*Input$',tmpAttr) != None:
            try:
                getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value
            except:
                pass
except:
    pass

try:
    from AthenaServices.SummarySvc import *
    useAthenaSummarySvc()
except:
    pass
EOF
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
            
    # set the InputCollections depending on what's in the namespace
    try:
        EventSelector.InputCollections = ic
    except NameError:
        svcMgr.EventSelector.InputCollections = ic

    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1
EOF

ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`
if [ $ATHENA_MAJOR_RELEASE -gt 12 ]
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
# Setup the local ATLAS patches and environment variables
# for Frontier/Squid
frontier_setup

# run athena
 
get_files PDGTABLE.MeV   

GANGATIME3=`date +'%s'`
################################################
# run athena
if [ $retcode -eq 0 ]
then
    prepare_athena
    run_athena $ATHENA_OPTIONS input.py
fi

ls -rtla

GANGATIME4=`date +'%s'`
################################################
# store output
if [ $retcode -eq 0 ]
then
    stage_outputs $LD_LIBRARY_PATH_GLITE $PATH_GLITE $PYTHONPATH_GLITE
fi

#################################################
# print AthSummary.txt

if [ -e AthSummary.txt ] 
    then
    echo "-------- AthSummay.txt ------------"
    cat AthSummary.txt
    echo "-----------------------------------"
fi

#################################################
GANGATIME5=`date +'%s'`

echo "GANGATIME1=$GANGATIME1"
echo "GANGATIME2=$GANGATIME2"
echo "GANGATIME3=$GANGATIME3"
echo "GANGATIME4=$GANGATIME4"
echo "GANGATIME5=$GANGATIME5"

./getstats.py

exit $retcode

