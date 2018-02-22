#!/bin/bash

# Vastly simplified script to unpack and run an Athena job on local resources

# make sure some output files are present just in case
echo "Create output files to keep Condor happy..."
touch output_location
touch output_guids
touch output_data
touch stats.pickle

# store env variables that get stomped on by the setup below
MY_ATHENA_OPTIONS=$ATHENA_OPTIONS
MY_OUTPUT_LOCATION=$OUTPUT_LOCATION
MY_ATLAS_EXETYPE=$ATLAS_EXETYPE
MY_ATHENA_MAX_EVENTS=$ATHENA_MAX_EVENTS
MY_ATHENA_SKIP_EVENTS=$ATHENA_SKIP_EVENTS

# check for bytestream
if [ n$USE_BYTESTREAM = nTrue ]; then
    MY_USE_BYTESTREAM=True
fi

# setup Atlas enviroenment
shopt -s expand_aliases
echo "------>  Setting up atlas environment"
# note: need to set this env variable as it's used in subsequent scripts
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh

# setup the base athena release
echo "------>  Setting up the base athena release $ATLAS_PROJECT $ATLAS_RELEASE $ATLAS_PRODUCTION"
if [ n$ATLAS_PRODUCTION = n ]; then
    ATLAS_VERSION=$ATLSA_RELEASE
else
    ATLAS_VERSION=$ATLAS_PRODUCTION
fi

# now do asetup
echo "------>  Running asetup $ATLAS_PROJECT,$ATLAS_VERSION,here..."
source $AtlasSetup/scripts/asetup.sh $ATLAS_PROJECT,$ATLAS_VERSION,here

# Now setup user code
if [ n$ATHENA_COMPILE = nTrue ]; then
    echo "------>  Building using cmake..."
    mkdir __athena_build__
    cd __athena_build__
    cmake ../
    make clean
    make
    source */setup.sh
    cd ../
else
    echo "------>  Setting up user code..."
    # NOTE: I'd prefer to incude $CMT_CONFIG here though this is what runAthena has so who am I to question that :)
    # http://pandaserver.cern.ch:25085/trf/user/runAthena-00-00-12
    source usr/*/*/InstallArea/*/setup.sh
fi

# create the input.py file to load in the input data
echo "------>  Creating the pre/post JO files..."
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
        if os.environ.has_key('MY_USE_BYTESTREAM'):
            ServiceMgr.ByteStreamInputSvc.FullFileName = ic
        else:
            ServiceMgr.EventSelector.InputCollections = ic
    except NameError:
        if os.environ.has_key('MY_USE_BYTESTREAM'):
            svcMgr.ByteStreamInputSvc.FullFileName = ic
        else:
            svcMgr.EventSelector.InputCollections = ic
    except AttributeError:
        jps.AthenaCommonFlags.FilesInput = ic

    if os.environ.has_key('MY_ATHENA_MAX_EVENTS') and os.environ['MY_ATHENA_MAX_EVENTS']:
        theApp.EvtMax = int(os.environ['MY_ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1

    if os.environ.has_key('MY_ATHENA_SKIP_EVENTS') and os.environ['MY_ATHENA_SKIP_EVENTS']:
        try:
            ServiceMgr.EventSelector.SkipEvents = int(os.environ['MY_ATHENA_SKIP_EVENTS'])
        except NameError:
            svcMgr.EventSelector.SkipEvents = int(os.environ['MY_ATHENA_SKIP_EVENTS'])
EOF

# re-export the max/skip events as all env variables have been stomped on from above
export MY_ATHENA_MAX_EVENTS
export MY_ATHENA_SKIP_EVENTS

# re-export the byte stream flag
if [ n$MY_USE_BYTESTREAM = nTrue ]; then
    export MY_USE_BYTESTREAM
fi

# Run Athena/EXE/Root
if [ n$MY_ATLAS_EXETYPE == n'ATHENA' ]
then
    echo "------>  Running athena preJobO.py $MY_ATHENA_OPTIONS input.py..."
    athena preJobO.py $MY_ATHENA_OPTIONS input.py ; echo $? > retcode.tmp
elif [ n$MY_ATLAS_EXETYPE == n'PYARA' ]
then
    echo "------>  Running python $MY_ATHENA_OPTIONS..."
    $pybin $MY_ATHENA_OPTIONS ; echo $? > retcode.tmp
elif [ n$MY_ATLAS_EXETYPE == n'ROOT' ]
then
    echo "------>  Running root -b -q $MY_ATHENA_OPTIONS..."
    root -b -q $MY_ATHENA_OPTIONS ; echo $? > retcode.tmp
elif [ n$MY_ATLAS_EXETYPE == n'EXE' ]
then
    echo "------>  Checking for %IN args in exe string..."
    EXE_FILELIST=$(tr '\n' ',' < input_files | sed 's/\//\\\//g' | sed s/,$//)
    NEW_ATHENA_OPTIONS=`echo $MY_ATHENA_OPTIONS | sed s/%IN/$EXE_FILELIST/`
    if [ -z "${NEW_ATHENA_OPTIONS}" ]
    then
	echo "Problem swapping out %IN args, see stderr for actual error. Maybe try using input_files instead."
	NEW_ATHENA_OPTIONS=$MY_ATHENA_OPTIONS
    fi
    export PATH=$PATH:.
    echo "------>  Running $NEW_ATHENA_OPTIONS..."
    ls
    eval $NEW_ATHENA_OPTIONS ; echo $? > retcode.tmp
else
    echo "------>  !!! ERROR: Athena exe type '$ATLAS_EXETYPE' not supported. Contact developers!"
    exit 1
fi

echo "------>  Staging output to $MY_OUTPUT_LOCATION..."
OUTPUT_LOCATION=$MY_OUTPUT_LOCATION
# check for EOS use
case $OUTPUT_LOCATION in
    root*) 
	echo "EOS output detected"
	OUTPUT_LOCATION=`echo ${OUTPUT_LOCATION} | sed 's/root://' | sed 's|//[a-z0-9]*/||'`
	echo "Changed output location to ${OUTPUT_LOCATION}"
	MKDIR_CMD="eos mkdir"
	CP_CMD="eos cp"
	;;
	*)
	  echo "Defaulting to mkdir and cp commands..."
	  MKDIR_CMD="mkdir" 
	  CP_CMD="cp" 
	  ;;
esac

echo $MKDIR_CMD -p $OUTPUT_LOCATION
$MKDIR_CMD -p $OUTPUT_LOCATION
cat output_files | while read filespec; do
    for file in $filespec; do
	if [ $SINGLE_OUTPUT_DIR ]
	then
	    outfile="${file%.*}.${SINGLE_OUTPUT_DIR}.${file##*.}"
	    echo "Changed output file from ${file} to ${outfile}"
	else
	    outfile=$file
	fi
	echo $CP_CMD $file $OUTPUT_LOCATION/$outfile
	$CP_CMD $file $OUTPUT_LOCATION/$outfile; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
	if [ $retcode -ne 0 ]; then
	    sleep 60
	    $CP_CMD $file $OUTPUT_LOCATION/$outfile; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	    if [ $retcode -ne 0 ]; then
		echo 'An ERROR during output stage-out occurred'
		break
	    fi
	fi
    done
done

