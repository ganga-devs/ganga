#!/bin/bash

# Vastly simplified script to unpack and run an Athena job on local resources

# store env variables that get stomped on by the setup below
MY_ATHENA_OPTIONS=$ATHENA_OPTIONS
MY_OUTPUT_LOCATION=$OUTPUT_LOCATION

# setup Atlas enviroenment
shopt -s expand_aliases
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

# Now create the build directory
echo "------>  Building using cmake..."
mkdir __athena_build__
cd __athena_build__
cmake ../
make clean
make
source x86_64-slc6-gcc49-opt/setup.sh
cd ../

################################################
# create the input.py file
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

sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
mv input.py.new input.py



# Running Athena
echo "------>  Running athena preJobO.py $MY_ATHENA_OPTIONS input.py..."
athena preJobO.py $MY_ATHENA_OPTIONS input.py

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

