#! /bin/sh -x
#
# Run SFrame Locally
#
#
# marcello.barisonzi@desy.de

retcode=0

#timestamping
python -c "import time; print time.gmtime()" >> timestamps.txt

# setup ATLAS software
unset CMTPATH

if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
then
    source $ATLAS_SOFTWARE/$ATLAS_RELEASE/setup.sh 
elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]
then
    if [ ! -z $ATLAS_PROJECT ]
    then
        source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_RELEASE,$ATLAS_PROJECT
    else
        source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE
    fi
fi


export X509_CERT_DIR=$X509CERTDIR
if [ ! -z $REMOTE_PROXY ]
then
    scp -o StrictHostKeyChecking=no $REMOTE_PROXY $PWD/.proxy
    export X509_USER_PROXY=$PWD/.proxy
fi

# use ATLAS SW ROOT
#export  ROOTSYS=$ATLAS_SOFTWARE/$ATLAS_RELEASE/sw/lcg/external/root/*/*/root/
#export  PATH=$ROOTSYS/bin:$PATH
#export  LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH

echo "ROOTSYS=" $ROOTSYS

#unpack SFrame

if [ ! -z $SFRAME_ARCHIVE ]
then
    if [ -z $SFRAME_COMPILE ]
    then  # simple extraction
	tar xzf $SFRAME_ARCHIVE	
	ln -s dev/JobConfig.dtd .
    else
	./compile_archive.py $SFRAME_ARCHIVE
    fi

    rm -rf  $SFRAME_ARCHIVE

    export  PATH=./bin:$PATH
    export  LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH    
fi

#export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

# prepare input data
if [ -e input_files ]
then 
    echo "Preparing input data ..."
    # DQ2Dataset
    if [ -e input_guids ] && [ -e ganga-stage-in-out-dq2.py ]
    then
	chmod +x ganga-stage-in-out-dq2.py
	chmod +x dq2_get
	./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp	

    # ATLASDataset, ATLASCastorDataset, ATLASLocalDataset
    elif [ -e ganga-stagein.py ]
    then
	chmod +x ganga-stagein.py
	./ganga-stagein.py -v -i input_files; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp
    else
	cat input_files | while read file
	  do
	  pool_insertFileToCatalog $file 2>/dev/null; echo $? > retcode.tmp
	  retcode=`cat retcode.tmp`
	  rm -f retcode.tmp
	done
    fi
fi

#printenv

# run SFrame

if [ $retcode -eq 0 ]
then
    echo "Running SFrame ..."
    # Parse jobOption file
    if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
    then
	chmod +x ganga-joboption-parse.py
	./ganga-joboption-parse.py
    fi
#    ./pool2sframe.py -s $SFRAME_XML -p PoolFileCatalog.xml; echo $? > retcode.tmp
    if [ -e PoolFileCatalog.xml ]
	then
	./pool2sframe.py $SFRAME_XML PoolFileCatalog.xml; echo $? > retcode.tmp
    elif [ -e input_files ]
	then
	./input2sframe.py $SFRAME_XML input_files; echo $? > retcode.tmp
    else
	echo "ERROR: cannot prepare xml config file"
    fi

    sframe_main ganga_$SFRAME_XML; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
fi

# timestamping
python -c "import time; print time.gmtime()" >> timestamps.txt

ls -rtla

# store output
if [ $retcode -eq 0 ]
then
    echo "Storing output data ..."
    if [ -e ganga-stage-in-out-dq2.py ] && [ -e output_files ] && [ ! -z $OUTPUT_DATASETNAME ]
    then
	chmod +x ganga-stage-in-out-dq2.py
	export DATASETTYPE=DQ2_OUT
	./ganga-stage-in-out-dq2.py --output=output_files.new	

    elif [ -n "$OUTPUT_LOCATION" -a -e output_files ]
    then
	TEST_CMD=`which rfmkdir 2>/dev/null`
	if [ ! -z $TEST_CMD ]
	then
	    MKDIR_CMD=$TEST_CMD
	else
	    MKDIR_CMD="mkdir" 
	fi
	TEST_CMD2=`which rfcp 2>/dev/null`
	if [ ! -z $TEST_CMD2 ]
	then
	    CP_CMD=$TEST_CMD2
	else
	    CP_CMD="cp" 
	fi

	$MKDIR_CMD -p $OUTPUT_LOCATION
	cat output_files | while read filespec
	do
	  for file in $filespec
	  do
	    $CP_CMD $file $OUTPUT_LOCATION/$file
	  done
	done
    fi
fi

exit $retcode
