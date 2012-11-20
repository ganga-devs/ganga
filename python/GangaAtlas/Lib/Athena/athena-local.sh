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
#
# ATLAS/ARDA - Dietrich.Liko@cern.ch

retcode=0

# Save LD_LIBRARY_PATH
LD_LIBRARY_PATH_ORIG=$LD_LIBRARY_PATH
PATH_ORIG=$PATH
PYTHONPATH_ORIG=$PYTHONPATH

# setup ATLAS software
unset CMTPATH

# Setup glite UI 
TEST_CMD=`which voms-proxy-init 2>/dev/null`
if [ ! -z $GANGA_GLITE_UI ] && [ -z $TEST_CMD ] 
then
    source $GANGA_GLITE_UI
fi

export LCG_CATALOG_TYPE=lfc

#  LFC Client Timeouts
export LFC_CONNTIMEOUT=180
export LFC_CONRETRY=2
export LFC_CONRETRYINT=60

# improve dcap reading speed
export DCACHE_RAHEAD=TRUE ; echo "Setting DCACHE_RAHEAD=TRUE"
#export DCACHE_RA_BUFFER=32768 ; echo "Setting DCACHE_RA_BUFFER=32768"

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


if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
then
    source $ATLAS_SOFTWARE/$ATLAS_RELEASE/setup.sh 
elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
then
    if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]
    then 
	source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_PROJECT,$ATLAS_PRODUCTION
    elif [ ! -z $ATLAS_PROJECT ]
    then
	source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_PROJECT,$ATLAS_RELEASE
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

# setup Athena

echo "Setting up the Athena environment ..."

if [ -z $USER_AREA ] && [ -z $ATHENA_USERSETUPFILE ]
then
    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
    then
	source $SITEROOT/dist/$ATLAS_RELEASE/Control/AthenaRunTime/AthenaRunTime-*/cmt/setup.sh
    elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
    then
	source $SITEROOT/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
    fi

elif [ ! -z $ATHENA_USERSETUPFILE ]
then
    . $ATHENA_USERSETUPFILE
else                                                                                                             
   mkdir work
   if [ ! -z $GROUP_AREA_REMOTE ] ; then
       echo "Fetching group area tarball $GROUP_AREA_REMOTE..."
       wget $GROUP_AREA_REMOTE
       FILENAME=`echo ${GROUP_AREA_REMOTE} | sed -e 's/.*\///'`
       tar xzf $FILENAME -C work
       NUMFILES=`ls work | wc -l`
       DIRNAME=`ls work`
       if [ $NUMFILES -eq 1 ]
       then
	   mv work/$DIRNAME/* work
	   rmdir work/$DIRNAME
       else
	   echo 'no group area clean up necessary'
       fi
   elif [ ! -z $GROUP_AREA ]
   then
       tar xzf $GROUP_AREA -C work
   fi
   tar xzf $USER_AREA -C work
   cd work
   pwd
   source install.sh
   if [ $? -ne 0 ]
   then
      echo "*************************************************************"
      echo "*** Compilation warnings. Return Code $?                  ***"
      echo "*************************************************************"
   fi
   pwd
   cd ..
fi

ls -la

# Unpack dq2info.tar.gz
if [ -e dq2info.tar.gz ]
then
    tar xzf dq2info.tar.gz
fi

# Determine PYTHON executable in ATLAS release
if [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
then
    export pybin=$(ls -r $ATLAS_SOFTWARE/*/sw/lcg/external/Python/*/*/bin/python | head -1)
else
    export pybin=$(ls -r $ATLAS_SOFTWARE/*/sw/lcg/external/Python/*/slc3_ia32_gcc323/bin/python | head -1)
fi


                                                                            
# prepare input data
if [ -e input_files ]
then
    echo "Preparing input data ..."
     # DQ2Dataset
    if [ -e input_guids ] && [ -e ganga-stage-in-out-dq2.py ]
    then 
	chmod +x ganga-stage-in-out-dq2.py
	chmod +x dq2_get
        LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
        PATH_BACKUP=$PATH
        PYTHONPATH_BACKUP=$PYTHONPATH
        export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
        export PATH=$PATH_ORIG:$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_ORIG:$PYTHONPATH_BACKUP
        which python32; echo $? > retcode.tmp
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        if [ $retcode -eq 0 ]; then
            python32 ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
        else
            if [ -e /usr/bin32/python ]
            then
                /usr/bin32/python ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
            else
                ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
            fi
        fi
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        # Fail over
        if [ $retcode -ne 0 ]; then
            $pybin ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        fi
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
        export PATH=$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_BACKUP

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

# run athena
 
get_files PDGTABLE.MeV   
if [ $retcode -eq 0 ]
then
    ls -al
    env | grep DQ2
    env | grep LFC
    echo "Running Athena ..."
    cat input.py
    if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
    then
	chmod +x ganga-joboption-parse.py
	./ganga-joboption-parse.py
    fi
    if [ n$ATLAS_EXETYPE == n'ATHENA' ]
    then 
	$timecmd athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    elif [ n$ATLAS_EXETYPE == n'PYARA' ]
    then
	$timecmd $pybin $ATHENA_OPTIONS ; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    elif [ n$ATLAS_EXETYPE == n'ROOT' ]
    then
	$timecmd root -b -q $ATHENA_OPTIONS ; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    else
	$timecmd athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    fi

fi

# store output
if [ $retcode -eq 0 ]
then
    echo "Storing output data ..."
    if [ -e ganga-stage-in-out-dq2.py ] && [ -e output_files ] && [ ! -z $OUTPUT_DATASETNAME ]
    then
	chmod +x ganga-stage-in-out-dq2.py
	export DATASETTYPE=DQ2_OUT
        LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
        PATH_BACKUP=$PATH
        PYTHONPATH_BACKUP=$PYTHONPATH
        export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
        export PATH=$PATH_ORIG:$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_ORIG:$PYTHONPATH_BACKUP
        which python32; echo $? > retcode.tmp
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        if [ $retcode -eq 0 ]; then
            python32 ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
        else
            if [ -e /usr/bin32/python ]
            then
                /usr/bin32/python ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
            else
                ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
            fi

            ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
        fi
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        # Fail over
        if [ $retcode -ne 0 ]; then
            $pybin ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        fi
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
        export PATH=$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_BACKUP

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
