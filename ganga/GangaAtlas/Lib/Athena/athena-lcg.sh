#! /bin/sh -x
#
# Run Athena on the Grid
#
# Following environment settings are required
#
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

# helper routines to create the PoolFileCatalog

if [ ! -z $ATLASOutputDatasetLFC ]
then
    export LFC_HOST=$ATLASOutputDatasetLFC
else
    export LFC_HOST='prod-lfc-atlas-local.cern.ch'
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


# check for GangaTnt subcollection and rename
ls | grep 'sub_collection_*' > tmp
if [ $? -eq 0 ]
then
mv sub_collection_* tag.pool.root
tar -c tag.pool.root > tag.tar
gzip tag.tar
fi

# setup ATLAS software

echo "Setting up the Athena environment ..."

if [ -z $VO_ATLAS_SW_DIR ]
then
   echo "No ATLAS Software found." 1>&2
   # step exits with an error
   # WRAPLCG_WNCHEKC_SWENV
   exit 410103
fi

# setup Athena

if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
then
    source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/setup.sh 
elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
then
    if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]
    then
        source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_PROJECT,$ATLAS_PRODUCTION
    elif [ ! -z $ATLAS_PROJECT ]
    then
        source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_PROJECT,$ATLAS_RELEASE
    else
	source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE
    fi
fi

################################################
# fix SL4 gcc problem
if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]
then 

# fix SL4 gcc problem
RHREL=`cat /etc/redhat-release`
SC41=`echo $RHREL | grep -c 'Scientific Linux CERN SLC release 4'`
SC42=`echo $RHREL | grep -c 'Scientific Linux SL release 4'`
which gcc32; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp	
if [ $retcode -eq 0 ] && ( [ $SC41 -gt 0 ] || [ $SC42 -gt 0 ] ) ; then
    mkdir comp
    cd comp

cat - >gcc <<EOF
#!/bin/sh

/usr/bin/gcc32 -m32 \$*

EOF

cat - >g++ <<EOF
#!/bin/sh

/usr/bin/g++32 -m32 \$*

EOF

chmod +x gcc
chmod +x g++
cd ..
export PATH=$PWD/comp:$PATH
ln -s /usr/lib/gcc/i386-redhat-linux/3.4.3/libg2c.a $PWD/comp/libg2c.a
export LD_LIBRARY_PATH=$PWD/comp:$LD_LIBRARY_PATH
export LIBRARY_PATH=$PWD/comp:$LIBRARY_PATH

fi

# fix SL3 g2c problem
SC31=`echo $RHREL | grep -c 'Scientific Linux'`
SC32=`echo $RHREL | grep -c 'elease 3.0'`
ls /usr/lib/libg2c.a; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp	
if [ $retcode -eq 1 ] &&  [ $SC31 -gt 0 ] && [ $SC32 -gt 0 ]  ; then
    ln -s /usr/lib/gcc-lib/i386-redhat-linux/3.2.3/libg2c.a $PWD/libg2c.a
    export LIBRARY_PATH=$PWD:$LIBRARY_PATH
fi

fi
#################################################

if [ -z $USER_AREA ] && [ -z $ATHENA_USERSETUPFILE ]
then
    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
    then
	source $SITEROOT/dist/$ATLAS_RELEASE/Control/AthenaRunTime/AthenaRunTime-*/cmt/setup.sh
    elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
    then
	source $SITEROOT/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
	if [ ! -z $ATLAS_PRODUCTION_ARCHIVE ]
	then
	    wget $ATLAS_PRODUCTION_ARCHIVE
	    export ATLAS_PRODUCTION_FILE=`ls AtlasProduction*.tar.gz`
	    tar xzf $ATLAS_PRODUCTION_FILE
	    export CMTPATH=`ls -d $PWD/work/AtlasProduction/*`
	    export MYTEMPDIR=$PWD
	    cd AtlasProduction/*/AtlasProductionRunTime/cmt
	    cmt config
	    source setup.sh
	    echo $CMTPATH
	    cd $MYTEMPDIR
	fi
    fi

elif [ ! -z $ATHENA_USERSETUPFILE ]
then
    . $ATHENA_USERSETUPFILE
else                                                                                                             
   mkdir work
   
   if [ ! -z $ATLAS_PRODUCTION_ARCHIVE ]
   then
       wget $ATLAS_PRODUCTION_ARCHIVE
       export ATLAS_PRODUCTION_FILE=`ls AtlasProduction*.tar.gz`
       tar xzf $ATLAS_PRODUCTION_FILE -C work
       export CMTPATH=`ls -d $PWD/work/AtlasProduction/*`
       export MYTEMPDIR=$PWD
       cd work/AtlasProduction/*/AtlasProductionRunTime/cmt
       cmt config
       source setup.sh
       echo $CMTPATH
       cd $MYTEMPDIR
   fi

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
       chmod -R +w work
   fi

   tar xzf $USER_AREA -C work
   cd work
   source install.sh
   if [ $? -ne 0 ]
   then
      echo "***************************************************************"
      echo "***      Compilation warnings. Return Code $?               ***"
      echo "***************************************************************"
   fi
   cd ..
fi

# Special setup for CNAF
if [ -e $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh ]
then
    source $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh
fi

get_files PDGTABLE.MeV
# Make a local copy of requested geomDB if none already available
if [ ! -e geomDB ]; then
 mkdir geomDB
 cd geomDB
 get_files -data geomDB/larHV_sqlite
 get_files -data geomDB/geomDB_sqlite
 cd ..
fi
if [ ! -e sqlite200 ]; then
 mkdir sqlite200
 cd sqlite200
 get_files -data sqlite200/ALLP200.db
 cd ..
fi

 
# Remove /lib and /usr/lib from LD_LIBRARY_PATH

dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

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

# Unpack dq2info.tar.gz
if [ -e dq2info.tar.gz ]
then
    tar xzf dq2info.tar.gz
fi

# Determine PYTHON executable in ATLAS release
if [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
then
    export pybin=$(ls -r $VO_ATLAS_SW_DIR/prod/releases/*/sw/lcg/external/Python/*/*/bin/python | head -1)
else
    export pybin=$(ls -r $VO_ATLAS_SW_DIR/prod/releases/*/sw/lcg/external/Python/*/slc3_ia32_gcc323/bin/python | head -1)
fi

# Determine python32 executable location 
# Set python32bin only if athena v14 is NOT setup
which python32; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp
if [ $retcode -eq 0 ] && [ -z `echo $ATLAS_RELEASE | grep 14.` ] ; then
    export python32bin=`which python32`
fi

# Determine SE type
if [ -e ganga-stage-in-out-dq2.py ]
then
    chmod +x ganga-stage-in-out-dq2.py

    LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
    PATH_BACKUP=$PATH
    PYTHONPATH_BACKUP=$PYTHONPATH
    export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
    export PATH=$PATH_ORIG:$PATH_BACKUP
    export PYTHONPATH=$PYTHONPATH_ORIG:$PYTHONPATH_BACKUP

    if [ ! -z $python32bin ]; then
	export GANGA_SETYPE=`$python32bin ./ganga-stage-in-out-dq2.py --setype`
    else
	export GANGA_SETYPE=`$pybin ./ganga-stage-in-out-dq2.py --setype`
    fi
    if [ -z $GANGA_SETYPE ]; then
        export GANGA_SETYPE=`$pybin ./ganga-stage-in-out-dq2.py --setype`
    fi
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
    export PATH=$PATH_BACKUP
    export PYTHONPATH=$PYTHONPATH_BACKUP
fi

# Fix of broken DCache ROOT access in 12.0.x
if [ -e libDCache.so ] && [ n$GANGA_SETYPE = n'DCACHE' ] &&  [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] 
then
    echo 'Fixing broken DCache ROOT access in athena 12.0.x'
    chmod +x libDCache.so
fi

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

# prepare input data
if [ -e input_files ] && [ n$DATASETTYPE != n'DQ2_COPY' ]
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

	if [ ! -z $python32bin ]; then
	    $python32bin ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
	else
	    ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
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
    # ATLASDataset
    elif [ -e ganga-stagein-lfc.py ]
    then
	chmod +x ganga-stagein-lfc.py
	./ganga-stagein-lfc.py -v -i input_files; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp
    elif [ -e ganga-stagein.py ]
    then
	chmod +x ganga-stagein.py
	./ganga-stagein.py -v -i input_files; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp

cat - >input.py <<EOF
ic = []
if os.path.exists('input_files'):
    for lfn in file('input_files'):
        name = os.path.basename(lfn.strip())
        pfn = os.path.join(os.getcwd(),name)
        if (os.path.exists(pfn)) and (os.stat(pfn).st_size>0):
            print 'Input: %s' % name
            ic.append('%s' % name)
    EventSelector.InputCollections = ic
    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1
EOF

    # Configuration error
    else
        # WRAPLCG_WNCHECK_UNSPEC
	retcode=410100
    fi

else
# no input_files
cat - >input.py <<EOF
if os.environ.has_key('ATHENA_MAX_EVENTS'):
   theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])

EOF
fi

ls -rtla


if [ $retcode -eq 0 ] || [ n$DATASETTYPE = n'DQ2_COPY' ]
then
    echo "Parsing jobOptions ..."
    # Parse jobOption file
    if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
    then
	chmod +x ganga-joboption-parse.py
	./ganga-joboption-parse.py
    fi
   # Work around for glite WMS spaced environement variable problem
    if [ -e athena_options ] 
    then
	ATHENA_OPTIONS_NEW=`cat athena_options`
	if [ ! "$ATHENA_OPTIONS_NEW" = "$ATHENA_OPTIONS" ]
	then
	    export ATHENA_OPTIONS=$ATHENA_OPTIONS_NEW	
	fi
    fi
fi


# Set timing command
if [ -x /usr/bin/time ]; then
   timecmd="/usr/bin/time -v"
else
   timecmd=time
fi

#   run athena in regular mode =========================================== 
if [ $retcode -eq 0 ] && [ n$DATASETTYPE != n'DQ2_COPY' ]
then
    cat input.py
    # Start athena
    echo "Running athena ..."
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

#   run athena in copy input file mode ===================================== 
if [ $retcode -eq 0 ] && [ n$DATASETTYPE = n'DQ2_COPY' ]
then
# Create generic input.py
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
if [ n$DATASETDATATYPE = n'MuonCalibStream' ] 
then
  sed 's/EventSelector.InputCollections/svcMgr.MuonCalibStreamFileInputSvc.InputFiles/' input.py > input.py.new
  mv input.py.new input.py
fi

if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] 
then
  sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
  mv input.py.new input.py
fi

cat input.py

if [ -e PoolFileCatalog.xml ]
then
    rm PoolFileCatalog.xml
fi


if [ -e dq2localid.txt ]
then
    export DQ2_LOCAL_ID=`cat dq2localid.txt`
    export DQ2_LOCAL_ID_BACKUP=$DQ2_LOCAL_ID
fi

if [ n$GANGA_SETYPE = n'DCACHE' ]
then	
    export DQ2_LOCAL_PROTOCOL='dcap'
elif [ n$GANGA_SETYPE = n'DPM' ] || [ n$GANGA_SETYPE = n'CASTOR' ]
then
    export DQ2_LOCAL_PROTOCOL='rfio'
fi	

# File counter
I=0

cat input_files | while read filespec
  do
  for file in $filespec
    do
    echo "Downloading input file $file ..."
    let "I += 1"

    LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
    PATH_BACKUP=$PATH
    PYTHONPATH_BACKUP=$PYTHONPATH
    export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
    export PATH=$PATH_ORIG:$PATH_BACKUP
    export PYTHONPATH=$PYTHONPATH_ORIG:$PYTHONPATH_BACKUP
    
    DQ2_LOCAL_ID=$DQ2_LOCAL_ID_BACKUP

    if [ ! -z $python32bin ]; then
	$python32bin dq2_get -v -t 300 $DATASETNAME $file ; echo $? > retcode.tmp
    else
	$pybin dq2_get -v -t 300 $DATASETNAME $file ; echo $? > retcode.tmp
    fi
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
    if [ $retcode -ne 0 ] || [ ! -e $file ]
    then
	export DQ2_LOCAL_ID=''
	export DQ2_COPY_COMMAND='lcg-cp --vo atlas '

	if [ ! -z $python32bin ]; then
	    $python32bin dq2_get -rv -s $DQ2_LOCAL_ID_BACKUP -t 300 $DATASETNAME $file ; echo $? > retcode.tmp
	else
	    $pybin dq2_get -rv -s $DQ2_LOCAL_ID_BACKUP -t 300 $DATASETNAME $file ; echo $? > retcode.tmp
	fi
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    fi

    ls -rtla
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
    export PATH=$PATH_BACKUP
    export PYTHONPATH=$PYTHONPATH_BACKUP

    if [ $retcode -eq 0 ] && [ -e $file ]
    then
        # Create PoolFileCatalog.xml
	pool_insertFileToCatalog $file; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
    fi
    if [ $retcode -eq 0 ] && [ -e $file ]
    then
	echo "Running Athena ..."
        # Start athena

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

	# Rename output files
	cat output_files.new | while read outfiles
	  do
	  for ofile in $outfiles
	    do
	    mv $ofile ${ofile}.$I
	    echo "${ofile}.$I" >> output_files.copy
	  done
	done

	if [ n$ATHENA_MAX_EVENTS != n'-1' ] && [ n$ATHENA_MAX_EVENTS != n'' ] 
	then
	    break
	fi
    else
	echo 'Problems with input file $file'
    fi
    rm $file
  done
  if [ n$ATHENA_MAX_EVENTS != n'-1' ] && [ n$ATHENA_MAX_EVENTS != n'' ] 
  then
      break
  fi
done

if [ -e output_files.copy ]
then
    mv output_files.new output_files.new.old
    mv output_files.copy output_files.new
fi


fi

ls -rtla

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

	if [ ! -z $python32bin ]; then
	    $python32bin ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp	
	else
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
	LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
	PATH_BACKUP=$PATH
	PYTHONPATH_BACKUP=$PYTHONPATH
	export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH_ORIG:/opt/globus/lib:$LD_LIBRARY_PATH_BACKUP
	export PATH=$PATH_ORIG:$PATH_BACKUP
	export PYTHONPATH=$PYTHONPATH_ORIG:$PYTHONPATH_BACKUP
	
	cat output_files | while read filespec
	do
	  for file in $filespec
	    do
	    lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	    if [ $retcode -ne 0 ]
	    then
		sleep 120
		lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
		if [ $retcode -ne 0 ]
		then
		    sleep 120
		    lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
		    retcode=`cat retcode.tmp`
		    rm -f retcode.tmp
		    if [ $retcode -ne 0 ]
		    then
			# WRAPLCG_STAGEOUT_LCGCR
			retcode=410403
		    fi
		fi
	    fi
	  done
	done
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
	export PATH=$PATH_BACKUP
	export PYTHONPATH=$PYTHONPATH_BACKUP
	
    fi
fi

exit $retcode
