#!/usr/bin/env bash

if [ $GANGA_LOG_DEBUG -eq 1 ]; then
    set -x 
fi

#
# Run Athena on the Grid
#
# Following environment settings are required
#
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# ATHENA_SKIP_EVENTS ... Skip the events before processed by  Athena
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
# check for TAG options
if [ n$DATASETTYPE == n'TAG_LOCAL' ];
then
    export DATASETTYPE='DQ2_LOCAL'
    export TAG_TYPE='AUTO'
fi

if [ n$DATASETTYPE == n'TAG_COPY' ];
then
    export DATASETTYPE='DQ2_COPY'
    export TAG_TYPE='AUTO'
fi

################################################
# setup CMT 
cmt_setup

################################################
# check for TAG subcollections and store the file list
ls | grep '\.subcoll\.' > tag_file_list
echo "Created TAG list:"
more tag_file_list

if [ `cat tag_file_list | wc -l` -le 0 ]
then
    rm tag_file_list
fi

################################################
# fix g2c/gcc issues against ATLAS release 11, 12, 13  
if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]; then
    fix_gcc_issue
fi

################################################
# fix g2c/gcc issues against SLC5

if [ n$CMTCONFIG != n'i686-slc5-gcc43-opt'  ]; then 

    fix_gcc_issue_sl5
fi

g++ --version
gcc --version

################################################
# setup ATLAS software and compile user codes

retcode=0

athena_setup; echo $? > retcode.tmp
retcode=`cat retcode.tmp`
rm -f retcode.tmp

if [ $retcode -ne 0 ]; then
    echo "Athena setup/compilation error." 1>&2
    exit $retcode
fi

################################################
# Special setup for CNAF
if [ -e $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh ]
then
    source $VO_ATLAS_SW_DIR/LCGutils/latest/setup.sh
fi

################################################
# Setup the local ATLAS patches and environment variables
# for Frontier/Squid
frontier_setup

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
	export PYTHONPATH=$PWD:$PYTHONPATH
	export DQ2_HOME=$PWD/opt/dq2
    fi
fi

detect_setype

#################################################
# Set Access Info 
# Set DQ2_LOCAL_SITE_ID and DQ2_LOCAL_PROTOCOL

access_info

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

check_voms_proxy

GANGATIME2=`date +'%s'`
################################################
# prepare/staging input data
if [ $retcode -eq 0 ] && [ -e input_files ]
then
    if [ n$DATASETTYPE == n'FILE_STAGER' ]; then

        filestager_setup

        make_filestager_joption $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
	echo 'input.txt start ----------'
	cat input.txt
	echo 'input.txt end ----------'
    else
        stage_inputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG
    fi
fi

################################################
# prepare input.py
if [ -e input_files ] && [ n$DATASETTYPE != n'DQ2_COPY' ]
then 
    if [ -e ganga-stagein.py ] && [ ! -e input_guids ] && [ ! -e ganga-stage-in-out-dq2.py ] && [ ! -e ganga-stagein-lfc.py ]
    then
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
    if os.environ.has_key('ATHENA_SKIP_EVENTS'):
        from AthenaCommon.AppMgr import ServiceMgr
        ServiceMgr.EventSelector.SkipEvents = int(os.environ['ATHENA_SKIP_EVENTS'])

EOF
    fi

else
# no input_files
cat - >input.py <<EOF
if os.environ.has_key('ATHENA_MAX_EVENTS'):
    theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
if os.environ.has_key('ATHENA_SKIP_EVENTS'):
    from AthenaCommon.AppMgr import ServiceMgr
    ServiceMgr.EventSelector.SkipEvents = int(os.environ['ATHENA_SKIP_EVENTS'])

EOF
fi

################################################
# Download DBRelease

export LD_LIBRARY_PATH_CURRENT=$LD_LIBRARY_PATH
export PATH_CURRENT=$PATH
export PYTHONPATH_CURRENT=$PYTHONPATH

download_dbrelease $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_CURRENT
export PATH=$PATH_CURRENT
export PYTHONPATH=$PYTHONPATH_CURRENT


################################################
# Make a local copy of requested geomDB if none already available
#if [ ! -e geomDB ]; then
# mkdir geomDB
# cd geomDB
# get_files -data geomDB/larHV_sqlite
# get_files -data geomDB/geomDB_sqlite
# cd ..
#fi
#if [ ! -e sqlite200 ]; then
# mkdir sqlite200
# cd sqlite200
# get_files -data sqlite200/ALLP200.db
# cd ..
#fi

ls -rtla

GANGATIME3=`date +'%s'`
#################################################
# run athena
if [ $retcode -eq 0 ] && [ n$DATASETTYPE != n'DQ2_COPY' ]
    then 
    prepare_athena
    if [ $retcode -eq 0 ]; then
	run_athena $ATHENA_OPTIONS input.py
    fi
fi

#################################################
# Specific input and running for DQ2_COPY
if [ n$DATASETTYPE = n'DQ2_COPY' ] || ( [ $retcode -ne 0 ] && [ ! -z $DATASETFAILOVER ] )
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
        elif (os.path.exists(lfn.strip()) and (os.stat(lfn.strip()).st_size>0) and not lfn in add_files):
            print 'Input: %s' % lfn.strip()
            ic.append('%s' % lfn.strip())
    EventSelector.InputCollections = ic
    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1
    if os.environ.has_key('ATHENA_SKIP_EVENTS'):
        from AthenaCommon.AppMgr import ServiceMgr
        EventSelector.SkipEvents = int(os.environ['ATHENA_SKIP_EVENTS'])

EOF
    if [ n$DATASETDATATYPE = n'MuonCalibStream' ] 
	then
	sed 's/EventSelector.InputCollections/svcMgr.MuonCalibStreamFileInputSvc.InputFiles/' input.py > input.py.new
	mv input.py.new input.py
    fi
    ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`
    if [ $ATHENA_MAJOR_RELEASE -gt 12 ]
    then
	sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
	mv input.py.new input.py
    fi
    
    cat input.py
    
# create preJobO.py
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

EOF


    if [ -e PoolFileCatalog.xml ]
	then
	rm PoolFileCatalog.xml
    fi

    # sort out input files for TAG if required
    if [ n$TAG_TYPE = n'LOCAL' ]
	then
	ls -ltr
	mv input_files input_files2

	# inflate the TAG files if necessary
	cat tag_file_list | while read filespec
	  do
	  
	  export LD_LIBRARY_PATH_TAG_BACKUP=$LD_LIBRARY_PATH
	  ext=`basename $filespec .dat`

	  if [ $ext != $filespec ]
	      then

	      echo "UNCOMPRESSING TAG FILE "$filespec

	      export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH 
	      retcode=0
	      ./CollInflateEventInfo.exe $filespec
	      echo $? > retcode.tmp

	      retcode=`cat retcode.tmp`
	      rm -f retcode.tmp
    
	      if [ $retcode -ne 0 ]
		  then
		  echo "ERROR: error during CollInflateEventInfo.exe. Retrying..."
		  export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH_BACKUP_ATH
		  export PATH=$PATH_BACKUP_ATH
		  export PYTHONPATH=$PYTHONPATH_BACKUP_ATH
		  ./CollInflateEventInfo.exe $filespec
		  echo "ERROR: error during CollInflateEventInfo.exe. Giving up..."
		  exit -1
	      fi

	      mv outColl.root $filespec.root
	      echo $filespec.root >> input_files
	  fi
	  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_TAG_BACKUP
	done
    fi

    # Setup new dq2- tools
    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
	then
	source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
    else
	echo 'ERROR: DQ2Clients with dq2-get are not installed at the site - please contact Ganga support mailing list.'
	echo '1'>retcode.tmp
    fi
    #export PATH=$pypath:$PATH
    #export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
	
    # Set DQ2_LOCAL_SITE_ID to dataset location
    if [ -e dq2localid.txt ]
	then
	export DQ2_LOCAL_SITE_ID=`cat dq2localid.txt`
	export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
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
    
    # Parse jobs jobOptions and set timing command
    if [ n$DATASETTYPE = n'DQ2_COPY' ]
	then
	prepare_athena
    fi
    export retcode=0

    if [ n$ATLAS_EXETYPE == n'TRF' ] && [ -e trf_params ]
	then
	if [ ! -z $DBDATASETNAME ] && [ ! -z $DBFILENAME ]
	    then
            # Set DQ2_LOCAL_SITE_ID to db dataset location

	    if [ -e db_dq2localid.py ]
		then
		export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
		chmod +x db_dq2localid.py
		if [ ! -z $python32bin ]; then
		    $python32bin ./db_dq2localid.py; echo $? > retcode.tmp
		else
		    if [ -e /usr/bin32/python ]
			then
			/usr/bin32/python ./db_dq2localid.py; echo $? > retcode.tmp
		    else
			./db_dq2localid.py; echo $? > retcode.tmp
		    fi
		fi
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
		if [ $retcode -ne 0 ]; then
		    $pybin ./db_dq2localid.py; echo $? > retcode.tmp
		    retcode=`cat retcode.tmp`
		    rm -f retcode.tmp
		fi
		export DQ2_LOCAL_SITE_ID=`cat db_dq2localid.txt`
	    fi
	    dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$DBFILENAME $DBDATASETNAME;  echo $? > retcode.tmp
	    if [ -e $DBDATASETNAME/$DBFILENAME ]
		then
		mv $DBDATASETNAME/* .
		echo successfully retrieved $DBFILENAME
		break
	    else
		echo 'ERROR: dq2-get of $DBDATASETNAME failed !'
		echo 'Retry with changed environment'
		LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
		PATH_BACKUP=$PATH
		PYTHONPATH_BACKUP=$PYTHONPATH
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
		export PATH=$PATH_ORIG
		export PYTHONPATH=$PYTHONPATH_ORIG
		if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
		    then
		    source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
		fi
		#export PATH=$pypath:$PATH
		#export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
	
		dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$DBFILENAME $DBDATASETNAME;  echo $? > retcode.tmp
		if [ -e $DBDATASETNAME/$DBFILENAME ]
		    then
		    mv $DBDATASETNAME/* .
		    echo successfully retrieved $DBFILENAME
		else
		    echo 'ERROR: dq2-get of $DBDATASETNAME failed !'
		    echo '1'>retcode.tmp
		fi 
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
		export PATH=$PATH_BACKUP
		export PYTHONPATH=$PYTHONPATH_BACKUP
	    fi
            # Set DQ2_LOCAL_SITE_ID to dataset location
	    if [ -e dq2localid.txt ]
		then
		export DQ2_LOCAL_SITE_ID=`cat dq2localid.txt`
		export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
	    fi

        fi
    fi

    # grab all the additional files
    cat add_files | while read filespec
      do
      
      file=`echo $filespec | cut -d: -f2`
      ADDDATASETNAME=`echo $filespec | cut -d: -f1`

      echo "==============================================="
      echo $file    $ADDDATASETNAME

      # Network traffic
      NET_ETH_RX_PREATHENA=0
      ETH=`/sbin/ifconfig | grep Ethernet | head -1 | awk '{print $1}'`
      if [ -z $ETH ] 
	  then
	  ETH='eth0'
      fi
      NET_ETH_RX_PREATHENA=`/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
      if [ -z $NET_ETH_RX_PREATHENA ] 
	  then
	  NET_ETH_RX_PREATHENA=`/usr/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
      fi
      echo NET_ETH_RX_PREATHENA=$NET_ETH_RX_PREATHENA
      
      echo "Downloading additional file $file ..."
	let "I += 1"
        # use dq2-get to download input file
	if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
	    then
	    for ((i=1;i<=3;i+=1)); do
		echo Copying $file, attempt $i of 3
		dq2-get --client-id=ganga -d --automatic --timeout=300 --files=$file $ADDDATASETNAME;  echo $? > retcode.tmp
		if [ -e $ADDDATASETNAME/$file ]
		    then
		    mv $ADDDATASETNAME/* .
		    echo $file > input.txt
		    echo successfully retrieved $file
		    break
		else
		    echo 'ERROR: dq2-get of addfile failed !'
		    echo 'Retry with changed environment'
		    LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
		    PATH_BACKUP=$PATH
		    PYTHONPATH_BACKUP=$PYTHONPATH
		    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
		    export PATH=$PATH_ORIG
		    export PYTHONPATH=$PYTHONPATH_ORIG
		    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
			then
			source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
		    fi
		    #export PATH=$pypath:$PATH
		    #export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
	
		    dq2-get --client-id=ganga -d --automatic --timeout=300 --files=$file $ADDDATASETNAME;  echo $? > retcode.tmp

		    if [ -e $ADDDATASETNAME/$file ]
			then
			mv $ADDDATASETNAME/* .
			echo $file > input.txt
			echo successfully retrieved $file
			export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
			export PATH=$PATH_BACKUP
			export PYTHONPATH=$PYTHONPATH_BACKUP
			break
		    else
			echo 'ERROR: dq2-get of addfile failed !'
			echo '1'>retcode.tmp
		    fi 
		    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
		    export PATH=$PATH_BACKUP
		    export PYTHONPATH=$PYTHONPATH_BACKUP
		fi
	    done
	else
	    echo 'ERROR: DQ2Clients with dq2-get are not installed at the 
site - please contact Ganga support mailing list.'
	    echo '1'>retcode.tmp
	fi

	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
	ls -rtla
	
	if [ $retcode -eq 0 ] && [ -e $file ]
	    then
        # Create PoolFileCatalog.xml
	    pool_insertFileToCatalog $file; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	fi
      done

    cat input_files | while read filespec
      do
      for file in $filespec
	do
	
	
	# check for local files as input due to TAG
	if [ n$TAG_TYPE != n'LOCAL' ]
	    then

            # Network traffic
	    NET_ETH_RX_PREATHENA=0
	    ETH=`/sbin/ifconfig | grep Ethernet | head -1 | awk '{print $1}'`
	    if [ -z $ETH ] 
		then
		ETH='eth0'
	    fi
	    NET_ETH_RX_PREATHENA=`/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
	    if [ -z $NET_ETH_RX_PREATHENA ] 
		then
		NET_ETH_RX_PREATHENA=`/usr/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
	    fi
	    echo NET_ETH_RX_PREATHENA=$NET_ETH_RX_PREATHENA
	    
	    echo "Downloading input file $file ..."
	    let "I += 1"
            # use dq2-get to download input file
	    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
		then
		for ((i=1;i<=3;i+=1)); do
		    echo Copying $file, attempt $i of 3
		    dq2-get --client-id=ganga -d --automatic --timeout=300 --files=$file $DATASETNAME;  echo $? > retcode.tmp
		    if [ -e $DATASETNAME/$file ]
			then
			mv $DATASETNAME/* .
			echo $file > input.txt
			echo successfully retrieved $file
			break
		    else
			echo 'ERROR: dq2-get of inputfile failed !'
			echo 'Retry with changed environment'
			LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
			PATH_BACKUP=$PATH
			PYTHONPATH_BACKUP=$PYTHONPATH
			export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
			export PATH=$PATH_ORIG
			export PYTHONPATH=$PYTHONPATH_ORIG
			if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
			    then
			    source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
			fi
		    #export PATH=$pypath:$PATH
		    #export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
			
			dq2-get --client-id=ganga -d --automatic --timeout=300 --files=$file $DATASETNAME;  echo $? > retcode.tmp
			
			if [ -e $DATASETNAME/$file ]
			    then
			    mv $DATASETNAME/* .
			    echo $file > input.txt
			    echo successfully retrieved $file
			    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
			    export PATH=$PATH_BACKUP
			    export PYTHONPATH=$PYTHONPATH_BACKUP
			    break
			else
			    echo 'ERROR: dq2-get of inputfile failed !'
			    echo '1'>retcode.tmp
			fi 
			export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
			export PATH=$PATH_BACKUP
			export PYTHONPATH=$PYTHONPATH_BACKUP
		    fi
		done
	    else
		echo 'ERROR: DQ2Clients with dq2-get are not installed at the 
site - please contact Ganga support mailing list.'
		echo '1'>retcode.tmp
	    fi

	else
	    echo '0'>retcode.tmp
	fi

	retcode=`cat retcode.tmp`
	rm -f retcode.tmp
	ls -rtla
	
	if [ $retcode -eq 0 ] && [ -e $file ] && [ n$TAG_TYPE == n'' ]
	    then
        # Create PoolFileCatalog.xml
	    pool_insertFileToCatalog $file; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	fi
	if [ $retcode -eq 0 ] && [ -e $file ]
	    then
	    more PoolFileCatalog.xml
	    echo "Running Athena ..."

        # Start athena
	    
	    if [ n$ATLAS_EXETYPE == n'ATHENA' ]
		then 
		$timecmd athena.py preJobO.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
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
	    elif [ n$ATLAS_EXETYPE == n'TRF' ] && [ -e trf_params ]
		then
                ## need to remove local link to db, or the dbrelease specified in the trf will not have any effect
		rm -rf sqlite200/ALLP200.db
	        ## Parse inputfile string from transformation 
		inputfile=`$ATHENA_OPTIONS | grep input | awk '{print $4}' | sed 's/<//' | sed 's/>//'`
		echo 'TRF inputtype start ...'
		echo $inputfile
		echo 'TRF inputtype end ...'
	        ## Start TRF
		if [ ! -z $DBFILENAME ]
		    then
		    $timecmd $ATHENA_OPTIONS $inputfile'='$file `cat trf_params` 'dbrelease='$DBFILENAME; echo $? > retcode.tmp
		else
		    $timecmd $ATHENA_OPTIONS $inputfile'='$file `cat trf_params`; echo $? > retcode.tmp
		fi
		##
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
	    elif [ n$ATLAS_EXETYPE == n'EXE' ]
		then
		
		# scan for %IN args
		$EXE_FILELIST=`tr '\n' ',' < input.txt`
		$NEW_ATHENA_OPTIONS=`echo "python aratest.py - %IN" | sed s/%IN/$EXE_FILELIST/`
		echo "New EXE command line: "
		echo $NEW_ATHENA_OPTIONS
		$timecmd $NEW_ATHENA_OPTIONS $inputfile'='$file `cat trf_params`; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
	    else
		$timecmd athena.py preJobO.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
	    fi
	    
	# Rename output files
	    cat output_files.new | while read outfiles
	      do
	      for ofile in $outfiles
		do
		# check for both original and new output filenames
		if [ -e $ofile ]
		    then
		    mv $ofile ${ofile}.$I
		    echo "${ofile}.$I" >> output_files.copy
		else
		    old_name=`echo $ofile | awk -F"." '{for (i = 7; i <= NF; i++) printf "%s.", $i}'`
		    old_name=`basename $old_name .`
		    if [ -e $old_name ]
			then
			mv $old_name ${ofile}.$I
			echo "${ofile}.$I" >> output_files.copy
		    fi
		fi
	      done
	    done

	    if [ n$ATHENA_MAX_EVENTS != n'-1' ] && [ n$ATHENA_MAX_EVENTS != n'' ] 
		then
		break
	    fi
	else
	    echo "Problems with input file $file"
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

    # Network traffic
    NET_ETH_RX_AFTERATHENA=0
    ETH=`/sbin/ifconfig | grep Ethernet | head -1 | awk '{print $1}'`
    if [ -z $ETH ] 
	then
	ETH='eth0'
    fi
    NET_ETH_RX_AFTERATHENA=`/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
    if [ -z $NET_ETH_RX_AFTERATHENA ] 
	then
	NET_ETH_RX_AFTERATHENA=`/usr/sbin/ifconfig $ETH | grep 'RX bytes' | awk '{print $2}' | cut -d : -f 2`
    fi
    echo NET_ETH_RX_AFTERATHENA=$NET_ETH_RX_AFTERATHENA

fi

ls -rtla

GANGATIME4=`date +'%s'`
#################################################
# store output
stage_outputs $LD_LIBRARY_PATH_ORIG $PATH_ORIG $PYTHONPATH_ORIG

#################################################
# print AthSummary.txt

if [ -e AthSummary.txt ] 
    then
    echo "-------- AthSummary.txt ------------"
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

#################################################
# Store log files in DQ2 if required
#if [ z$GANGA_LOG_HANDLER == z"DQ2" ] || [ z$GANGA_LOG_HANDLER == z"WMS" ]
if [ z$GANGA_LOG_HANDLER == z"DQ2" ]
    then

    LOGTIME=`date +'%Y%m%d%H%M%S'`
    LOGNAME=${OUTPUT_DATASETNAME}.${LOGTIME}_${OUTPUT_JOBID}.log.tgz
    echo "Storing logfiles as "$LOGNAME" in dq2 dataset..."
    mkdir tarball
    cp stdout stderr tarball/
    tar czhf $LOGNAME tarball/
    echo $LOGNAME > logfile
    DATASETTYPE=DQ2_OUT
    if [ ! -z $python32bin ]; then
	$python32bin ./ganga-stage-in-out-dq2.py --output=logfile; echo $? > retcode.tmp
    else
	if [ -e /usr/bin32/python ]
	    then
	    /usr/bin32/python ./ganga-stage-in-out-dq2.py --output=logfile; echo $? > retcode.tmp
	else
	    ./ganga-stage-in-out-dq2.py --output=logfile; echo $? > retcode.tmp
	fi
    fi
    retcodelog=`cat retcode.tmp`
    rm -f retcode.tmp
    # Fail over
    if [ $retcodelog -ne 0 ]; then
	$pybin ./ganga-stage-in-out-dq2.py --output=logfile; echo $? > retcode.tmp
	retcodelog=`cat retcode.tmp`
	rm -f retcode.tmp
    fi
    if [ $retcodelog -ne 0 ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
	export PATH=$PATH_ORIG
	export PYTHONPATH=$PYTHONPATH_ORIG
	if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
	    then
	    source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
	else
	    if [ -e dq2info.tar.gz ]; then
		tar xzf dq2info.tar.gz
		export PYTHONPATH=$PWD:$PYTHONPATH
		export DQ2_HOME=$PWD/opt/dq2
	    fi
	fi
	./ganga-stage-in-out-dq2.py --output=logfile; echo $? > retcode.tmp
	retcodelog=`cat retcode.tmp`
	rm -f retcode.tmp
    fi
fi

./getstats.py

exit $retcode
