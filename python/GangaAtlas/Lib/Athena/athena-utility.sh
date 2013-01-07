#!/usr/bin/env bash

# function for resolving and setting TMPDIR env. variable
resolve_tmpdir () {
    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        #  - fistly respect the TMPDIR setup on WN
        #  - use SCRATCH_DIRECTORY from pre-WS globus gatekeeper 
        #  - use EDG_WL_SCRATCH from gLite middleware
        #  - use /tmp dir forcely in the worst case
        if [ -z $TMPDIR ]; then
            if [ ! -z $SCRATCH_DIRECTORY ]; then
                export TMPDIR=$SCRATCH_DIRECTORY  
            elif [ ! -z $EDG_WL_SCRATCH ]; then
                export TMPDIR=$EDG_WL_SCRATCH
            else 
                export TMPDIR=`mktemp -d /tmp/ganga_scratch_XXXXXXXX`
                if [ $? -ne 0 ]; then
                    echo "cannot create and setup TMPDIR"
                    exit 1
                fi
            fi
        fi
    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        #  - check if WORKDIR is given in the LSF case
        if [ ! -z $LSB_JOBID ] && [ ! -z $WORKDIR ]; then
            export TMPDIR=`mktemp -d $WORKDIR/ganga_scratch_XXXXXXXX`
        fi
    fi
}

## function for checking voms proxy information
check_voms_proxy() {
    echo "== voms-proxy-info -all =="
    voms-proxy-info -all
    echo "===="
}

## function for printing WN env. info 
print_wn_info () {
    echo "== hostname =="
    hostname -f
    echo "===="
    echo 

    echo "== system =="
    uname -a
    echo "===="
    echo 

    echo "==  env. variables =="
    env
    echo "===="
    echo 

    echo "==  disk usage  =="
    df .
    echo "===="
    echo 
}

## function for setting up file stager environment
filestager_setup() {

    if [ -e $PWD/fs-copy.py ]; then
        chmod +x $PWD/fs-copy.py
    fi
}

## function for setting up frontier configurations properly
frontier_setup() {
    if [ -e $VO_ATLAS_SW_DIR/local/setup.sh ]; then
        source $VO_ATLAS_SW_DIR/local/setup.sh
    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
	if [[ $DQ2_LOCAL_SITE_ID == DESY-HH* ]] || [[ $DQ2_LOCAL_SITE_ID == DESY-ZN* ]]; then
	    if [ -e /afs/naf.desy.de/group/atlas/software/conditions/local/setup.sh ]; then 
		source /afs/naf.desy.de/group/atlas/software/conditions/local/setup.sh
	    fi
	fi
    fi

    echo "==  Frontier + ATLAS_POOLCOND_PATH setup  =="
    if [ -z "$ATLAS_POOLCOND_PATH" ];then
	echo 'ATLAS_POOLCOND_PATH env not set'
	PFCFAILOVER=1
#    elif [ ! -f $ATLAS_POOLCOND_PATH/poolcond/PoolFileCatalog.xml ];then
    elif [ ! -f $ATLAS_POOLCOND_PATH/poolcond/PoolFileCatalog.xml ] && [ ! -f $ATLAS_POOLCOND_PATH/poolcond/PoolCat_oflcond.xml ]; then
	echo "$ATLAS_POOLCOND_PATH/poolcond/PoolFileCatalog.xml and $ATLAS_POOLCOND_PATH/poolcond/PoolCat_oflcond.xml does not exist"
	PFCFAILOVER=1
    else
	echo "ATLAS_POOLCOND_PATH: $ATLAS_POOLCOND_PATH"
	PFCFAILOVER=0
    fi

    if [ $PFCFAILOVER -eq 1 ];then
	echo 'Failing over to http backup for CD PFC'
	mkdir poolcond
	wget --timeout=60 -O poolcond/PoolFileCatalog.xml http://atlas-conditions.web.cern.ch/atlas-conditions/poolcond/catalogue/web/PoolFileCatalog.xml
	export ATLAS_POOLCOND_PATH=`pwd`
	echo '!!!!!!!!!!!!!!!! ATTENTION !!!!!!!!!!!!!!!!!!!!'
	echo "ATLAS_POOLCOND_PATH: $ATLAS_POOLCOND_PATH"
	echo 'ATLAS_POOLCOND_PATH is not properly set - please setup correct path to conditions data pool flat files'
	echo 'Using http fail-over for now'
	echo '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    fi


    if [ -n $FRONTIER_SERVER ]; then
	echo 'FRONTIER_SERVER : ' $FRONTIER_SERVER
    else
	echo 'ERROR: FRONTIER_SERVER not set !' 
    fi
    echo "===="
    echo 


}

## function for setting up CMT environment
cmt_setup () {

    # FIX CREAM CE quotes environment problem
    #if [ -n $CREAM_JOBID ]; then 
    #    env | grep \' | sed "s/'//g" | xargs -0 | sed "s/^/export /g"  | sed '$d' > repairenv.sh
    #    source repairenv.sh
    #fi

    # setup ATLAS software
    unset CMTPATH
    
    export LCG_CATALOG_TYPE=lfc
    
    #  LFC Client Timeouts
    export LFC_CONNTIMEOUT=180
    export LFC_CONRETRY=2
    export LFC_CONRETRYINT=60
    
    # improve dcap reading speed
    export DCACHE_RAHEAD=TRUE
    #export DCACHE_RA_BUFFER=262144
    # Switch on private libdcap patch with improved read-ahead buffer algorithm
    export DC_LOCAL_CACHE_BUFFER=1
    if [ n$DQ2_LOCAL_SITE_ID == n'LRZ-LMU_DATADISK' ] && [ n$DATASETTYPE == n'DQ2_LOCAL' ]; then  
	export DCACHE_CLIENT_ACTIVE=1
    fi

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        ATLAS_RELEASE_DIR=$VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE
    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        ATLAS_RELEASE_DIR=$ATLAS_SOFTWARE/$ATLAS_RELEASE
    fi
 
    ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`

    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]; then
        source $ATLAS_RELEASE_DIR/setup.sh
    # New athena v16 AtlasSetup #################
    elif [ $ATHENA_MAJOR_RELEASE -gt 15 ]; then
	# ##### CVMFS setup ###########################################
	if [[ $ATLAS_RELEASE_DIR == /cvmfs/* ]]; then
	    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
	    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
	    if [[ $ATLAS_RELEASE_DIR == /cvmfs/* ]]; then
		export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
		source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
		if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
		    source $AtlasSetup/scripts/asetup.sh $ATLAS_PROJECT,$ATLAS_PRODUCTION,$ATLAS_ARCH
		elif [ ! -z $ATLAS_PROJECT ]; then
		    source $AtlasSetup/scripts/asetup.sh $ATLAS_PROJECT,$ATLAS_RELEASE,$ATLAS_ARCH
		else
		    source $AtlasSetup/scripts/asetup.sh $ATLAS_RELEASE,$ATLAS_ARCH
		fi
	    fi

	elif [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
	    source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh $ATLAS_PRODUCTION,$ATLAS_PROJECT,$ATLAS_ARCH,setup
	elif [ ! -z $ATLAS_PROJECT ]; then
	    source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh $ATLAS_RELEASE,$ATLAS_PROJECT,$ATLAS_ARCH,setup
	else
	    source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh AtlasOffline,$ATLAS_RELEASE,$ATLAS_ARCH,setup
	fi

    else 
        #if [ n$ATLAS_PROJECT = n'AtlasPoint1' ]; then
        if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_PRODUCTION,$ATLAS_PROJECT
        elif [ ! -z $ATLAS_PROJECT ]; then
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_RELEASE,$ATLAS_PROJECT
        else
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE
        fi

	# check if 64 bit was made and correct it
	if [ n$CMTCONFIG == n'x86_64-slc5-gcc43-opt'  ]; then 

	    if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
		source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_PRODUCTION,$ATLAS_PROJECT,$ATLAS_ARCH,setup
	    elif [ ! -z $ATLAS_PROJECT ]; then
		source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_RELEASE,$ATLAS_PROJECT,$ATLAS_ARCH,setup
	    else
		source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE,$ATLAS_ARCH,setup
	    fi
	fi    
    fi

    # print relevant env. variables for debug 
    echo "CMT setup:"
    env | grep 'CMT'
    echo "SITE setup:"
    env | grep 'SITE'
}

## function for setting up pre-installed dq2client tools on WN
dq2client_setup () {

    if [ ! -z $DQ2_CLIENT_VERSION ]; then

        ## client side version request
        source $VO_ATLAS_SW_DIR/ddm/$DQ2_CLIENT_VERSION/setup.sh

    else

        ## latest version: general case
        source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh

    fi
  
    # check configuration
    echo "DQ2CLIENT setup:"
    env | grep 'DQ2_'
    which dq2-ls

    ## return 0 if dq2-ls is found in PATH; otherwise, return 1
    return $?
}

## function for getting grid proxy from a remote endpoint 
get_remote_proxy () {
    export X509_CERT_DIR=$X509CERTDIR
    if [ ! -z $REMOTE_PROXY ]; then
	REMOTE_PROXY_PATH=`echo $REMOTE_PROXY | awk -F ':' '{print $2}'`
	if [ -f $REMOTE_PROXY_PATH ]; then
	    echo cp $REMOTE_PROXY_PATH $PWD/.proxy
	    cp $REMOTE_PROXY_PATH $PWD/.proxy
	else
	    echo scp -o StrictHostKeyChecking=no $REMOTE_PROXY $PWD/.proxy 
	    scp -o StrictHostKeyChecking=no $REMOTE_PROXY $PWD/.proxy
	fi
	if [ -e $PWD/.proxy ]; then 
	    export X509_USER_PROXY=$PWD/.proxy
        fi
    fi

    # print relevant env. variables for debug 
    env | grep 'GLITE'
    env | grep 'X509'
    env | grep 'GLOBUS' 
    voms-proxy-info -all
}

fix_gcc_issue_sl5 () {

    # fix SL5 gcc/g++ problem - need to use version 3.4
    RHREL=`cat /etc/redhat-release`
    SC51=`echo $RHREL | grep -c 'Scientific Linux CERN SLC release 5'`
    SC52=`echo $RHREL | grep -c 'Scientific Linux SL release 5'`

    if [ $SC51 -gt 0 ] || [ $SC52 -gt 0 ]; then 
        gcc34_path=`which gcc34`

        if [ $? -eq 0 ]; then
            if [ ! -d comp ]; then
                mkdir comp
            fi
            ln -sf $gcc34_path comp/gcc
        fi

        gpp34_path=`which g++34`
        if [ $? -eq 0 ]; then
            if [ ! -d comp ]; then
                mkdir comp
            fi
            ln -sf $gpp34_path comp/g++
        fi

        export PATH=$PWD/comp:$PATH
    fi
}

## function for fixing g2c/gcc issues on SLC3/SLC4 against
## ATLAS release 11, 12, 13 
fix_gcc_issue () {
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
}

## function for setting up athena runtime environment 
athena_setup () {

    echo "Setting up the Athena environment ..."

    retcode=0

    if [ -z $USER_AREA ] && [ -z $ATHENA_USERSETUPFILE ]
    then
	runtime_setup
    elif [ ! -z $ATHENA_USERSETUPFILE ]
    then
        . $ATHENA_USERSETUPFILE
    else
	athena_compile
    retcode=$?
    fi

    return $retcode
}

# determine lcg_utils version and setup lcg-* commands
get_lcg_util () {

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
	LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
	PATH_BACKUP=$PATH
	PYTHONPATH_BACKUP=$PYTHONPATH

	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
	export PATH=$PATH_ORIG
	export PYTHONPATH=$PYTHONPATH_ORIG
    fi

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
	LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
	PATH_BACKUP=$PATH
	PYTHONPATH_BACKUP=$PYTHONPATH

	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_GLITE
	export PATH=$PATH_GLITE
	export PYTHONPATH=$PYTHONPATH_GLITE
    fi

    # find version string
    export lcgutil_str=`lcg-cr --version | grep lcg | cut -d- -f2`
    a=`echo ${lcgutil_str} | cut -d. -f1`
    b=`echo ${lcgutil_str} | cut -d. -f2`
    c=`echo ${lcgutil_str} | cut -d. -f3`
    export lcgutil_num=`echo $a \* 1000000 + $b \* 1000 + $c | bc`
    
    echo "Current LCG Utilities version: "$lcgutil_str

    # default commands
    export lcgcr="lcg-cr -t 300 "

    # check against version 1.7.2 (1000000 * 1 + 1000 * 7 + 2 = 1007002)
    if ( [ $lcgutil >= 1007002 ] ); then
	
	# use the new timout options for lcg-cr
	echo "WARNING: New lcg-cr timeout commands being used"
	export lcgcr="lcg-cr --connect-timeout 150 --sendreceive-timeout 150 --srm-timeout 150 --bdii-timeout 150 "
    fi

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ] || [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
	export PATH=$PATH_BACKUP
	export PYTHONPATH=$PYTHONPATH_BACKUP
    fi

}

# Determine PYTHON executable in ATLAS release
get_pybin () {

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        ATLAS_PYBIN_LOOKUP_PATH=$ATLAS_SOFTWARE

    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        ATLAS_PYBIN_LOOKUP_PATH=$VO_ATLAS_SW_DIR/prod/releases

    else
        echo "get_pybin not implemented"
    fi

    ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`

    if ( [ ! -z `echo $CMTCONFIG | grep slc5` ] ); then
	export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/*/bin/python | grep slc5 | grep 2.5 |  head -1)
    elif ( [ $ATHENA_MAJOR_RELEASE -gt 13 ] ); then
        export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/*/bin/python | grep 2.5 | head -1)
    elif ( [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] ); then
	export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/slc3_ia32_gcc323/bin/python | head -1)
    else
	export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/*/bin/python | grep slc5  |  head -1)
    fi

    # default PYTHON version of Athena
    dum=`which python`
    export pybin_alt=$dum

    dum=`echo $pybin | sed 's/bin\/python$/bin/g'`
    export pypath=$dum
    dum=`echo $pybin | sed 's/bin\/python$/lib/g'`
    export pyldpath=$dum

    # Determine python32 executable location 

    which python32; echo $? > retcode.tmp
    retcodepy=`cat retcode.tmp`
    rm -f retcode.tmp
    #if [ $retcode -eq 0 ] && [ -z `echo $ATLAS_RELEASE | grep 14.` ] ; then
    if [ $retcodepy -eq 0 ]; then
	export python32bin=`which python32`
    fi

}

# detecting the se type
detect_setype () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORIG=$1
    MY_PATH_ORIG=$2
    MY_PYTHONPATH_ORIG=$3

    if [ -e ganga-stage-in-out-dq2.py ]; then

        chmod +x ganga-stage-in-out-dq2.py
    
        LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
        PATH_BACKUP=$PATH
        PYTHONPATH_BACKUP=$PYTHONPATH
        export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
        export PATH=$MY_PATH_ORIG:$PATH_BACKUP
        export PYTHONPATH=$MY_PYTHONPATH_ORIG:$PYTHONPATH_BACKUP

        # Remove lib64/python from PYTHONPATH
	dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'lib64/python' | tr '\n' ':' `
	export PYTHONPATH=$dum

	if [ ! -z $python32bin ]; then
	    export GANGA_SETYPE=`$python32bin ./ganga-stage-in-out-dq2.py --setype`
	else
	    if [ -e /usr/bin32/python ]
	    then	
		export GANGA_SETYPE=`/usr/bin32/python ./ganga-stage-in-out-dq2.py --setype`
            else
		export GANGA_SETYPE=`./ganga-stage-in-out-dq2.py --setype`
	    fi
		
	fi
	if [ -z $GANGA_SETYPE ]; then
	    export GANGA_SETYPE=`$pybin ./ganga-stage-in-out-dq2.py --setype`
	fi
	if [ -z $GANGA_SETYPE ]; then
	    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
	    export PATH=$PATH_ORIG
	    export PYTHONPATH=$PYTHONPATH_ORIG
	    if [ ! -z $PATH_GLITE ]; then
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_GLITE
		export PATH=$PATH:$PATH_GLITE
		export PYTHONPATH=$PYTHONPATH:$PYTHONPATH_GLITE
	    fi
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
	    export GANGA_SETYPE=`./ganga-stage-in-out-dq2.py --setype`
	fi

        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
        export PATH=$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_BACKUP
    fi
}

# staging input data files
stage_inputs () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORIG=$1
    MY_PATH_ORIG=$2
    MY_PYTHONPATH_ORIG=$3

    # Unpack dq2info.tar.gz
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

    if [ -e input_files ]; then
	echo "Preparing input data ..."
        # DQ2Dataset
	if [ -e input_guids ] && [ -e ganga-stage-in-out-dq2.py ]; then
	    chmod +x ganga-stage-in-out-dq2.py
	    chmod +x dq2_get
	    LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
	    PATH_BACKUP=$PATH
	    PYTHONPATH_BACKUP=$PYTHONPATH
	    
            # store athena env in case TAG needs it
            export LD_LIBRARY_PATH_BACKUP_ATH=$LD_LIBRARY_PATH_BACKUP
            export PATH_BACKUP_ATH=$PATH_BACKUP
            export PYTHONPATH_BACKUP_ATH=$PYTHONPATH_BACKUP

	    export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
	    export PATH=$MY_PATH_ORIG:$PATH_BACKUP
	    export PYTHONPATH=$MY_PYTHONPATH_ORIG:$PYTHONPATH_BACKUP

            # Remove lib64/python from PYTHONPATH
	    dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'lib64/python' | tr '\n' ':' `
	    export PYTHONPATH=$dum

	    if [ ! -z $python32bin ]; then
		$python32bin ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
	    else
		if [ -e /usr/bin32/python ]; then
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
	    if [ $retcode -ne 0 ]; then
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
		export PATH=$PATH_ORIG
		export PYTHONPATH=$PYTHONPATH_ORIG
		if [ ! -z $PATH_GLITE ]; then
		    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_GLITE
		    export PATH=$PATH:$PATH_GLITE
		    export PYTHONPATH=$PYTHONPATH:$PYTHONPATH_GLITE
		fi
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
		./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
	    fi

	    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
	    export PATH=$PATH_BACKUP
	    export PYTHONPATH=$PYTHONPATH_BACKUP
	    
        # ATLASDataset, ATLASCastorDataset, ATLASLocalDataset
	elif [ -e ganga-stagein-lfc.py ]; then 
	    chmod +x ganga-stagein-lfc.py
	    ./ganga-stagein-lfc.py -v -i input_files; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	elif [ -e ganga-stagein.py ]; then
	    chmod +x ganga-stagein.py
	    ./ganga-stagein.py -v -i input_files; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	else
	    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
		retcode=410100
	    else
		if [ ! -e PoolFileCatalog.xml ]; then
		    echo "Adding files to PoolFileCatalog.xml"

		    pool_insertFileToCatalog.py `cat input_files` 2>/dev/null; echo $? > retcode.tmp
		    retcode=`cat retcode.tmp`
		    rm -f retcode.tmp
		    if [ n$USE_POOLFILECATALOG_FAILOVER == n'1' ] && ( [ $retcode -ne 0 ] || [ ! -e PoolFileCatalog.xml ] )
			then
			cat input_files | while read file
			  do
			  pool_insertFileToCatalog.py $file 2>/dev/null; echo $? > retcode.tmp
			  retcode=`cat retcode.tmp`
			  rm -f retcode.tmp
			done
		    fi
		    if ( [ $retcode -ne 0 ] || [ ! -e PoolFileCatalog.xml ] )
		    then
			echo "PoolFileCatalog.xml creation failed. Continuing..."
			retcode=0
		    fi		
		else
		    echo "PoolFileCatalog provided. Skipping..."
		    retcode=0
		fi
	    fi
	fi
    fi
}

# staging output files 
stage_outputs () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORIG=$1
    MY_PATH_ORIG=$2
    MY_PYTHONPATH_ORIG=$3

    if [ $retcode -eq 0 ]
    then
	echo '===================================='
        echo "Storing output data ..."

        if [ -e ganga-stage-in-out-dq2.py ] && [ -e output_files ] && [ ! -z $OUTPUT_DATASETNAME ]
        then
            chmod +x ganga-stage-in-out-dq2.py
            export DATASETTYPE=DQ2_OUT
            LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
            PATH_BACKUP=$PATH
            PYTHONPATH_BACKUP=$PYTHONPATH

	    export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
	    export PATH=$MY_PATH_ORIG:$PATH_BACKUP
	    export PYTHONPATH=$MY_PYTHONPATH_ORIG:$PYTHONPATH_BACKUP   
	    
	    
            # Remove lib64/python from PYTHONPATH
	    dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'lib64/python' | tr '\n' ':' `
	    export PYTHONPATH=$dum
	    
	    if [ ! -z $python32bin ]; then
		$python32bin ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
	    else
		if [ -e /usr/bin32/python ]
		    then
		    /usr/bin32/python ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
		else
		    ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
		fi
	    fi
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp

	    if [ $retcode -eq 139 ];
		then
		echo "!!!WARNING!!!   Caught segfault when running ganga-stage-in-out-dq2. Adapting return code..."
		if [ -e dq2_retcode.tmp ]; 
		    then
		    retcode=`cat dq2_retcode.tmp`
		fi
	    fi
	    rm -f dq2_retcode.tmp

            # Fail over
	    if [ $retcode -ne 0 ]; then
		$pybin ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
	    fi

	    if [ $retcode -eq 139 ];
		then
		echo "!!!WARNING!!!   Caught segfault when running ganga-stage-in-out-dq2. Adapting return code..."
		if [ -e dq2_retcode.tmp ]; 
		    then
		    retcode=`cat dq2_retcode.tmp`
		fi
	    fi
	    rm -f dq2_retcode.tmp

	    if [ $retcode -ne 0 ]; then
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG
		export PATH=$PATH_ORIG
		export PYTHONPATH=$PYTHONPATH_ORIG
		if [ ! -z $PATH_GLITE ]; then
		    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_GLITE
		    export PATH=$PATH:$PATH_GLITE
		    export PYTHONPATH=$PYTHONPATH:$PYTHONPATH_GLITE
		fi
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
		./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp

		if [ $retcode -eq 139 ];
		    then
		    echo "!!!WARNING!!!   Caught segfault when running ganga-stage-in-out-dq2. Adapting return code..."
		    if [ -e dq2_retcode.tmp ]; 
			then
			retcode=`cat dq2_retcode.tmp`
		    fi
		fi
		rm -f dq2_retcode.tmp
	    fi
	    
            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
            export PATH=$PATH_BACKUP
            export PYTHONPATH=$PYTHONPATH_BACKUP

        elif [ -n "$OUTPUT_LOCATION" -a -e output_files ]; then

            if [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then

	        # check for EOS use
		echo $OUTPUT_LOCATION
		case $OUTPUT_LOCATION in
		    root*) 
			echo "EOS output detected"
			OUTPUT_LOCATION=`echo ${OUTPUT_LOCATION} | sed 's/root://' | sed 's|//[a-z0-9]*/||'`
			echo "Changed output location to ${OUTPUT_LOCATION}"
			if [ -e $EOS_COMMAND_PATH ]
			    then
			    echo "Using EOS Command Path '${EOS_COMMAND_PATH}'"
			    MKDIR_CMD="${EOS_COMMAND_PATH} mkdir"
			    CP_CMD="${EOS_COMMAND_PATH} cp" 
			else    
			    TEST_CMD=`which eos 2>/dev/null`
			    if [ ! -z $TEST_CMD ]
				then
				MKDIR_CMD="eos mkdir"
				CP_CMD="eos cp"
			    else
				echo "Couldn't find EOS in PATH or set by environmnet. Defaulting to AFS install"
				MKDIR_CMD="/afs/cern.ch/project/eos/installation/pro/bin/eos.select mkdir"
				CP_CMD="/afs/cern.ch/project/eos/installation/pro/bin/eos.select cp"
			    fi
			fi
			;;
		    *)
			echo "Attempting rf* commands..."
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
			;;
		esac

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

            elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then

                LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
                PATH_BACKUP=$PATH
                PYTHONPATH_BACKUP=$PYTHONPATH
                export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORIG:/opt/globus/lib:$LD_LIBRARY_PATH_BACKUP
                export PATH=$MY_PATH_ORIG:$PATH_BACKUP
                export PYTHONPATH=$MY_PYTHONPATH_ORIG:$PYTHONPATH_BACKUP

                ## copy and register files with 3 trials
                cat output_files | while read filespec; do
                    for file in $filespec; do
                        $lcgcr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                        retcode=`cat retcode.tmp`
                        rm -f retcode.tmp
                        if [ $retcode -ne 0 ]; then
                            sleep 120
                            $lcgcr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                            retcode=`cat retcode.tmp`
                            rm -f retcode.tmp
                            if [ $retcode -ne 0 ]; then
                                sleep 120
                                $lcgcr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                                retcode=`cat retcode.tmp`
                                rm -f retcode.tmp
                                if [ $retcode -ne 0 ]; then
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
    fi
}

# prepare athena
prepare_athena () { 

    # Set timing command
    if [ -x /usr/bin/time ]; then
	export timecmd="/usr/bin/time -v"
    else
	export timecmd=time
    fi

    # parse the job options
    if [ $retcode -eq 0 ] || [ n$DATASETTYPE = n'DQ2_COPY' ]; then

        echo "Parsing jobOptions ..."
        if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
        then
            chmod +x ganga-joboption-parse.py
            ./ganga-joboption-parse.py ; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
        fi
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
} 

# run athena
run_athena () {

    job_options=$*

# run athena in regular mode =========================================== 
    if [ $retcode -eq 0 ]; then
        ls -al
        env | grep DQ2
        env | grep LFC
	env | grep DCACHE

	echo '====================='
	echo "DBRELEASE: $DBRELEASE"
	echo "DBRELEASE_OVERRIDE: $DBRELEASE_OVERRIDE"
	env | grep DBRelease
	echo '====================='

	echo 'input.py start ---------'
	cat input.py
	echo 'input.py end --------'
 
	export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_ORIG

	cat PoolFileCatalog.xml

        echo "Running Athena ..."


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

	if [ n$ATLAS_EXETYPE == n'ATHENA' ]
	    then 
	    
	    # if run dir given, change to that 
            if [ ! -z $ATLAS_RUN_DIR ]
                then
		old_run_dir=`pwd`
		echo "Changing dir from ${old_run_dir} to ${ATLAS_RUN_DIR}..."
		cp preJobO.py work/$ATLAS_RUN_DIR/.
		cp input.py work/$ATLAS_RUN_DIR/.
                cd work/$ATLAS_RUN_DIR   
		pwd
            fi

	    if [ n$RECEXTYPE == n'' ]
		then
		if [ -f preJobO.py ]; then
		    echo 'prepJobO.py start ---------'
		    cat preJobO.py 
		    echo 'prepJobO.py end --------'
		    $timecmd athena.py preJobO.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
		else
		    $timecmd athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
		fi
	    else

		if [ n$DATASETTYPE == n'FILE_STAGER' ]; 
		    then
		    FILESTAGER_JOS=`echo $ATHENA_OPTIONS | { read first next; echo $first ;}`
		    PARENT_JOS=`echo $ATHENA_OPTIONS | { read first next; echo $next ;}`
		else
		    FILESTAGER_JOS=''
 		    PARENT_JOS=$ATHENA_OPTIONS" evtmax.py"
 		fi

		$timecmd athena.py $FILESTAGER_JOS input.py $PARENT_JOS; echo $? > retcode.tmp
	    fi

	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp

	    if [ ! -z $ATLAS_RUN_DIR ]
                then
		echo "Reverting dir to ${old_run_dir}..."
                cd $old_run_dir
                pwd
            fi

	elif [ n$ATLAS_EXETYPE == n'PYARA' ]
	    then
	    $timecmd $pybin_alt $ATHENA_OPTIONS ; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	elif [ n$ATLAS_EXETYPE == n'ROOT' ]
	    then
	    $timecmd root -b -q $ATHENA_OPTIONS ; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	elif [ n$ATLAS_EXETYPE == n'EXE' ]
	    then
		
            # scan for %IN args
	    pwd
	    EXE_FILELIST=$(tr '\n' ',' < input.txt | sed 's/\//\\\//g' | sed s/,$//)  
	    echo $EXE_FILELIST
	    NEW_ATHENA_OPTIONS=`echo $ATHENA_OPTIONS | sed s/%IN/$EXE_FILELIST/`
	    echo "New EXE command line: "
	    echo $NEW_ATHENA_OPTIONS
	    export PATH=$PATH:.
	    $timecmd $NEW_ATHENA_OPTIONS ; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	elif [ n$ATLAS_EXETYPE == n'TRF' ] && [ -e trf_params ]
	    then
	    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ] 
		then
		source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh 
		export PATH=$pypath:$PATH
		export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH

                # Set DQ2_LOCAL_SITE_ID to db dataset location
		if [ -e db_dq2localid.py ]
		    then
		    export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
		    chmod +x db_dq2localid.py
		    ./db_dq2localid.py
		    export DQ2_LOCAL_SITE_ID=`cat db_dq2localid.txt`
		fi
		if [ ! -z $DBDATASETNAME ] && [ ! -z $DBFILENAME ]
		    then
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

			dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$DBFILENAME $DBDATASETNAME;  echo $? > retcode.tmp
			if [ -e $DBDATASETNAME/$DBFILENAME ]
			    then
			    mv $DBDATASETNAME/* .
			    echo successfully retrieved $DBFILENAME
			else
			    echo 'ERROR: dq2-get of DBRELEASE failed !'
			    echo 'Yet another retry with changed environment'
			    export PATH=$pypath:$PATH
			    export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
			    dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$DBFILENAME $DBDATASETNAME;  echo $? > retcode.tmp
			    if [ -e $DBDATASETNAME/$DBFILENAME ]
			    then
				mv $DBDATASETNAME/* .
				echo successfully retrieved $DBFILENAME
			    else
				echo 'ERROR: dq2-get of $DBDATASETNAME failed !'
				echo '1'>retcode.tmp
			    fi
			fi 
			export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
			export PATH=$PATH_BACKUP
			export PYTHONPATH=$PYTHONPATH_BACKUP
   		    fi
		fi
	    fi
	    ##
	    echo 'input.py start ---------'
	    cat input.py
	    echo 'input.py end -----------'
	    ##
	    cat - >parse_input_files.py <<EOF
#!/usr/bin/env python
lfns = [ line.strip() for line in file('input_files') ]
alllfns = ''
for lfn in lfns:
    alllfns = alllfns + lfn + ','
print alllfns[:-1]
EOF
	    chmod +x ./parse_input_files.py
	    $pybin ./parse_input_files.py > input_files.txt            
	    echo 'input_files.txt start ...'
	    cat input_files.txt
	    echo 'input_files.txt end ...'
	    ##
	    ls -rtla
            ## need to remove local link to db, or the dbrelease specified in the trf will not have any effect
	    rm -rf sqlite200/ALLP200.db
	    ## Parse inputfile string from transformation 
	    inputfile=`$ATHENA_OPTIONS | grep input | awk '{print $4}' | sed 's/<//' | sed 's/>//'`
	    echo 'TRF inputtype start ...'
	    echo $inputfile
	    echo 'TRF inputtype end ...'
	    ##
	    if [ ! -z $DBFILENAME ]
		then
		$timecmd $ATHENA_OPTIONS $inputfile'='`cat input_files.txt` `cat trf_params` 'dbrelease='$DBFILENAME ; echo $? > retcode.tmp
	    else
		$timecmd $ATHENA_OPTIONS $inputfile'='`cat input_files.txt` `cat trf_params`; echo $? > retcode.tmp
	    fi
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp

            # Rename output files
            cat output_files.new | while read outfiles
              do
              for ofile in $outfiles
                do
                mv $ofile ${ofile}.$I
                echo "${ofile}.$I" >> output_files.copy
              done
            done
	    if [ -e output_files.copy ]
		then
		mv output_files.new output_files.new.old
		mv output_files.copy output_files.new
	    fi
	else
	    if [ n$RECEXTYPE == n'' ]
		then
		if [ -f preJobO.py ]; then
		    echo 'prepJobO.py start ---------'
		    cat preJobO.py 
		    echo 'prepJobO.py end --------'
		    $timecmd athena.py preJobO.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
		else
		    $timecmd athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
		fi
	    else
		$timecmd athena.py input.py $ATHENA_OPTIONS evtmax.py; echo $? > retcode.tmp
	    fi

	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
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

	# DQ2 tracer report
	if [ -e dq2tracerreport.py ]
	    then
	    chmod +x dq2tracerreport.py
	    $pybin ./dq2tracerreport.py; echo $? > retcode.tmp
	    retcodetr=`cat retcode.tmp`
	    rm -f retcode.tmp
	    if [ $retcodetr -ne 0 ]; then
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
		./dq2tracerreport.py
	    fi
	fi
    fi
}

## routine for prepending FileStager_jobOption.py in the Athena job option list
prepend_filestager_joption() {
    export ATHENA_OPTIONS="FileStager_jobOption.py $ATHENA_OPTIONS"

    if [ -e athena_options ]; then
        ATHENA_OPTIONS_NEW=`cat athena_options`
        echo "FileStager_jobOption.py $ATHENA_OPTIONS_NEW" > athena_options
    fi
}

## routine for append FileStager_jobOption.py in the Athena job option list
append_filestager_joption() {
    export ATHENA_OPTIONS="$ATHENA_OPTIONS FileStager_jobOption.py "

    if [ -e athena_options ]; then
        ATHENA_OPTIONS_NEW=`cat athena_options`
        echo "$ATHENA_OPTIONS_NEW FileStager_jobOption.py" > athena_options
    fi
}

## routine for making file stager job option: input.py
make_filestager_joption() {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORIG=$1
    MY_PATH_ORIG=$2
    MY_PYTHONPATH_ORIG=$3

    # setting up the filestager copy wrapper with retry mechanism
    if [ -f fs-copy.py ]; then
        chmod +x fs-copy.py
    fi

    if [ -f make_filestager_joption.py ]; then
        chmod +x make_filestager_joption.py

        LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
        PATH_BACKUP=$PATH
        PYTHONPATH_BACKUP=$PYTHONPATH
        export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORIG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
        export PATH=$MY_PATH_ORIG:$PATH_BACKUP
        export PYTHONPATH=$MY_PYTHONPATH_ORIG:$PYTHONPATH_BACKUP

        # Remove lib64/python from PYTHONPATH
        #dum=`echo $PYTHONPATH | tr ':' '\n' | egrep -v 'lib64/python' | tr '\n' ':' `
        #export PYTHONPATH=$dum

        if [ ! -z $python32bin ]; then
            $python32bin ./make_filestager_joption.py; echo $? > retcode.tmp
        else
            if [ -e /usr/bin32/python ]; then
                /usr/bin32/python ./make_filestager_joption.py; echo $? > retcode.tmp
            else
                ./make_filestager_joption.py; echo $? > retcode.tmp
            fi
        fi
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp

        # Fail over
        if [ $retcode -ne 0 ]; then
            $pybin ./make_filestager_joption.py; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        fi

        if [ $retcode -ne 0 ]; then
            export LD_LIBRARY_PATH=$MY_LD_LIBRARY_PATH_ORIG
            export PATH=$MY_PATH_ORIG
            export PYTHONPATH=$MY_PYTHONPATH_ORIG
            if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]; then
                source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
            else
                if [ -e dq2info.tar.gz ]; then
                    tar xzf dq2info.tar.gz
		    export PYTHONPATH=$PWD:$PYTHONPATH
		    export DQ2_HOME=$PWD/opt/dq2
                fi
            fi
            ./make_filestager_joption.py; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        fi

        # at this stage, the "FileStager_jobOption.py" and "input.py" should be created.
        # prepend "FileStager_jobOption.py" to the list of user job options.
        # "input.py" will be treated later in run_athena.
        if [ n$ATLAS_EXETYPE == n'PYARA' ] || [ n$ATLAS_EXETYPE == n'ROOT' ] ; then
            #append_filestager_joption
	    echo "Not appending/prepending FileStager_jobOptions.py"
	else 
            prepend_filestager_joption
        fi

        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
        export PATH=$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_BACKUP
    fi

    return 0
}

## function for downloading/overriding the DBRelease
download_dbrelease() {

    retcode=0

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORIG=$1
    MY_PATH_ORIG=$2
    MY_PYTHONPATH_ORIG=$3

    if [ ! -z $ATLAS_DBRELEASE ] && [ ! -z $ATLAS_DBFILE ]
    then
        # Setup new dq2- tools
        if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]
            then
            source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
        else
            echo 'ERROR: DQ2Clients with dq2-get are not installed at the site - please contact Ganga support mailing list.'
            echo '1'>retcode.tmp
        fi
	export PATH=$pypath:$PATH
	export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH

        # Set DQ2_LOCAL_SITE_ID to db dataset location
        if [ -e db_dq2localid.py ]; then
            export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
            chmod +x db_dq2localid.py
	    
            if [ ! -z $python32bin ]; then
                $python32bin ./db_dq2localid.py; echo $? > retcode.tmp
            else
                if [ -e /usr/bin32/python ]; then
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
	
        if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]; then
            for ((i=1;i<=3;i+=1)); do
                echo Copying $ATLAS_DBFILE, attempt $i of 3
                dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$ATLAS_DBFILE $ATLAS_DBRELEASE;  echo $? > retcode.tmp
                if [ -e $ATLAS_DBRELEASE/$ATLAS_DBFILE ]; then
                    mv $ATLAS_DBRELEASE/* .
                    echo successfully retrieved $ATLAS_DBFILE
                    tar xzf $ATLAS_DBFILE
                    cd DBRelease/current/
                    python setup.py | grep = | sed -e 's/^/export /' > dbsetup.sh
                    source dbsetup.sh
                    cd ../../
                    break
                else
                    echo 'ERROR: dq2-get of DBRELEASE failed !'
                    echo 'Retry with changed environment'
		    
                    LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
                    PATH_BACKUP=$PATH
                    PYTHONPATH_BACKUP=$PYTHONPATH
		    
                    export LD_LIBRARY_PATH=$MY_LD_LIBRARY_PATH_ORIG
                    export PATH=$MY_PATH_ORIG
                    export PYTHONPATH=$MY_PYTHONPATH_ORIG
		    
                    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]; then
                        source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
                    fi
		    
                    dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$ATLAS_DBFILE $ATLAS_DBRELEASE;  echo $? > retcode.tmp
                    if [ -e $ATLAS_DBRELEASE/$ATLAS_DBFILE ]; then
                        mv $ATLAS_DBRELEASE/* .
                        echo successfully retrieved $ATLAS_DBFILE
                        tar xzf $ATLAS_DBFILE
                        cd DBRelease/current/
                        python setup.py | grep = | sed -e 's/^/export /' > dbsetup.sh
                        source dbsetup.sh
                        cd ../../
                        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
                        export PATH=$PATH_BACKUP
                        export PYTHONPATH=$PYTHONPATH_BACKUP
                        break
                    else
			echo 'ERROR: dq2-get of DBRELEASE failed !'
			echo 'Yet another retry with changed environment'
			export PATH=$pypath:$PATH
			export LD_LIBRARY_PATH=$pyldpath:$LD_LIBRARY_PATH
	
			dq2-get --client-id=ganga -L `cat db_dq2localid.txt` -d --automatic --timeout=300 --files=$ATLAS_DBFILE $ATLAS_DBRELEASE;  echo $? > retcode.tmp
			if [ -e $ATLAS_DBRELEASE/$ATLAS_DBFILE ]
			    then
			    mv $ATLAS_DBRELEASE/* .
			    echo successfully retrieved $ATLAS_DBFILE
			    tar xzf $ATLAS_DBFILE
			    cd DBRelease/current/
			    python setup.py | grep = | sed -e 's/^/export /' > dbsetup.sh
			    source dbsetup.sh
			    cd ../../
			    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
			    export PATH=$PATH_BACKUP
			    export PYTHONPATH=$PYTHONPATH_BACKUP
			    break
			else
			    echo 'ERROR: dq2-get of $ATLAS_DBRELEASE failed !'
			    echo '1'>retcode.tmp
			fi
		    fi
		    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
		    export PATH=$PATH_BACKUP
		    export PYTHONPATH=$PYTHONPATH_BACKUP
		fi
            done
	else
            echo 'ERROR: DQ2Clients with dq2-get are not installed at the site - please contact Ganga support mailing list.'
            echo '1'>retcode.tmp
        fi

        # Set DQ2_LOCAL_SITE_ID to dataset location
        if [ -e dq2localid.txt ]
            then
            export DQ2_LOCAL_SITE_ID=`cat dq2localid.txt`
            export DQ2_LOCAL_SITE_ID_BACKUP=$DQ2_LOCAL_SITE_ID
        fi
    fi

    echo '====================='
    echo "DBRELEASE: $DBRELEASE"
    echo "DBRELEASE_OVERRIDE: $DBRELEASE_OVERRIDE"
    env | grep DBRelease
    echo '====================='

    if [ -e retcode.tmp ]; then
        retcode=`cat retcode.tmp`
    fi
    
    return $retcode
}

## function for setting up athena runtime environment, no compilation
runtime_setup () {

    ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`
    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
    then
        source $SITEROOT/dist/$ATLAS_RELEASE/Control/AthenaRunTime/AthenaRunTime-*/cmt/setup.sh
    elif [ $ATHENA_MAJOR_RELEASE -gt 12 ]
    then
	if [ -z $ATLAS_PROJECT ]
	then
	    source $SITEROOT/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
	elif [ ! -z $ATLAS_PROJECT ]
	then
	    source $SITEROOT/${ATLAS_PROJECT}/$ATLAS_PRODUCTION/${ATLAS_PROJECT}RunTime/cmt/setup.sh
	fi

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

}

## copy sqlite files to local working directory
## this one has to be called after athena_setup
get_sqlite_files()
{
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

    ls -rtla
}  

## compile the packages
athena_compile()
{
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
    fi
    tar xzf $USER_AREA -C work

    if [ n$ATLAS_EXETYPE == n'EXE' ]; then
	mv work/* .
    else
	cd work
    fi
    pwd
    source install.sh; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
    if [ $retcode -ne 0 ]; then
        echo "*************************************************************"
        echo "*** Compilation warnings. Return Code $retcode            ***"
        echo "*************************************************************"
    fi
    pwd

    if [ ! n$ATLAS_EXETYPE == n'EXE' ]; then
	cd ..
    fi

    return $retcode
 
}

## Unpack access_info pickle
access_info()
{
    if [ -e access_info.pickle ]
    then
	export DATASETTYPE=`./access_info.py -t`
	export DQ2_LOCAL_PROTOCOL=`./access_info.py -p`
    fi

}
