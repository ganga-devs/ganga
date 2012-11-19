#!/bin/sh

do_CERN_setup(){
    echo "checking CERN AFS setup"
    athena=`which athena.py`
    if [ -z "$athena" ]; then
	echo "athena not found, please check your setup!"
	exit 3
    fi
    return
}

function do_KIT_setup(){
    # ATLAS_ROOT setting for LCG (change them if necessary)
    if [ ! -z "${VO_ATLAS_SW_DIR}" ]; then 
	export ATLAS_ROOT=${VO_ATLAS_SW_DIR}/software/${T_RELEASE}
    else
	export ATLAS_ROOT=$SITEROOT
    fi
    if [ -z "${ATLAS_ROOT}" ]; then
	echo "Error, could not set ATLAS_ROOT: SITEROOT and VO_ATLAS_SW_DIR are unset. Aborting"
	exit 22
    fi

    echo "data"
    echo ${ATLAS_ROOT} 
    echo $SITEROOT 
    echo ${T_RELEASE} 
    echo $CMTSITE
    echo "end data"
    # Setup the Distribution Kit
    if [ ! -z `echo $T_RELEASE | grep 16.` -o ! -z `echo $PROD_RELEASE | grep 16.` ]; then
	if [ -z "$PROD_RELEASE" ] ; then
	    echo "base release setup:"
	    echo "source $ATLAS_ROOT/cmtsite/asetup.sh $T_RELEASE,32,setup"
	    source $ATLAS_ROOT/cmtsite/asetup.sh AtlasOffline,$T_RELEASE,32,setup
	   else
	    echo "prod patch requested"
	    source $ATLAS_ROOT/cmtsite/asetup.sh $PROD_RELEASE,32,setup # AtlasProduction not needed?
        fi
	echo "Done 16.0.X setup"
        return # no need to do more: $PWD is already in CMTPATH
    else 
	echo "source ${ATLAS_ROOT}/setup.sh"        
	source ${ATLAS_ROOT}/setup.sh   
	if [ -e "${ATLAS_ROOT}/setup-release.sh" ]; then
	    echo "source ${ATLAS_ROOT}/setup-release.sh"
	    if [ -z "$PROD_RELEASE" ] ; then
		source ${ATLAS_ROOT}/setup-release.sh
	    else
		source ${ATLAS_ROOT}/setup-release.sh -tag=$PROD_RELEASE,AtlasProduction,opt,runtime # this should be enough, assuming that it is working
	    fi 
	fi
    fi

    [ "$GCC_SITE" == "" ] && export GCC_SITE=${ATLAS_ROOT}/gcc-alt-3.2
    export PATH=${GCC_SITE}/bin:${PATH}
    export LD_LIBRARY_PATH=${GCC_SITE}/lib:${LD_LIBRARY_PATH}
    
    if [ $ATLRMAIN -lt 11 -o $T_RELEASE \< '11.5.0' ]; then
	echo "setting up monolithic release"
	export T_DISTRT=`ls -d ${ATLAS_ROOT}/dist/${T_RELEASE}/Control/AthenaRunTime/*`
	if [ -z "$T_DISTRT" ] ; then
	    echo "No AthenaRunTime environment found, aborting"
	    exit 123
        fi
	echo "## Setting up the release:"
	echo "## source ${T_DISTRT}/cmt/setup.sh"
	source $T_DISTRT/cmt/setup.sh 
	export PATH=${ATLAS_ROOT}/dist/${T_RELEASE}/InstallArea/share/bin:$PATH
	printenv | grep PATH
	echo "Athena is `which athena.py`"
	ATHENA=`which athena.py`
	if [ -z $ATHENA ]; then
	exit 
	fi
	POOL_home="`(cd $T_DISTRT/cmt; cmt show macro_value POOL_home)`"
	echo $POOL_home
	export PATH=$POOL_home/bin:$PATH
	
    else
	# again, this is all kit...
	echo "setting up project based release"
	export T_DISTRT=$ATLAS_ROOT/AtlasOffline/${T_RELEASE}/AtlasOfflineRunTime
	if [ -z "$T_DISTRT" ] ; then
	    echo "No AthenaRunTime environment found, aborting"
	    exit 123
        fi
	echo "## Setting up the release:"
	if [ -z "$PROD_RELEASE" ] ; then
	    echo "## source $ATLAS_ROOT/cmtsite/setup.sh"
	    source $ATLAS_ROOT/cmtsite/setup.sh
	else
	    echo "## source $ATLAS_ROOT/cmtsite/setup.sh -tag=$PROD_RELEASE,AtlasProduction,opt,runtime"
	    source $ATLAS_ROOT/cmtsite/setup.sh -tag=$PROD_RELEASE,AtlasProduction,opt,runtime
	    printenv
	    if [ ! -z "$SITEROOT" ]; then
		if [ -e "$SITEROOT/AtlasProduction/$PROD_RELEASE" ]; then
		    echo "source $SITEROOT/AtlasProduction/$PROD_RELEASE/AtlasProductionRunTime/cmt/setup.sh"
		    source $SITEROOT/AtlasProduction/$PROD_RELEASE/AtlasProductionRunTime/cmt/setup.sh
		else
		    echo "source $SITEROOT/AtlasProduction/${T_RELEASE}/AtlasProductionRunTime/cmt/setup.sh"
		    source $SITEROOT/AtlasProduction/${T_RELEASE}/AtlasProductionRunTime/cmt/setup.sh
		fi 
	    fi
	fi
    fi

}

## function for setting up frontier configurations properly
frontier_setup() {
    if [ -e $VO_ATLAS_SW_DIR/local/setup.sh ]; then
        source $VO_ATLAS_SW_DIR/local/setup.sh
    elif [ ! -z "$CMTSITE" -a "$BACKEND" != "LCG"  ]; then 
	if [[ $DQ2_LOCAL_SITE_ID == DESY-HH* ]] || [[ $DQ2_LOCAL_SITE_ID == DESY-ZN* ]]; then
	    if [ -e /afs/naf.desy.de/group/atlas/software/conditions/local/setup.sh ]; then 
		source /afs/naf.desy.de/group/atlas/software/conditions/local/setup.sh
	    fi
	fi
    fi

    echo "==  Frontier + ATLAS_POOLCOND_PATH setup  =="
    if [ -z "$ATLAS_POOLCOND_PATH" ]; then
	echo 'ATLAS_POOLCOND_PATH env not set'
	PFCFAILOVER=1
    elif [ ! -f $ATLAS_POOLCOND_PATH/poolcond/PoolFileCatalog.xml ]; then
	echo "$ATLAS_POOLCOND_PATH/poolcond/PoolFileCatalog.xml does not exist"
	PFCFAILOVER=1
    else
	echo "ATLAS_POOLCOND_PATH: $ATLAS_POOLCOND_PATH"
	PFCFAILOVER=0
    fi

    if [ $PFCFAILOVER -eq 1 ]; then
	echo 'Failing over to http backup for CD PFC'
	mkdir poolcond
	wget --timeout=60 -O poolcond/PoolFileCatalog.xml http://voatlas62.cern.ch/conditions/PoolFileCatalog.xml
	export ATLAS_POOLCOND_PATH=`pwd`
	echo "ATLAS_POOLCOND_PATH: $ATLAS_POOLCOND_PATH"
    fi


    if [ -n $FRONTIER_SERVER ]; then
	echo 'FRONTIER_SERVER : ' $FRONTIER_SERVER
    else
	echo 'ERROR: FRONTIER_SERVER not set !' 
    fi
    echo "===="
    echo 


}


### MAIN ###################################################################

#### release setup should not depend upon the input archive, but the release number!!!
if [ -z "$T_RELEASE" ] ; then
echo "ERROR, T_RELEASE unset"
exit 4
fi
echo $CMTSITE
echo $BACKEND
export ATLRMAIN=`echo $T_RELEASE | sed -e "s:\..*::" `

if [ ! -z "$CMTSITE" -a "$BACKEND" != "LCG" ]; then 
# CERN AFS setup for Cern Local and LSF backends. Do not use for LCG backend as we are expected to use the kit on the grid.
    do_CERN_setup
else
    do_KIT_setup
fi

frontier_setup

export JTPATH=""
if [ ! -z "$isJT" ]; then 
    echo "Using JobTransforms"
    export JTPATH=`ls -d ${PWD}/JobTransforms/JobTransforms-*/share`
elif [ -e "AtlasProduction" ]; then
    echo "Using user Python transforms on core release"
    # specific to AtlasProduction archive setup
    which cmt
    reldir=`ls AtlasProduction`
    echo "local release directory is $reldir"
    if [ ! -e "AtlasProduction/$reldir/AtlasProductionRunTime/cmt/requirements" ]; then
	echo "requirements file missing in archive. Please check that AtlasProductionRunTime/cmt/requirements is in your archive or get it from the relevant AtlasProduction*noarch.tar.gz tarball"
	exit 25
    fi
    cd AtlasProduction/$reldir/AtlasProductionRunTime/cmt
    cmt config
    cd $T_HOMEDIR
    export CMTPATH=AtlasProduction/$reldir:$CMTPATH
    source AtlasProduction/$reldir/AtlasProductionRunTime/cmt/setup.sh
    echo "CMTPATH is now $CMTPATH"
    echo "********"

elif [ -e "AtlasTier0" ]; then
    echo "Using user Python transforms on core release"
    # specific to AtlasTier0 archive setup
    which cmt
    reldir=`ls AtlasTier0`
    echo "local release directory is $reldir"
    if [ ! -e "AtlasTier0/$reldir/AtlasTier0RunTime/cmt/requirements" ]; then
	echo "requirements file missing in archive. Please check that AtlasTier0RunTime/cmt/requirements is in your archive or get it from the relevant AtlasTier0*noarch.tar.gz tarball"
	exit 25
    fi
    cd AtlasTier0/$reldir/AtlasTier0RunTime/cmt
    cmt config
    cd $T_HOMEDIR
    source AtlasTier0/$reldir/AtlasTier0RunTime/cmt/setup.sh
    echo "CMTPATH is now $CMTPATH"
    echo "********"
fi
if [ ! -z "$USER_AREA" ]; then
   tar xzf $USER_AREA
   export CMTPATH=$PWD:$CMTPATH
   echo '**********************************************************'
   echo 'CMTPATH = ' $CMTPATH
   echo 'PYTHONPATH = '$PYTHONPATH
   echo '**********************************************************'

    cmt config
    cmt broadcast source setup.sh
    cmt broadcast cmt config
    source setup.sh

    echo '**********************************************************'
    echo 'After User setup:'
    echo 'CMTPATH = ' $CMTPATH
    echo 'PYTHONPATH = '$PYTHONPATH
    echo 'LD_LIBRARY_PATH = '$LD_LIBRARY_PATH
    echo '**********************************************************'

fi
if [ ! -z "$ATLASDBREL" ]; then
    echo "Setting requested DB release. First looking for local instances"
    DBpath=`dirname $DB_location`
    echo "DB path is $DBpath"
    if [ -e "$DBpath/$ATLASDBREL" ]; then
	echo "found local instance: $DBpath/$ATLASDBREL"
	echo "Starting local setup"
	export newDB_location="$DBpath/$ATLASDBREL"
	export newDATAPATH=`echo $DATAPATH | sed -e "s:$DB_location\:::"`
	export newDATAPATH=$newDB_location:$newDATAPATH
	export DATAPATH=$newDATAPATH
	export DBRELEASE=$ATLASDBREL
	export DBRELEASE_REQUESTED=$ATLASDBREL
	export DBRELEASE_REQUIRED=$ATLASDBREL
	export CORAL_AUTH_PATH=$newDB_location/XMLConfig
	export CORAL_DBLOOKUP_PATH=$newDB_location/XMLConfig
	export TNS_ADMIN=$newDB_location/oracle-admin
	export newArgs=`echo $T_ARGS | sed -e "s:DBRelease=DBRelease-${ATLASDBREL}.tar.gz::"`
	export T_ARGS=$newArgs
    else
	echo "No local instance found, will have to download archive"
    fi
fi
cd $T_HOMEDIR

