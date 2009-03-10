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

### MAIN ###################################################################

#### release setup should not depend upon the input archive, but the release number!!!
if [ -z "$T_RELEASE" ] ; then
echo "ERROR, T_RELEASE unset"
exit 4
fi
echo $CMTSITE

export ATLRMAIN=`echo $T_RELEASE | sed -e "s:\..*::" `

if [ "$CMTSITE" == "CERN" ]; then 
# CERN AFS setup for Cern Local and LSF backends.
    do_CERN_setup
else
    do_KIT_setup
fi
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
cd $T_HOMEDIR

echo $T_JTFLAGS

