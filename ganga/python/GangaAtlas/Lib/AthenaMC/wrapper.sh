#!/bin/sh


export T_RELEASE="${1}"
export T_SE="${2}"
export T_LOGFILE="${3}"
export T_TRF="$4"

if [ -e trfargs.conf ] ; then
export T_ARGS=`cat trfargs.conf`
echo $T_ARGS
else
shift 4
export T_ARGS="$*"
shift $#
fi


if [ -z "$BACKEND" ]; then
    echo "Error, backend was not transmitted properly, aborting"
    exit 15
fi

export LCG_GFAL_INFOSYS=$T_LCG_GFAL_INFOSYS
echo $X509_CERT_DIR
if [ -z $X509_CERT_DIR ] ; then
   export X509_CERT_DIR=/etc/grid-security/certificates
fi
echo $X509_CERT_DIR

echo "============================================="
echo ">>> PRINTING CURRENT DIRECTORY AND EVIRONMENT"
echo "============================================="
echo
/usr/bin/env date
echo
\ls
echo "--------------"
env
echo "--------------"
echo
# Work around for glite WMS spaced environement variable problem
if [ -e inputfiles.conf ] 
then
    INPUTFILES_NEW=`cat inputfiles.conf`
    if [ ! "$INPUTFILES_NEW" = "$INPUTFILES" ]
    then
	export INPUTFILES=$INPUTFILES_NEW	
    fi
fi
echo "INPUT FILES: $INPUTFILES"
if [ -e inputturls.conf ] 
then
    INPUTTURLS_NEW=`cat inputturls.conf`
    if [ ! "$INPUTTURLS_NEW" = "$INPUTTURLS" ]
    then
	export INPUTTURLS=$INPUTTURLS_NEW	
    fi
fi
if [ -e inputdsets.conf ] 
then
    INPUTDATASETS_NEW=`cat inputdsets.conf`
    if [ ! "$INPUTDATASETS_NEW" = "$INPUTDATASETS" ]
    then
	export INPUTDATASETS=$INPUTDATASETS_NEW	
    fi
fi

#if [ -e inputsites.conf ] 
#then
#    INPUTSITES_NEW=`cat inputsites.conf`
#    if [ ! "$INPUTSITES_NEW" = "$INPUTSITES" ]
#    then
#	export INPUTSITES=$INPUTSITES_NEW	
#    fi
#fi

# output data
if [ -e outputfiles.conf ] 
then
    OUTPUTFILES_NEW=`cat outputfiles.conf`
    if [ ! "$OUTPUTFILES_NEW" = "$OUTPUTFILES" ]
    then
	export OUTPUTFILES=$OUTPUTFILES_NEW	
    fi
fi
echo "OUTPUT FILES: $OUTPUTFILES"

# Working directory
T_HOMEDIR=${PWD}

if [ ! -z $TRANSFORM_ARCHIVE ] ; then
    echo "Fetching transform archive $TRANSFORM_ARCHIVE..."
    NCC=`wget --help | grep no-check-certificate | head -1`
    if [ -z "$NCC" ]; then
        wget $TRANSFORM_ARCHIVE
    else
        wget -nv --no-check-certificate $TRANSFORM_ARCHIVE
    fi
fi

JTARCHIVE=`ls | grep JobTransforms | head -1`
if [ -z "$JTARCHIVE" ]; then
    JTARCHIVE=`ls | grep AtlasProduction | head -1`
else
    isJT=JTARCHIVE
fi
if [ -z "$JTARCHIVE" -a -z "$PROD_RELEASE" -a -z "$T_RELEASE" ]; then
    echo "Missing or badly formatted transform archive, exiting"
    exit
fi

echo "ARCHIVE is $JTARCHIVE"
#mv $JTARCHIVE $T_TMPDIR
#cd ${T_TMPDIR}

# Unpack the JobTransforms
if [ ! -z "$JTARCHIVE" ]; then
tar xzf $JTARCHIVE
fi

if [ ! -z "$CUSTOM_JOB_OPTION" -a -e $T_HOMEDIR/$CUSTOM_JOB_OPTION ]; then
     echo "Copying custom job option: $CUSTOM_JOB_OPTION"
     cp $T_HOMEDIR/$CUSTOM_JOB_OPTION AtlasProduction/*/InstallArea/jobOptions/*[Jj]ob[Oo]ptions
fi

T_JTFLAGS=`echo $TRFLAGS | sed -e "s:/F:\-:g" -e "s:/W: :g"`
echo "TRF FLAGS: $T_JTFLAGS"

# Insert dry-run possibility
if [ ! -z "$DRYRUN" ]; then
   #if ((0==$(($RANDOM % 5)))); then exit 128; fi
   echo "./$T_TRF $T_JTFLAGS $T_ARGS"
   exit 0
fi


# 13.0.30 turnaround: saving local LCG setup as 13.0.30 breaks LCg tools with useless, obsolete stuff shipped in.
export LD_LIBRARY_PATH_SAVE=$LD_LIBRARY_PATH
export PATH_SAVE=$PATH
export PYTHONPATH_SAVE=$PYTHONPATH


# set up the release
if [ "$BACKEND" != "Local" ]; then
echo "## source $T_HOMEDIR/setup-release.sh"
source $T_HOMEDIR/setup-release.sh
fi

export LD_LIBRARY_PATH_SAVE2=$LD_LIBRARY_PATH
export PATH_SAVE2=$PATH
export PYTHONPATH_SAVE2=$PYTHONPATH
# restore LCG presetup (needed by DQ2 tools!)
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_SAVE
export PATH=$PATH_SAVE
export PYTHONPATH=$PYTHONPATH_SAVE
# stage-in input data 
echo "## source $T_HOMEDIR/stage-in.sh"
source $T_HOMEDIR/stage-in.sh
status=$?
echo "Listing input data"
ls -l
if [ $status -ne 0 ];then
echo "Error in stage-in, aborting"
exit $status
fi

# restore athena setup. 
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_SAVE2
export PATH=$PATH_SAVE2
export PYTHONPATH=$PYTHONPATH_SAVE2

echo $PYTHONPATH
echo $PATH
echo $LD_LIBRARY_PATH

echo
/usr/bin/env date
echo
echo "output SE is $T_SE"

#--------------------------------------------------------------------------
#          transformation script call
#--------------------------------------------------------------------------
#echo ">> ls $JTPATH/$T_TRF"
#\ls $JTPATH/$T_TRF 2>&1
which $T_TRF

echo
echo "======================="
echo "TRANSFORMATION STARTING"
echo "======================="
echo

if [ -z "$JTPATH" ]; then
    if [ -e "$T_TRF" ]; then
	chmod +x $T_TRF
	echo "./$T_TRF $T_JTFLAGS $T_ARGS"
	eval ./$T_TRF $T_JTFLAGS $T_ARGS
        retcode=$?
	echo $retcode > retcode.tmp
    else
	echo "$T_TRF $T_JTFLAGS $T_ARGS"
	eval $T_TRF $T_JTFLAGS $T_ARGS
        retcode=$?
	echo $retcode > retcode.tmp
    fi
else
   echo "$JTPATH/$T_TRF $T_JTFLAGS $T_ARGS"
   $JTPATH/$T_TRF $T_JTFLAGS $T_ARGS
   retcode=$?
   echo $retcode > retcode.tmp
fi

echo
echo "======================="
echo " END OF TRANSFORMATION"
echo "======================="
echo
trfstatus=`cat retcode.tmp`
echo "transform exit code is $trfstatus"
echo
/usr/bin/env date
echo
\ls -l 

if [ -z $isJT ]; then
    echo "LOGFILE is $T_LOGFILE"
    cat csc_*.log >> $T_LOGFILE
elif [ -f "log" ]; then
    mv log $T_LOGFILE
elif [ -f "joblog" ] ; then
    mv joblog $T_LOGFILE
fi
trflog=`echo $T_TRF | sed -e "s:\_trf\.py:\.log:"`
echo "adding $trflog to logfile"
cat $trflog >> $T_LOGFILE
# athena processing finished, we can restore the original set up (LCG tools) needed by stage-out.sh
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_SAVE
export PATH=$PATH_SAVE
export PYTHONPATH=$PYTHONPATH_SAVE
source $T_HOMEDIR/stage-out.sh
\ls -l 
pwd
echo $T_HOMEDIR

echo
echo "==============================="
echo "           END OF JOB          "
echo "==============================="

if [ ! -s "output_data" -a "$BACKEND" != "Local" ]; then
    echo "Output data missing, returning non-zero exit code"
    exit 128
fi

echo "returning transform exit code $trfstatus"
# disabling this as it results in a waste of CPU resources for returning a failure on harmless error messages from the transform.
#exit $trfstatus
