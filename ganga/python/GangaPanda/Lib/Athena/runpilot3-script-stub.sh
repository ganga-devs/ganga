#!/bin/bash
# $Id: runpilot3-script-stub.sh,v 1.1 2008-10-01 09:11:03 dvanders Exp $
#
function lfc_test() {
    echo -n "Testing LFC module for $1: "
    which $1 &> /dev/null
    if [ $? != "0" ]; then
        echo "No $1 found in path."
        return 1
    fi
    $1 <<EOF
import sys
try:
    import lfc
    print "LFC module imported ok."
except:
    print "Failed to import LFC module."
    sys.exit(1)
EOF
}

function find_lfc_compatible_python() {
    ## Try to figure out what python to run

    # We first look for a 32bit python in the ATLAS software area
    # This is usually more up to date than the OS version
    # (N.B. when LFC is released for 64 bit this may no longer work, unless the site
    #  has a 64 bit build of python from atlas.)
    pybin=$(ls -r $VO_ATLAS_SW_DIR/prod/releases/*/sw/lcg/external/Python/*/slc*_ia32_gcc*/bin/python | head -1)
    if [ -z "$pybin" ]; then
        echo "ERROR: No python found in ATLAS SW release - site is probably very broken"
    else
        pydir=${pybin%/bin/python}
        ORIG_PATH=$PATH
        ORIG_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
        PATH=$pydir/bin:$PATH
        LD_LIBRARY_PATH=$pydir/lib:$LD_LIBRARY_PATH
        lfc_test $pybin
        if [ $? = "0" ]; then
            return 0
        fi
        # Reset paths
        PATH=$ORIG_PATH
        LD_LIBRARY_PATH=$ORIG_LD_LIBRARY_PATH
    fi

    # Some 64bit sites put a 32bit python in our path, so we have to test
    # this explicitly
    pybin=python
    lfc_test $pybin
    if [ $? = "0" ]; then
        return 0
    fi

    # Now see if python32 exists
    pybin=python32
    lfc_test $pybin
    if [ $? == "0" ]; then
        return 0
    fi

    # Oh dear, we're doomed...
    echo "ERROR: Failed to find an LFC compatible python."
    echo "Going in with pybin=python - will probably fail..."
    pybin=python
}

function get_pilot() {
    # Try different methods of extracting the pilot
    #  1. uuencoded attachment of this script
    #  2. http from BNL, then svr017 (or a server of your own choice)

    # BNL tarballs have no pilot3/ directory stub, so we conform to that...
    mkdir pilot3
    cd pilot3

    extract_uupilot $1
    if [ $? = "0" ]; then
        return 0
    fi

    get_pilot_http
    if [ $? = "0" ]; then
        return 0
    fi

    echo "Could not get pilot code from any source. Self desctruct in 5..4..3..2..1.."
    return 1
}


function extract_uupilot() {
    # Try pilot extraction from this script
    echo Attempting to extract pilot from $1
    python - $1 <<EOF
import uu, sys
uu.decode(sys.argv[1])
EOF

    if [ ! -f pilot3.tgz ]; then
        echo "Error uudecoding pilot"
        return 1
    fi

    echo "Pilot extracted successfully"
    tar -xzf pilot3.tgz
    rm -f pilot3.tgz
    return 0
}


function get_pilot_http() {
    # If you define the environment variable PILOT_HTTP_SOURCES then
    # loop over those servers. Otherwise use BNL, with Glasgow as a fallback.
    if [ -z "$PILOT_HTTP_SOURCES" ]; then
        PILOT_HTTP_SOURCES="http://gridui02.usatlas.bnl.gov:25880/cache/pilotcode.tar.gz http://svr017.gla.scotgrid.ac.uk/factory/release/pilot3-svn.tgz"
    fi
    for source in $PILOT_HTTP_SOURCES; do
        echo "Trying to download pilot from $source..."
        curl --connect-timeout 30 --max-time 180 -sS $source | tar -xzf -
        if [ -f pilot.py ]; then
            echo "Downloaded pilot from $source"
            return 0
        fi
        echo "Download from $source failed."
    done
    return 1
}


## main ##

echo "This is pilot wrapper $Id: runpilot3-script-stub.sh,v 1.1 2008-10-01 09:11:03 dvanders Exp $"

# Check what was delivered
echo "Scanning landing zone..."
echo -n "Current dir: "
startdir=$(pwd)
echo $startdir
ls -l
me=$0
echo "Me and my args: $0 $@"
if [ ! -f $me ]; then
    echo "Trouble ahead - cannot find myself."
fi
echo

echo unset https_proxy HTTPS_PROXY
unset https_proxy HTTPS_PROXY

# For EGEE sites we should run in $EDG_WL_SCRATCH or $TMPDIR
# (TMPDIR is more standard, but EDG_WL_SCRATCH more specific and has 
#  historical precedent from the lcg-RB days)
if [ -n "$EDG_WL_SCRATCH" ]; then
    cd $EDG_WL_SCRATCH
elif [ -n "$TMPDIR" ]; then
    cd $TMPDIR
fi
templ=$(pwd)/condorg_XXXXXXXX
temp=$(mktemp -d $templ)
echo Changing work directory to $temp
cd $temp

# Try to get pilot code...
get_pilot $me
ls -l
if [ ! -f pilot.py ]; then
    echo "Problem with pilot delivery - failing after dumping environment"
fi


# Environment sanity check (useful for debugging)
echo "---- Host Environment ----"
uname -a
hostname
hostname -f
echo

echo "---- JOB Environment ----"
env | sort
echo

# Trouble with tags file in VO dir?
echo "---- VO SW Area ----"
ls -l $VO_ATLAS_SW_DIR/
echo
if [ -e $VO_ATLAS_SW_DIR/tags ]; then
  echo Tag file contents:
  cat $VO_ATLAS_SW_DIR/tags
else
  echo Error: Tags file does not exist: $VO_ATLAS_SW_DIR/tags
fi
echo

# Buggy LFC api? Symptom of old gLite release at site.
# Really site is broken, but one may want to work around this
hostname -f | egrep "this is turned off right now" &> /dev/null
if [ $? -eq 0 ]; then
    echo "Employing LFC workaround"
    wget http://trshare.triumf.ca/~rodwalker/lfc.tgz
    tar -zxf lfc.tgz
    export PYTHONPATH=`pwd`/lib/python:$PYTHONPATH
fi
# Set lfc api timeouts
export LFC_CONNTIMEOUT=60
export LFC_CONRETRY=2
export LFC_CONRETRYINT=60


echo "---- Searching for LFC compatible python ----"
find_lfc_compatible_python
echo "Using $pybin for python LFC compatibility"

# This is where the pilot rundirectory is - maybe left after job finishes
scratch=`pwd`

echo "My Arguments: $@"

# Prd server and pass arguments
cmd="$pybin pilot.py -d $scratch -l $scratch -q None -m false -p 25443 -w https://pandasrv.usatlas.bnl.gov $@"

echo cmd: $cmd
$cmd

echo
echo "Pilot exit status was $?"

# Now wipe out our temp run directory, so as not to leave rubbish lying around
echo "Now clearing run directory of all files."
cd $startdir
rm -fr $temp

# The end
exit

