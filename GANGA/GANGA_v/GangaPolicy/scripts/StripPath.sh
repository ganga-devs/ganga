scriptdir=$GANGAPOLICYROOT/scripts

tmpfile=`python $scriptdir/PathStripper.py --shell=sh --mktemp -e PATH -e LD_LIBRARY_PATH -e PYTHONPATH -e JOBOPTSEARCHPATH -e HPATH --dirac --ganga --root`
. $tmpfile
rm -f $tmpfile
unset tmpfile
unset scriptdir
