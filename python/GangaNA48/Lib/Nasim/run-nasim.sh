#!/usr/bin/env bash

# A wrapper script for running Nasim on the grid

#----------------------------------------------------
# functions first
print_debug() {
    # print a load of debug info
    echo "-----------------------------------------"
    echo "Printing debug info"
    ls -ltr 
    env
}

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Setting up environment to run Nasim"

export NA48_ROOT=/afs/cern.ch/user/n/na48grid/public
export NA48_USER=${NA48_ROOT}/nasim
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${NA48_ROOT}/lib
export COMPACT_SQL_DATABASE=${NA48_ROOT}/compact/database/database.db

export CDSERV=${NA48_ROOT}/hepdb

mv ${NA48_JOB_FILE} cmc007.job.old
mv ${NA48_TITLES_FILE} cmc007user.titles.old

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Altering Job options file"
echo
echo "Original version:"
echo
cat cmc007.job.old
echo
echo "Making the following changes:"
echo
echo "CDSERV change"
sed 's/set isslc4/if (\! \${\?CDSERV}) then\n&/g' cmc007.job.old | sed 's/\#\#\# Parse/endif\n&/g' > temp1
echo
echo "Link changes:"
sed 's/\/afs\/cern.ch\/na48\/offline\/mc\/beams2003\.pass8\.hbook/\$\{NA48_USER\}\/steerfile2003/g' temp1 | sed 's/\/afs\/cern.ch\/na48\/offline\/mc\/beams2004\.pass5\.hbook/\$\{NA48_USER\}\/steerfile2004/g' | sed 's/\/afs\/cern.ch\/na48\/offline\/mc\/beams2007\.pass2\.hbook/\$\{NA48_USER\}\/steerfile2007/g' | sed 's/\/afs\/cern.ch\/na48\/offline\/mc/\$\{NA48_USER\}/g' | sed 's/\/afs\/cern.ch\/na48\/offline2\/compact\/compact-7\.2\/compact\/GeomFiles\/kabes_cal.dat/\$\{NA48_USER\}\/fort\.71/g' > temp2

echo "Application Changes:"
sed 's/^BEAMTY ./BEAMTY '${NA48_BEAM_TYPE}'/g' temp2 | sed 's/^ISEEDG ../ISEEDG '${NA48_SEED}' /g' | sed 's/^TRIG/TRIG '${NA48_NUM_TRIGS}'\nC TRIG/g' > temp3

echo
echo "New Version:"
echo
mv temp3 cmc007.job
cat cmc007.job

echo "-----------------------------------------"
echo "Altering User Titles file"
echo
echo "Original version:"
echo
cat cmc007user.titles.old
echo
echo "Making the following changes:"
echo
echo "Shower Library changes"
export NEWPATH=`echo ${NA48_USER} | sed 's/\//\\\//g'`
echo ${NEWPATH}
echo ${NA48_USER} | sed 's/\//\\\//g' > newpath
export NEWPATH=`cat newpath`
echo ${NEWPATH}
sed 's/\/afs\/cern\.ch\/na48\/maxi97b/'${NEWPATH}'/g' cmc007user.titles.old > temp1
echo "Run Number change"
sed '/REST/c\                                 :REST    '${NA48_RUN_NUM}'. 0.' temp1 > cmc007user.titles
echo
echo "New Version:"
echo
cat cmc007user.titles

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Running Nasim"

chmod +x cmc007.job

./cmc007.job

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Storing Data"

python store_data.py

echo "All done!"

#----------------------------------------------------
print_debug

#----------------------------------------------------
