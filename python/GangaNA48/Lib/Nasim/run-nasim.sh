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

export NA48_ROOT=${VO_NA48_SW_DIR}
export NA48_USER=${NA48_ROOT}/nasim
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${NA48_USER}/lib
export COMPACT_SQL_DATABASE=${NA48_ROOT}/compact/database/database.db

export CDSERV=${NA48_ROOT}/hepdb

mv ${NA48_JOB_FILE} cmc007.job.old
mv ${NA48_TITLES_FILE} cmc007user.titles.old

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Some sanity checks..."

if [ n${NA48_DATASET_NAME} = n ]; then
    echo "ERROR: No Dataset name specified. Exiting..."
    exit 1
fi
    
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
echo ${VO_NA48_SW_DIR} | sed 's/\//\\\//g' > newpath
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
if [ $? -ne 0 ]; then
    echo "ERROR: Problems running cmc. Dodgy install?"
    exit 1
fi

if [ `ls -ltr | grep mcrun | wc -l` -eq 0 ]; then
    echo "ERROR: No MC files produced. Unhandlend exception in Nasim."
    exit 2
fi

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Storing Data"

python store_data.py
if [ $? -ne 0 ]; then
    echo "ERROR: Problems storing data"
    exit $?
fi

echo "All done!"

#----------------------------------------------------
print_debug

#----------------------------------------------------
