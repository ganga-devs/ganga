#!/usr/bin/env bash

# A wrapper script for running Fluka on the grid
retcode=0

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
echo "Setting up environment to run Fluka"

export NA48_ROOT=${VO_NA48_SW_DIR}
export FLUKA=${NA48_ROOT}/fluka
export FLUPRO=$FLUKA

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
echo "Altering card file"
echo
echo "Original version:"
echo
cat ${NA48_CARD_FILE}
echo
echo "Making the following changes:"
echo
sed '/^RANDOMIZE/cRANDOMIZE        1.0       '${NA48_SEED}'.' ${NA48_CARD_FILE} | sed '/^START/cSTART    '${NA48_NUM_TRIGS}'.' > fluka_card.inp

echo
echo "New Version:"
echo
cat fluka_card.inp

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Running Fluka"

$FLUKA/flutil/rfluka -M 1 -N 0 fluka_card

ls -ltr

if [ $? -ne 0 ]; then
    echo "ERROR: Problems running fluka. Dodgy install?"
    exit 1
fi

if [ `ls -ltr | grep fort.40 | wc -l` -eq 0 ]; then
    echo "ERROR: No results files produced. Unhandlend exception in FLUKA."
    retcode=1
fi

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "-----------------------------------------"
echo "Storing Data"

python fluka_store_data.py
if [ $? -ne 0 ]; then
    echo "ERROR: Problems storing data"
    exit 1
fi

#----------------------------------------------------
print_debug

#----------------------------------------------------
echo "All done!"
exit ${retcode}

