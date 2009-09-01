#! /bin/sh -x
#
# GangaMedAustron - Dietrich Liko, August 2009
#
# Install the GEANT tarball for MedAustron

if [ -z $VO_VOCE_SW_DIR ]
then
   echo "install: VO_VOCE_SW_DIR is not defined."
   exit 9
fi

if [ ! -d $VO_VOCE_SW_DIR ]
then
   echo "install: VOCE SW directory does not exist"
   exit 9
fi

uuid=`uuidgen`
touch $VO_VOCE_SW_DIR/$uuid
if [ $? -ne 0 ]
then
   echo "install: VOCE SW directory not writeable"
   exit 9
fi

# Clean up old installation

if [ -e $VO_VOCE_SW_DIR/medaustron ]
then
   rm -rf $VO_VOCE_SW_DIR/medaustron
fi

./install_geant4 $VO_VOCE_SW_DIR
