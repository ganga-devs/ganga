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
rm $VO_VOCE_SW_DIR

tarball="geant4-9.2.p01-slc4-medaustron.tbz"
srmurl="srm://hephyse.oeaw.ac.at:8446/srm/managerv2?SFN=/dpm/oeaw.ac.at/home/cms/store/user/liko"

lcg-cp \
   -v --connect-timeout 10 --srm-timeout 120 --sendreceive-timeout 200 \
   -b -D srmv2 "$srmurl/$tarball" $tarball
if [ $? -ne 0 ]
then
   echo "install: Downloading tarball with lcg-cp failed."
   exit 9
fi

md5sum -c <<EOF
11d2e5a508a6068da962f8b5255ae5e7  geant4-9.2.p01-slc4-medaustron.tbz
EOF
if [ $? -ne  0 ]
then
   echo "install: MD5SUM of tarball wrong"
   exit 9
fi

# Clean up old installation

if [ -e $VO_VOCE_SW_DIR/medaustron ]
then
   rm -rf $VO_VOCE_SW_DIR/medaustron
fi

tar xjvf $tarball -C $VO_VOCE_SW_DIR

