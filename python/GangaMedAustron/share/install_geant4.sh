#!/bin/sh
#
# Install GEANT4 in current directory
#
# Dietrich Liko, August 2009

directory=$(cd `dirname $0` && pwd)

tarball="geant4-9.2.p01-slc4-medaustron.tbz"
srmurl="srm://hephyse.oeaw.ac.at:8446/srm/managerv2?SFN=/dpm/oeaw.ac.at/home/cms/store/user/liko"

lcg-cp -t 600 -v -b -D srmv2 "$srmurl/$tarball" $tarball
if [ $? -ne 0 ]
then
   echo "install_geant4: Downloading tarball with lcg-cp failed."
   exit 9
fi

md5sum -c $directory/MD5SUM
if [ $? -ne  0 ]
then
   echo "install_geant4: MD5SUM of downloaded tarball wrong"
   exit 9
fi

tar xjvf $tarball

if [ `uname -m` = "x86_64" ]
then
   echo "Discovered 64bit architrecture. Patching build files."
   patch medaustron/dirGeant4-9.2.p01/config/architecture.gmk $directory/patch1
   patch medaustron/dirGeant4-9.2.p01/config/sys/Linux-g++.gmk $directory/patch2
fi
