#!/bin/sh -x
#
# Run GEANT4 on grid site   - Dietrich.Liko@cern.ch
#
tarball=$1
directory=$2
macro=$3

# setup GEANT4. If there is no installation, perfom it on the fly

if [ -z $G4INSTALLATION_DIR ]
then 
   if [ ! -e $VO_VOCE_SW_DIR/medaustron/setup.sh ]
   then
      tarinstall="geant4-9.2.p01-slc4-medaustron.tbz"
      srmurl="srm://hephyse.oeaw.ac.at:8446/srm/managerv2?SFN=/dpm/oeaw.ac.at/home/cms/store/user/liko"
      lcg-cp \
         -v --connect-timeout 10 --srm-timeout 120 --sendreceive-timeout 200 \
         -b -D srmv2 "$srmurl/$tarinstall" $tarinstall
      if [ $? -ne 0 ]
      then
         echo "gridrun_geant4: Downloading tarball with lcg-cp failed."
         exit 9
      fi
      md5sum -c <<EOF
11d2e5a508a6068da962f8b5255ae5e7  geant4-9.2.p01-slc4-medaustron.tbz
EOF
      if [ $? -ne  0 ]
      then
         echo "gridrun_geant4: MD5SUM of tarball wrong"
         exit 9
      fi
      tar xjvf $tarinstall
      export VO_VOCE_SW_DIT=$PWD
   fi 
   if [ `uname -m` = "x86_64" ]
   then 
      echo "Setting up GEANT4 on 64 bit in 32bit mode."
      source $VO_VOCE_SW_DIR/medaustron/setup64.sh
   else
      echo "Setting up GEANT4 on 32 bit."
      source $VO_VOCE_SW_DIR/medaustron/setup.sh
   fi

#  fix the enviroment

   unset G4LIB_BUILD_SHARED
   export LD_LIBRARY_PATH=$G4AIDA_DIR/lib:$G4CLHEP_DIR/lib:$LD_LIBRARY_PATH
fi

# prepare the user area

if [ ! -e $tarball ]
then
   echo "Tarball $tarball does not exist." >&2
   exit 9
fi

tar xvzf $tarball

if [ ! -d $directory ]
then
   echo "Directory $directory does not exist." >&2
   exit 9
fi

if [ ! -e $directory/GNUmakefile ]
then
   echo "File $directory/GNUmakefile does not exist." >&2
   exit 9
fi

name=`awk '/name :=/ { print $3 }' $directory/GNUmakefile`

echo "Binary is called $name" >&2

# build the software

make -C $directory clean
make -C $directory all

# run the software

if [ ! -e $directory/$macro ]
then
   echo "Macro $directory/$macro not found" >$2
   exit 9
fi

time $G4WORKDIR/bin/Linux-g++/$name $directory/$macro

