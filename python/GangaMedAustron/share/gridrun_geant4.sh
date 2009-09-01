#!/bin/sh -x
#
# Run GEANT4 on grid site   - Dietrich.Liko@cern.ch

tarball=$1
macro=$2

# setup GEANT4. If there is no installation, perfom it on the fly

if [ -z $G4INSTALLATION_DIR ]
then
   if [ ! -e $VO_VOCE_SW_DIR/medaustron/setup.sh ]
   then
      ./install_geant4.sh $PWD
      if [ $? -ne 0 ]
      then
         echo "Installation failed"
         exit 9
      fi
      export VO_VOCE_SW_DIR=$PWD
   fi 
   source $VO_VOCE_SW_DIR/medaustron/setup.sh

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

package=`basename $tarball .tar.gz`

if [ ! -d $package ]
then
   echo "Package $package not found." >&2
   exit 9
fi

if [ ! -e $package/GNUmakefile ]
then
   echo "File $package/GNUmakefile does not exist." >&2
   exit 9
fi

binary=`awk '/name :=/ { print $3 }' $package/GNUmakefile`
if [ $? -ne 0 ]
then
   echo "Could not extract the name of the binary from GNUmakefile." >&2
   exit 9
fi

echo "Binary is called $binary" >&2

# build the software

make -C $package clean
make -C $package all

# run the software

if [ ! -e $macro ]
then
   echo "Macro $macro not found" >$2
   exit 9
fi

time $G4WORKDIR/bin/Linux-g++/$binary $macro

ls -l 
