#!/bin/bash

cvmfs_server transaction ganga.cern.ch

cd /cvmfs/ganga.cern.ch/Ganga/install/DEV

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/sft.cern.ch/lcg/releases/LCG_88/Python/2.7.13/x86_64-slc6-gcc62-opt/lib

. bin/activate

pip install --upgrade git+https://github.com/ganga-devs/ganga.git@develop 

deactivate

sed -i '1s/.*/#!\/usr\/bin\/env python2\.7/' bin/ganga

cd ~

cvmfs_server publish ganga.cern.ch

