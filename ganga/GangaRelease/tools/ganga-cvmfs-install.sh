#!/bin/bash

cvmfs_server transaction ganga.cern.ch

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/sft.cern.ch/lcg/releases/LCG_88/Python/2.7.13/x86_64-slc6-gcc62-opt/lib

cd /cvmfs/ganga.cern.ch/Ganga/install

virtualenv -p /cvmfs/sft.cern.ch/lcg/releases/LCG_88/Python/2.7.13/x86_64-slc6-gcc62-opt/bin/python $1

. $1/bin/activate

pip install --upgrade pip setuptools

pip install git+https://github.com/ganga-devs/ganga.git@$1 

deactivate

sed -i '1s/.*/#!\/usr\/bin\/env python2\.7/' $1/bin/ganga

rm -f /cvmfs/ganga.cern.ch/Ganga/install/LATEST

ln -s /cvmfs/ganga.cern.ch/Ganga/install/$1 /cvmfs/ganga.cern.ch/Ganga/install/LATEST

cd ~

cvmfs_server publish ganga.cern.ch


