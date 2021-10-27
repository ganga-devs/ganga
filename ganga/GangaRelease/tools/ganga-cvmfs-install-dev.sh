#!/bin/bash

cvmfs_server transaction ganga.cern.ch

cd /cvmfs/ganga.cern.ch/Ganga/install/DEV

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/sft.cern.ch/lcg/releases/LCG_100/Python/3.8.6/x86_64-centos7-gcc9-opt/lib

. bin/activate

pip install --upgrade git+https://github.com/ganga-devs/ganga.git@develop#egg=ganga[LHCb] 

deactivate

cd ~

cvmfs_server publish ganga.cern.ch

