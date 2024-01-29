#!/bin/bash

cvmfs_server transaction ganga.cern.ch

conda activate ganga

cd /cvmfs/ganga.cern.ch/Ganga/install

python -m venv $1

. $1/bin/activate

pip install --upgrade pip setuptools

pip install ganga[LHCb,Dirac]@git+https://github.com/ganga-devs/ganga.git@$1

deactivate

conda deactivate

rm -f /cvmfs/ganga.cern.ch/Ganga/install/LATEST

ln -s /cvmfs/ganga.cern.ch/Ganga/install/$1 /cvmfs/ganga.cern.ch/Ganga/install/LATEST

cd ~

cvmfs_server publish ganga.cern.ch


