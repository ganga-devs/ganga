#!/bin/bash

cvmfs_server transaction ganga.cern.ch

cd /cvmfs/ganga.cern.ch/Ganga/install/DEV

. bin/activate

pip install --upgrade git+https://github.com/ganga-devs/ganga.git@develop 

deactivate

sed -i '1s/.*/#!\/usr\/bin\/env python/' bin/ganga

cd ~

cvmfs_server publish ganga.cern.ch

