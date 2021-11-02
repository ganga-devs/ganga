#!/bin/bash

cvmfs_server transaction ganga.cern.ch

cd /cvmfs/ganga.cern.ch/Ganga/install/DEV

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/sft.cern.ch/lcg/releases/LCG_100/Python/3.8.6/x86_64-centos7-gcc9-opt/lib

. bin/activate

pip install --upgrade git+https://github.com/ganga-devs/ganga.git@develop#egg=ganga[LHCb] 


sed -i "23i\
lib_string = '/cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-gcc9-opt/lib64:/cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-gcc9-opt/lib:/cvmfs/sft.cern.ch/lcg/releases/gcc/9.2.0-afc57/x86_64-centos7/lib:/cvmfs/sft.cern.ch/lcg/releases/gcc/9.2.0-afc57/x86_64-centos7/lib64'\n\
sys.path.append('/cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-gcc9-opt/lib/python3.8/site-packages')\n\
if not 'LD_LIBRARY_PATH' in os.environ.keys():\n\
    os.environ['LD_LIBRARY_PATH'] = lib_string\n\
    os.execv(sys.argv[0], sys.argv)\n\
elif not lib_string in os.environ['LD_LIBRARY_PATH']:\n\
    os.environ['LD_LIBRARY_PATH'] += ':'+lib_string\n\
    os.execv(sys.argv[0], sys.argv)" bin/ganga

deactivate

cd ~

cvmfs_server publish ganga.cern.ch

