source /cvmfs/lhcb.cern.ch/lib/LbLogin.sh
export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini
export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config
ls -l .globus
lhcb-proxy-init
#SetupProject ganga
