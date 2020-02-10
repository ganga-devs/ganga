#!/bin/sh

CMSSW_VERSION=CMSSW_3_7_0
CMSSWHOME=xxx/crab/${CMSSW_VERSION}

cd ${CMSSWHOME}/src

#setup cmssw env
source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
source ${VO_CMS_SW_DIR}/cmsset_default.sh
cmsenv
source /afs/cern.ch/cms/ccs/wm/scripts/Crab/crab.sh

