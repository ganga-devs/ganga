VERSION=$1.$2.$3-pre

source /afs/cern.ch/sw/ganga/install/etc/setup-atlas.sh ${VERSION}
/afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga -o'[Configuration]RUNTIME_PATH=GangaAtlas:GangaNG:GangaPanda' /afs/cern.ch/user/g/gangage/ganga/release/tools/generate_template_file.gpi
