export PATH=/usr/bin:$PATH
REL=v$10$2r$3
VERSION=$1.$2.$3-pre
svn update ~/scripts
rm -rf ~/cmtuser
~/scripts/lhcb-prepare -p ${REL}
. SetupProject.sh Ganga ${REL}
/afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga -o'[Configuration]RUNTIME_PATH=GangaLHCb' /afs/cern.ch/user/g/gangage/ganga/release/tools/generate_template_file.gpi
