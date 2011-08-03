REL=v$10$2r$3
VERSION=$1.$2.$3-pre
. SetupProject.sh Ganga ${REL}
rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]Releas\
eTesting=True' -o'Config=localxml.ini' GangaLHCb/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb.out
