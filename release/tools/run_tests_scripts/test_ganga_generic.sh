VERSION=$1.$2.$3-pre

rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]Releas\
eTesting=True' -o'Config=localxml.ini' Ganga/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga.out

rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]Releas\
eTesting=True' -o'Config=localxml.ini' GangaRobot/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaRobot.out
