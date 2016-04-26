
VERSION=$1.$2.$3-pre

rm /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_{1,2,3,4,5,6,7}.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/Bugs 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_1.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/GPI 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_2.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/Internals 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_3.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/Performance 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_4.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/Regression 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_5.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/Schema 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_6.out

rm -rf ~/gangadir_testing ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' Ganga/old_test/XMLRepository 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_7.out

rm /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga.out
cat /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga_{1,2,3,4,5,6,7}.out >> /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/Ganga.out





####rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaRobot/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaRobot.out
rm -rf ~/gangadir_testing_robot ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaRobot/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaRobot.out

/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_gen.sh -f -v ${VERSION}
/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_test.sh -v ${VERSION}

