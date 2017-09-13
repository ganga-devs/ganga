#VERSION=5.4.0-pre

VERSION=$1.$2.$3-pre

source /afs/cern.ch/sw/ganga/install/etc/setup-atlas-pre.sh ${VERSION}
#source /afs/cern.ch/sw/ganga/install/etc/setup-atlas.sh ${VERSION}

###rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaNG:GangaPanda:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaNG/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaNG.out &

rm -rf ~/gangadir_testing_GangaPanda ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaPanda:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaPanda/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaPanda.out

###note that GangaRobot tests are now run with the gangage account
####rm -rf ~/gangadir_testing_GangaRobot && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaRobot:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaRobot/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaRobot.out &

rm -rf ~/gangadir_testing_GangaAtlas ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaPanda:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaAtlas/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaAtlas.out

/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_gen.sh -f -v ${VERSION}
/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_test.sh -v ${VERSION}

