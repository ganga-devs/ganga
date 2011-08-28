VERSION=$1.$2.$3-pre

source /afs/cern.ch/sw/ganga/install/etc/setup-atlas.sh ${VERSION}


rm -rf ~/gangadir_testing && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaNG:GangaPanda:Ganga\
Test' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaNG/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/outp\
ut/GangaNG.out &

rm -rf ~/gangadir_testing_GangaPanda && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaNG:Ganga\
Panda:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaPanda/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/repor\
ts/latest/output/GangaPanda.out &

rm -rf ~/gangadir_testing_GangaAtlas && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=:::GangaAtlas:GangaNG:Ganga\
Panda:GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaAtlas/test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/repor\
ts/latest/output/GangaAtlas.out 


