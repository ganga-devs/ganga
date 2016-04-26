
export PATH=/usr/bin:$PATH
REL=v$10$2r$3
VERSION=$1.$2.$3-pre

echo "Preparing Ganga code"
## get latest lhcb-prepare script from git
rm -fr $HOME/lhcbsetupproject
git clone --quiet https://github.com/ganga-devs/lhcbsetupproject.git $HOME/lhcbsetupproject
## Copied from old create config file script
rm -fr $HOME/cmtuser
## Run lhcb-prepare to Setup this version of Ganga for SetupProject use!
python $HOME/lhcbsetupproject/scripts/lhcb-prepare -d $HOME/cmtuser -x -p ${REL}

echo "Running SetupProject Ganga ${REL}"
. SetupProject.sh Ganga ${REL}

echo "Running Tests"

rm -rf ~/gangadir_testing_GangaDirac ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaDirac/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaDirac.out

rm -rf ~/gangadir_testing_GangaGaudi ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaGaudi/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaGaudi.out

rm /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_{1,2{a,b,c,d,e,f,g,h},3}.out

## Here for reference but try not to use it please
##rm -rf ~/gangadir_testing_GangaLHCb && /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/Bugs 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_1.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/C* 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2a.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/Da* 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2b.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/Di* 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2c.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/GaudiPython 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2d.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/GaudiSplitters 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2e.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/I* 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2f.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/Tasks 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2g.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/GPI/Test* 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_2h.out

rm -rf ~/gangadir_testing_GangaLHCb ; /afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga --test -o'[Configuration]RUNTIME_PATH=GangaTest' -o'[TestingFramework]ReleaseTesting=True' -o'Config=localxml.ini' GangaLHCb/old_test/Lib 2>&1 | tee /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_3.out

rm /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb.out

cat /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb_{1,2{a,b,c,d,e,f,g,h},3}.out >> /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output/GangaLHCb.out


#cp  /afs/cern.ch/user/g/gangalb/cmtuser/GANGA/GANGA_${REL}/install/ganga/reports/latest/output/GangaLHCb.test.* /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/output
#cp  /afs/cern.ch/user/g/gangalb/cmtuser/GANGA/GANGA_${REL}/install/ganga/reports/latest/* /afs/cern.ch/sw/ganga/install/${VERSION}/reports/latest/

/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_gen.sh -f -v ${VERSION}
/afs/cern.ch/sw/ganga/install/${VERSION}/release/tools/schema_test.sh -v ${VERSION}

