#!/bin/bash

usage()
{
cat << EOF
usage: $0 [options] 

The default behaviour (i.e. without the -f/-g/-t flags) of this script is:
1) Create a new Ganga repository for the indicated Ganga release version.
2) Run the given version of Ganga, using all repositories found in the user's gangadir.


OPTIONS:
   -l      Don't run tests, just list available repositories.
   -r      Absolute location of repository (default: ~/schema_test_gangadir).
   -v      Version of Ganga to execute from /afs/cern.ch/sw/ganga/install (format: 5.8.9-pre).
   -t      Repository version to test (default is to test all available repositories (format: 5.8.9-pre).
   -h      Show this message.
EOF
}

echo ""
echo ""

while getopts "lv:t:r:" OPTION
do
    case $OPTION in
        l)
            LIST=TRUE
            ;;
        v)
            VERSION=$OPTARG
            ;;
        h)
            usage
            exit 1
            ;;
        t)
            TEST_REPO=$OPTARG
            ;;
        r)
            GANGADIR=$OPTARG
            ;;
    esac
done

if [[ ! -d $GANGADIR ]] 
then
    echo 'Gangadir option not set. Using default of ~/schema_test_gangadir'
    GANGADIR=~/schema_test_gangadir
fi


if [[ -n $LIST ]]
then
    echo ""
    echo "Repositories available for testing:"
    echo ""
    for prev_version in `find ${GANGADIR}/* -maxdepth 0 \( ! -name "repository" \)`
    do
        echo $prev_version
    done
    exit 0
fi

if [[ -z "$VERSION" ]] 
then
    echo 'Missing ganga version. Format: 5.8.9-pre etc.'
    usage
    exit 1
fi



if [[ -n "$TEST_REPO" ]] 
then
    echo 'Only testing for repository version:' $TEST_REPO
    REPO_LOC=${GANGADIR}"/"${TEST_REPO}
    if [ ! -d ${REPO_LOC} ]
    then
        echo ${REPO_LOC} "repository not found. Re-run $0 and generate repository"
        exit 1
    fi
fi


GANGA_EXE=/afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga


if [ ! -e ${GANGA_EXE} ]
then
    echo ${GANGA_EXE} "not found"
    exit 1
fi


if [[  -n "$REPO_LOC" ]]
##Run the test across one repo
then
    echo "Running ${GANGA_EXE} test series against existing repo ${REPO_LOC}"
    cmd="${GANGA_EXE} --test  -o[Configuration]gangadir=${GANGADIR}  -o[TestingFramework]SchemaTesting=${TEST_REPO} -o[Configuration]RUNTIME_PATH=GangaTest:GangaAtlas:GangaLHCb -o[TestingFramework]ReleaseTesting=True -o[TestingFramework]AutoCleanup=False Ganga/test/Schema/Test/"
    $cmd
else
##Run the test across all repos
    for prev_version in `find ${GANGADIR}/* -maxdepth 0 \( ! -name "repository" \)`
    do
        prev_version=`basename $prev_version`
        echo "Running version ${GANGA_EXE} test series against existing repo ${prev_version}"
        cmd="${GANGA_EXE} --test -o[Configuration]gangadir=${GANGADIR} -o[TestingFramework]SchemaTesting=${prev_version}   -o[Configuration]RUNTIME_PATH=GangaTest:GangaAtlas:GangaLHCb -o[TestingFramework]ReleaseTesting=True -o[TestingFramework]AutoCleanup=False Ganga/test/Schema/Test/"
        $cmd
    done
fi
