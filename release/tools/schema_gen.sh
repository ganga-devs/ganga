#!/bin/bash

usage()
{
cat << EOF
usage: $0 [options] 

The default behaviour (i.e. without the -f/-g/-t flags) of this script is:
1) Create a new Ganga repository for the indicated Ganga release version.
2) Run the given version of Ganga, using all repositories found in the user's gangadir.


OPTIONS:
   -f      Force the recreation of the test repository. This will overwrite the existing repo, if it exists.
   -d      Destination of repository (default: ~/gangadir_schema_test).
   -v      Version of Ganga to execute from /afs/cern.ch/sw/ganga/install (e.g. 5.8.9-pre).
   -r      Location of (temporary) Gangadir in which to create repository. If this exists, an attempt will always be made to delete it.
   -g      Version of Ganga from which the Generate.gpi file is taken. This is necessary to generate repositories for Ganga versions that pre-date the inclusion of Generate.gpi
   -h      Show this message.
EOF
}

echo ""
echo ""

FORCE=0
while getopts "d:fv:rg:" OPTION
do
    case $OPTION in
        f)
            FORCE=1
            ;;
        g)
            GEN_VER=$OPTARG
            ;;
        v)
            VERSION=$OPTARG
            ;;
        h)
            usage
            exit 1
            ;;
        d)
            DESTINATION=$OPTARG
            ;;
        r)
            GANGADIR=$OPTARG
            ;;
    esac
done

if [[ -z "$VERSION" ]] 
then
    echo 'Missing ganga version. Format: 5.8.9-pre etc.'
    usage
    exit 1
fi

if [[ -z "$GANGADIR" ]] 
then
    echo 'Gangadir option not set. Using default of ~/gangadir_temporary'
    GANGADIR=~/gangadir_temporary
fi

if [[ -z "$DESTINATION" ]]
then
    echo 'DESTINATION directory not set. Using default of ~/gangadir_schema_test'
    DESTINATION=~/gangadir_schema_test
fi


NEW_REPO_LOC=${DESTINATION}"/"${VERSION}

if [ -d ${GANGADIR} ]
then
    echo "Deleting existing gangdir:" ${GANGADIR}
    rm -r ${GANGADIR}
fi


GANGA_EXE=/afs/cern.ch/sw/ganga/install/${VERSION}/bin/ganga
if [[ -z "$GEN_VER" ]]
then
    GANGA_GEN=/afs/cern.ch/sw/ganga/install/${VERSION}/python/Ganga/old_test/Schema/Generate/Generate.gpi
else
    GANGA_GEN=/afs/cern.ch/sw/ganga/install/${GEN_VER}/python/Ganga/old_test/Schema/Generate/Generate.gpi
#   GANGA_GEN='Ganga/old_test/Schema/Generate'
fi

if [ ! -e ${GANGA_EXE} ]
then
    echo ${GANGA_EXE} "not found"
    exit 1
fi


#find out who's running this script
case `whoami` in 
    gangage )
        export LOAD_PACKAGES='GangaTest' ;;
    gangaat )
        export LOAD_PACKAGES='GangaTest:GangaAtlas' ;;
    gangalb )
        export LOAD_PACKAGES='GangaTest:GangaLHCb' ;;
esac

##Run the repo generation 
if [ ! -d ${NEW_REPO_LOC} ] || [ $FORCE == 1 ]
then
    if [ -d ${NEW_REPO_LOC} ]
    then
        echo "Deleting repository:" ${NEW_REPO_LOC} 
        rm -rf ${NEW_REPO_LOC}
    fi
    echo "Generating repository:" ${GANGADIR}
    #cmd="${GANGA_EXE}  --test -o[Configuration]user=testframework -o[Configuration]gangadir=${GANGADIR}  -o[TestingFramework]SchemaTesting=True -o[Configuration]RUNTIME_PATH=$LOAD_PACKAGES $GANGA_GEN"
    cmd="${GANGA_EXE}  --test -o[Configuration]gangadir=${GANGADIR}  -o[TestingFramework]SchemaTesting=${VERSION} -o[Configuration]RUNTIME_PATH=$LOAD_PACKAGES -o[TestingFramework]ReleaseTesting=True -o[TestingFramework]AutoCleanup=False $GANGA_GEN"
    echo $cmd
    $cmd
    
    echo "Moving repository: " ${GANGADIR} "->" ${NEW_REPO_LOC}
    mkdir -p ${NEW_REPO_LOC}
    mv ${GANGADIR}/${VERSION}/*  ${NEW_REPO_LOC}
else
    echo "Test repository" ${NEW_REPO_LOC} "already exists. Use -f to force regeneration."
fi
