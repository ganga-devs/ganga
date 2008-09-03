#!/bin/sh

# function for resolving and setting TMPDIR env. variable
resolve_tmpdir () {
    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        #  - fistly respect the TMPDIR setup on WN
        #  - use SCRATCH_DIRECTORY from pre-WS globus gatekeeper 
        #  - use EDG_WL_SCRATCH from gLite middleware
        #  - use /tmp dir forcely in the worst case
        if [ -z $TMPDIR ]; then
            if [ ! -z $SCRATCH_DIRECTORY ]; then
                export TMPDIR=$SCRATCH_DIRECTORY  
            elif [ ! -z $EDG_WL_SCRATCH ]; then
                export TMPDIR=$EDG_WL_SCRATCH
            else 
                export TMPDIR=`mktemp -d /tmp/ganga_scratch_XXXXXXXX`
                if [ $? -ne 0 ]; then
                    echo "cannot create and setup TMPDIR"
                    exit 1
                fi
            fi
        fi
    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        #  - check if WORKDIR is given in the LSF case
        if [ ! -z $LSB_JOBID ] && [ ! -z $WORKDIR ]; then
            export TMPDIR=`mktemp -d $WORKDIR/ganga_scratch_XXXXXXXX`
        fi
    fi
}

## function for printing WN env. info 
print_wn_info () {
    echo "== hostname =="
    hostname -f
    echo "===="
    echo 

    echo "== system =="
    uname -a
    echo "===="
    echo 

    echo "==  env. variables =="
    env
    echo "===="
    echo 

    echo "==  disk usage  =="
    df
    echo "===="
    echo 
}

## function for setting up CMT environment
cmt_setup () {

    # setup ATLAS software
    unset CMTPATH
    
    export LCG_CATALOG_TYPE=lfc
    
    #  LFC Client Timeouts
    export LFC_CONNTIMEOUT=180
    export LFC_CONRETRY=2
    export LFC_CONRETRYINT=60
    
    # improve dcap reading speed
    export DCACHE_RAHEAD=TRUE
    #export DCACHE_RA_BUFFER=196608
  
    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        ATLAS_RELEASE_DIR=$VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE
    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        ATLAS_RELEASE_DIR=$ATLAS_SOFTWARE/$ATLAS_RELEASE
    fi
 
    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]; then
        source $ATLAS_RELEASE_DIR/setup.sh 
    elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]; then
        #if [ n$ATLAS_PROJECT = n'AtlasPoint1' ]; then
        if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_PRODUCTION,$ATLAS_PROJECT
        elif [ ! -z $ATLAS_PROJECT ]; then
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_RELEASE,$ATLAS_PROJECT
        else
            source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE
        fi
    fi

    # print relevant env. variables for debug 
    echo "CMT setup:"
    env | grep 'CMT'
    echo "SITE setup:"
    env | grep 'SITE'
}

## function for getting grid proxy from a remote endpoint 
get_remote_proxy () {
    export X509_CERT_DIR=$X509CERTDIR
    if [ ! -z $REMOTE_PROXY ]; then
        scp -o StrictHostKeyChecking=no $REMOTE_PROXY $PWD/.proxy
        export X509_USER_PROXY=$PWD/.proxy
    fi

    # print relevant env. variables for debug 
    env | grep 'GLITE'
    env | grep 'X509'
    env | grep 'GLOBUS' 
    voms-proxy-info -all
}

## function for fixing g2c/gcc issues on SLC3/SLC4 against
## ATLAS release 11, 12, 13 
fix_gcc_issue () {
    # fix SL4 gcc problem
    RHREL=`cat /etc/redhat-release`
    SC41=`echo $RHREL | grep -c 'Scientific Linux CERN SLC release 4'`
    SC42=`echo $RHREL | grep -c 'Scientific Linux SL release 4'`
    which gcc32; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
    if [ $retcode -eq 0 ] && ( [ $SC41 -gt 0 ] || [ $SC42 -gt 0 ] ) ; then
        mkdir comp
        cd comp

        cat - >gcc <<EOF
#!/bin/sh

/usr/bin/gcc32 -m32 \$*

EOF

        cat - >g++ <<EOF
#!/bin/sh

/usr/bin/g++32 -m32 \$*

EOF

        chmod +x gcc
        chmod +x g++
        cd ..
        export PATH=$PWD/comp:$PATH
        ln -s /usr/lib/gcc/i386-redhat-linux/3.4.3/libg2c.a $PWD/comp/libg2c.a
        export LD_LIBRARY_PATH=$PWD/comp:$LD_LIBRARY_PATH
        export LIBRARY_PATH=$PWD/comp:$LIBRARY_PATH
    fi

    # fix SL3 g2c problem
    SC31=`echo $RHREL | grep -c 'Scientific Linux'`
    SC32=`echo $RHREL | grep -c 'elease 3.0'`
    ls /usr/lib/libg2c.a; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
    if [ $retcode -eq 1 ] &&  [ $SC31 -gt 0 ] && [ $SC32 -gt 0 ]  ; then
        ln -s /usr/lib/gcc-lib/i386-redhat-linux/3.2.3/libg2c.a $PWD/libg2c.a
        export LIBRARY_PATH=$PWD:$LIBRARY_PATH
    fi
}

## function for setting up athena runtime environment 
athena_setup () {

    echo "Setting up the Athena environment ..."

    if [ -z $USER_AREA ] && [ -z $ATHENA_USERSETUPFILE ]
    then
        if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
        then
            source $SITEROOT/dist/$ATLAS_RELEASE/Control/AthenaRunTime/AthenaRunTime-*/cmt/setup.sh
        elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]
        then
            source $SITEROOT/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
            if [ ! -z $ATLAS_PRODUCTION_ARCHIVE ]
            then
                wget $ATLAS_PRODUCTION_ARCHIVE
                export ATLAS_PRODUCTION_FILE=`ls AtlasProduction*.tar.gz`
                tar xzf $ATLAS_PRODUCTION_FILE
                export CMTPATH=`ls -d $PWD/work/AtlasProduction/*`
                export MYTEMPDIR=$PWD
                cd AtlasProduction/*/AtlasProductionRunTime/cmt
                cmt config
                source setup.sh
                echo $CMTPATH
                cd $MYTEMPDIR
            fi
        fi

    elif [ ! -z $ATHENA_USERSETUPFILE ]
    then
        . $ATHENA_USERSETUPFILE
    else
        mkdir work

        if [ ! -z $ATLAS_PRODUCTION_ARCHIVE ]
        then
            wget $ATLAS_PRODUCTION_ARCHIVE
            export ATLAS_PRODUCTION_FILE=`ls AtlasProduction*.tar.gz`
            tar xzf $ATLAS_PRODUCTION_FILE -C work
            export CMTPATH=`ls -d $PWD/work/AtlasProduction/*`
            export MYTEMPDIR=$PWD
            cd work/AtlasProduction/*/AtlasProductionRunTime/cmt
            cmt config
            source setup.sh
            echo $CMTPATH
            cd $MYTEMPDIR
        fi

        if [ ! -z $GROUP_AREA_REMOTE ] ; then
            echo "Fetching group area tarball $GROUP_AREA_REMOTE..."
            wget $GROUP_AREA_REMOTE
            FILENAME=`echo ${GROUP_AREA_REMOTE} | sed -e 's/.*\///'`
            tar xzf $FILENAME -C work
            NUMFILES=`ls work | wc -l`
            DIRNAME=`ls work`
            if [ $NUMFILES -eq 1 ]
            then
	        mv work/$DIRNAME/* work
	        rmdir work/$DIRNAME
            else
	        echo 'no group area clean up necessary'
            fi
        elif [ ! -z $GROUP_AREA ]
        then
            tar xzf $GROUP_AREA -C work
        fi
        tar xzf $USER_AREA -C work
        cd work
        pwd
        source install.sh; echo $? > retcode.tmp
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        if [ $retcode -ne 0 ]; then
            echo "*************************************************************"
            echo "*** Compilation warnings. Return Code $retcode            ***"
            echo "*************************************************************"
        fi
        pwd
        cd ..
    fi
}

# Determine PYTHON executable in ATLAS release
get_pybin () {

    if [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
        ATLAS_PYBIN_LOOKUP_PATH=$ATLAS_SOFTWARE

    elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
        ATLAS_PYBIN_LOOKUP_PATH=$VO_ATLAS_SW_DIR/prod/releases

    else
        echo "get_pybin not implemented"
    fi

    if [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]; then
        export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/*/bin/python | head -1)
    else
        export pybin=$(ls -r $ATLAS_PYBIN_LOOKUP_PATH/*/sw/lcg/external/Python/*/slc3_ia32_gcc323/bin/python | head -1)
    fi
}

# detecting the se type
detect_setype () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORG=$1
    MY_PATH_ORG=$2
    MY_PYTHONPATH_ORG=$3

    if [ -e ganga-stage-in-out-dq2.py ]; then

        chmod +x ganga-stage-in-out-dq2.py
    
        LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
        PATH_BACKUP=$PATH
        PYTHONPATH_BACKUP=$PYTHONPATH
        export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
        export PATH=$MY_PATH_ORG:$PATH_BACKUP
        export PYTHONPATH=$MY_PYTHONPATH_ORG:$PYTHONPATH_BACKUP
        which python32; echo $? > retcode.tmp
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
        if [ $retcode -eq 0 ]; then
            export GANGA_SETYPE=`python32 ./ganga-stage-in-out-dq2.py --setype`
        else
            export GANGA_SETYPE=`$pybin ./ganga-stage-in-out-dq2.py --setype`
        fi
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
        export PATH=$PATH_BACKUP
        export PYTHONPATH=$PYTHONPATH_BACKUP
    fi
}

# staging input data files
stage_inputs () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORG=$1
    MY_PATH_ORG=$2
    MY_PYTHONPATH_ORG=$3

    # Unpack dq2info.tar.gz
    if [ -e dq2info.tar.gz ]; then
        tar xzf dq2info.tar.gz
    fi

    if [ -e input_files ]; then
        echo "Preparing input data ..."
        # DQ2Dataset
        if [ -e input_guids ] && [ -e ganga-stage-in-out-dq2.py ]; then
            chmod +x ganga-stage-in-out-dq2.py
            chmod +x dq2_get
            LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
            PATH_BACKUP=$PATH
            PYTHONPATH_BACKUP=$PYTHONPATH
            export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
            export PATH=$MY_PATH_ORG:$PATH_BACKUP
            export PYTHONPATH=$MY_PYTHONPATH_ORG:$PYTHONPATH_BACKUP
            which python32; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
            if [ $retcode -eq 0 ]; then
                python32 ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
            else
                if [ -e /usr/bin32/python ]; then
                    /usr/bin32/python ./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
                else
                    ./ganga-stage-in-out-dq2.py -v; echo $? > retcode.tmp
                fi
            fi
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
            # Fail over
            if [ $retcode -ne 0 ]; then
                $pybin ./ganga-stage-in-out-dq2.py -v; echo $? > retcode.tmp
                retcode=`cat retcode.tmp`
                rm -f retcode.tmp
            fi
            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
            export PATH=$PATH_BACKUP
            export PYTHONPATH=$PYTHONPATH_BACKUP

        # ATLASDataset, ATLASCastorDataset, ATLASLocalDataset
        elif [ -e ganga-stagein-lfc.py ]; then 
            chmod +x ganga-stagein-lfc.py
            ./ganga-stagein-lfc.py -v -i input_files; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        elif [ -e ganga-stagein.py ]; then
	    chmod +x ganga-stagein.py
	    ./ganga-stagein.py -v -i input_files; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
            rm -f retcode.tmp
        else
	    cat input_files | while read file
	    do
	        pool_insertFileToCatalog $file 2>/dev/null; echo $? > retcode.tmp
	        retcode=`cat retcode.tmp`
	        rm -f retcode.tmp
	    done
        fi
    fi
}

# staging output files 
stage_outputs () {

    # given the library/binaray/python paths for data copy commands
    MY_LD_LIBRARY_PATH_ORG=$1
    MY_PATH_ORG=$2
    MY_PYTHONPATH_ORG=$3

    if [ $retcode -eq 0 ]
    then
        echo "Storing output data ..."
        if [ -e ganga-stage-in-out-dq2.py ] && [ -e output_files ] && [ ! -z $OUTPUT_DATASETNAME ]
        then
            chmod +x ganga-stage-in-out-dq2.py
            export DATASETTYPE=DQ2_OUT
            LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
            PATH_BACKUP=$PATH
            PYTHONPATH_BACKUP=$PYTHONPATH
            export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORG:$LD_LIBRARY_PATH_BACKUP:/opt/globus/lib
            export PATH=$MY_PATH_ORG:$PATH_BACKUP
            export PYTHONPATH=$MY_PYTHONPATH_ORG:$PYTHONPATH_BACKUP
            which python32; echo $? > retcode.tmp
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
            if [ $retcode -eq 0 ]; then
                python32 ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
            else
                if [ -e /usr/bin32/python ]
                then
                    /usr/bin32/python ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
                else
                    ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
                fi

                ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
            fi
            retcode=`cat retcode.tmp`
            rm -f retcode.tmp
            # Fail over
            if [ $retcode -ne 0 ]; then
                $pybin ./ganga-stage-in-out-dq2.py --output=output_files.new; echo $? > retcode.tmp
                retcode=`cat retcode.tmp`
                rm -f retcode.tmp
            fi

            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
            export PATH=$PATH_BACKUP
            export PYTHONPATH=$PYTHONPATH_BACKUP

        elif [ -n "$OUTPUT_LOCATION" -a -e output_files ]; then

            if [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
                TEST_CMD=`which rfmkdir 2>/dev/null`
                if [ ! -z $TEST_CMD ]; then
                    MKDIR_CMD=$TEST_CMD
                else
                    MKDIR_CMD="mkdir" 
                fi

                TEST_CMD2=`which rfcp 2>/dev/null`
                if [ ! -z $TEST_CMD2 ]; then
                    CP_CMD=$TEST_CMD2
                else
                    CP_CMD="cp" 
                fi
             
                $MKDIR_CMD -p $OUTPUT_LOCATION
                cat output_files | while read filespec; do
                    for file in $filespec; do
                      $CP_CMD $file $OUTPUT_LOCATION/$file
                    done
                done

            elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then

                LD_LIBRARY_PATH_BACKUP=$LD_LIBRARY_PATH
                PATH_BACKUP=$PATH
                PYTHONPATH_BACKUP=$PYTHONPATH
                export LD_LIBRARY_PATH=$PWD:$MY_LD_LIBRARY_PATH_ORG:/opt/globus/lib:$LD_LIBRARY_PATH_BACKUP
                export PATH=$MY_PATH_ORG:$PATH_BACKUP
                export PYTHONPATH=$MY_PYTHONPATH_ORG:$PYTHONPATH_BACKUP

                ## copy and register files with 3 trials
                cat output_files | while read filespec; do
                    for file in $filespec; do
                        lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                        retcode=`cat retcode.tmp`
                        rm -f retcode.tmp
                        if [ $retcode -ne 0 ]; then
                            sleep 120
                            lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                            retcode=`cat retcode.tmp`
                            rm -f retcode.tmp
                            if [ $retcode -ne 0 ]; then
                                sleep 120
                                lcg-cr --vo atlas -t 300 -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
                                retcode=`cat retcode.tmp`
                                rm -f retcode.tmp
                                if [ $retcode -ne 0 ]; then
                                    # WRAPLCG_STAGEOUT_LCGCR
                                    retcode=410403
                                fi
                            fi
                        fi
                    done
                done

                export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP
                export PATH=$PATH_BACKUP
                export PYTHONPATH=$PYTHONPATH_BACKUP
            fi
        fi
    fi
}

# generate additional AMA job option files 
#  - AMAConfigFile.py
#  - input.py
ama_make_options () {

    # make AMAConfigFile
    if [ ! -f AMAConfigFile.py ] && [ $retcode -eq 0 ]; then 
        cat - >AMAConfigFile.py <<EOF
# AMA ConfigFile
SampleFile = 'grid_sample.list'
SampleName = os.environ['AMA_SAMPLE_NAME']
ConfigFile = os.environ['AMA_DRIVER_CONF']

FlagList = ""
if os.environ.has_key('AMA_FLAG_LIST'):
    FlagList = os.environ['AMA_FLAG_LIST']

## set number of the max. events 
EvtMax = -1
if os.environ.has_key('ATHENA_MAX_EVENTS'):
    EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
EOF
    fi

    ## generate the input.py dependent on if FileStager is enabled
    if [ ! -f input.py ] && [ $retcode -eq 0 ]; then 
        cat - >input.py <<EOF
ic = []
if os.environ.has_key('AMA_WITH_STAGER'):
    # input with FileStager
    from FileStager.FileStagerTool import FileStagerTool
    stagetool = FileStagerTool(sampleFile=SampleFile)

    ## get Reference to existing Athena job
    from FileStager.FileStagerConf import FileStagerAlg
    from AthenaCommon.AlgSequence import AlgSequence
    
    thejob = AlgSequence()
    
    if stagetool.DoStaging():
        thejob += FileStagerAlg('FileStager')
        thejob.FileStager.InputCollections = stagetool.GetSampleList()
        #thejob.FileStager.PipeLength = 2
        #thejob.FileStager.VerboseStager = True
        thejob.FileStager.BaseTmpdir    = stagetool.GetTmpdir()
        thejob.FileStager.InfilePrefix  = stagetool.InfilePrefix
        thejob.FileStager.OutfilePrefix = stagetool.OutfilePrefix
        thejob.FileStager.CpCommand     = stagetool.CpCommand
        thejob.FileStager.CpArguments   = stagetool.CpArguments
        thejob.FileStager.FirstFileAlreadyStaged = stagetool.StageFirstFile
    
    ## set input collections
    if stagetool.DoStaging():
        ic = stagetool.GetStageCollections()
    else:
        ic = stagetool.GetSampleList()
else:
    ## implement the case that FileStager is not enabled
    if os.path.exists('input_files'):
        for lfn in file('input_files'):
            name = os.path.basename(lfn.strip())
            pfn = os.path.join(os.getcwd(),name)
            if (os.path.exists(pfn) and (os.stat(pfn).st_size>0)):
                print 'Input: %s' % name
                ic.append('%s' % name)
            elif (os.path.exists(lfn.strip()) and (os.stat(lfn.strip()).st_size>0)):
                print 'Input: %s' % lfn.strip()
                ic.append('%s' % lfn.strip())

## get a handle on the ServiceManager
svcMgr = theApp.serviceMgr()
svcMgr.EventSelector.InputCollections = ic
EOF
    fi
}

# run athena
run_athena () {

    job_options=$*

    if [ $retcode -eq 0 ] || [ n$DATASETTYPE = n'DQ2_COPY' ]; then

        echo "Parsing jobOptions ..."
        if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
        then
            chmod +x ganga-joboption-parse.py
            ./ganga-joboption-parse.py
        fi
    fi

    # run athena in regular mode =========================================== 
    if [ $retcode -eq 0 ]; then
        ls -al
        env | grep DQ2
        env | grep LFC

        echo "Running Athena ..."
        athena.py $job_options; echo $? > retcode.tmp
        retcode=`cat retcode.tmp`
        rm -f retcode.tmp
    fi
}
