#!/bin/sh

## function for defining exitcode of the AMAAthena/Ganga job wrapper
define_ama_exitcode() {
    export EC_ATLAS_SOFTWARE_UNAVAILABLE=103
    export EC_ATHENA_COMPILATION_ERROR=104
    export EC_ATHENA_RUNTIME_ERROR=105
    export EC_STAGEIN_ERROR=106
    export EC_STAGEOUT_ERROR=107
    export EC_MAKEOPT_ERROR=108
}

## function for checking/restoring LCG runtime libraries
check_lcg_env() {
    echo "== check lcg python path =="
    if [ ! -z $LCG_LOCATION ]; then
        lcg_pythonpath=$LCG_LOCATION/lib/python
        if [ -z `echo $PYTHONPATH | grep $lcg_pythonpath` ]; then
            export PYTHONPATH=$PYTHONPATH:$lcg_pythonpath
            echo "append $lcg_pythonpath to PYTHONPATH"
        fi
    fi
    echo "===="
}

## function for printing WN env. info 
print_ext_wn_info () {
    echo "== resource limitation =="
    ulimit -a
    echo "===="
    echo 

    echo "== memory information =="
    cat /proc/meminfo 
    echo "===="
    echo 
}

## function for fixing dCache/DPM/Castor libraries
load_special_dm_libraries() {
    #################################################
    # Fix of broken DCache ROOT access in 12.0.x
    if [ -e libDCache.so ] && [ n$GANGA_SETYPE = n'DCACHE' ] && [ ! -z `echo $ATLAS_RELEASE | grep 12.` ]; then 
        echo 'Fixing broken DCache ROOT access in athena 12.0.x'
        chmod +x libDCache.so
    fi
    
    #################################################
    # Fix of broken DPM ROOT access in 12.0.x
    if [ -e libRFIO.so ] && [ n$GANGA_SETYPE = n'DPM' ] && ( [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] ); then
        echo 'Fixing broken DPM ROOT access in athena 12.0.x'
        chmod +x libRFIO.so
    fi
    if [ -e libRFIO.so ] && [ n$GANGA_SETYPE = n'DPM' ] && [ ! -z `echo $ATLAS_RELEASE | grep 13.2.0` ]
    then
        echo 'Remove libRFIO.so in athena 13.2.0'
        rm libRFIO.so
    fi
    if [ n$GANGA_SETYPE = n'DPM' ] 
    then
        echo 'Creating soft link to fix broken DPM ROOT access in athena'
        ln -s $LCG_LOCATION/lib/libdpm.so libshift.so.2.1
    fi
    
    export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH
}


# generate additional AMA job option files
#  - AMAConfigFile.py
#  - input.py if not presented
ama_make_options () {

    retcode=0

    # make AMAConfigFile
    if [ ! -f AMAConfigFile.py ]; then
        cat - >AMAConfigFile.py <<EOF
from AMAAthena.AMAAthenaConf import *
from AMAAthena.AMAUtilsTool import AMAUtilsTool
from AthenaCommon.AlgSequence import AlgSequence

## get algorithm sequence
thejob = AlgSequence()

## get number of the max. events from environment
EvtMax = -1
if os.environ.has_key('ATHENA_MAX_EVENTS'):
    EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])

## set number of the max. events in Athena
theApp.EvtMax = EvtMax

## get AMA driver flag from environment
FlagList = ""
if os.environ.has_key('AMA_FLAG_LIST'):
    FlagList = ' '.join( os.environ['AMA_FLAG_LIST'].split(':') )

## get AMA driver config/sample file from environment
ConfigFile = os.environ['AMA_DRIVER_CONF']
SampleName = os.environ['AMA_SAMPLE_NAME']

## load AMA driver
amatool = AMAUtilsTool(ConfigFile)
try:
    ## for new AMAUtilsTool
    amatool.CreateJobOptions(ConfigFile, outputfile='AMADriver_jobOptions.py', flags=FlagList.split())
    if os.path.exists('AMADriver_jobOptions.py'):
        include( 'AMADriver_jobOptions.py' )
    else:
        raise IOError( 'Job options for AMADriver not created properly: AMADriver_jobOptions.py' )
except AttributeError:
    ## fail-over for the old-fashion AMAUtilsTool
    bml,eml,ReaderMap,ModuleList = amatool.ListModules()

    print "Now adding AMA driver: Driver"
    thejob += AMADriverAlg('Driver')
    thejob.Driver.EvtMax     = EvtMax
    thejob.Driver.ConfigFile = ConfigFile
    thejob.Driver.SampleName = SampleName
    thejob.Driver.FlagList   = FlagList

    ## load AMA reader
    for key in ReaderMap.keys():
        if (len(ReaderMap[key])==1):
            print "Now adding AMA reader: %s" % (key)
            reader = AMAReaderAlg(key)
            reader.ContainerList = ReaderMap[key]
            thejob += reader

    for mod in ModuleList:
        print "Now adding AMA module: %s" % (mod)
        thejob += AMAModuleAlg(mod)
EOF
    fi

    # make input.py if not yet presented
    if [ ! -f input.py ]; then
        # case 1: the FileStager sample_file.list is presented, take it directly
        if [ ! -z $AMA_WITH_STAGER ] && [ -f grid_sample.list ]; then
            cat - >input.py <<EOF
ic = []
# input with FileStager
from FileStager.FileStagerTool import FileStagerTool
stagetool = FileStagerTool(sampleFile='grid_sample.list')

## get Reference to existing Athena job
from FileStager.FileStagerConf import FileStagerAlg
from AthenaCommon.AlgSequence import AlgSequence

thejob = AlgSequence()

if stagetool.DoStaging():
    thejob += FileStagerAlg('FileStager')
    thejob.FileStager.InputCollections = stagetool.GetSampleList()
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

## get a handle on the ServiceManager
svcMgr = theApp.serviceMgr()
svcMgr.EventSelector.InputCollections = ic
EOF

        ## input_files is only meaningful for jobs running locally
        ## as it's provided on the client side
        elif [ -f input_files ] && [ n'GANGA_ATHENA_WRAPPER_MODE' == n'local' ] ; then
            cat - >input.py <<EOFF
ic = []
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
EOFF
        fi
    fi

    return $retcode
}

# run athena
ama_run_athena () {

    job_options=$*

    retcode=0

    export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

    echo "Running Athena ..."

    if [ -z $AMA_LOG_LEVEL ]; then
        export AMA_LOG_LEVEL=INFO
    fi

    $timecmd athena.py -l $AMA_LOG_LEVEL $job_options; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp

    return $retcode
}

## function for running DQ2_COPY mode 
ama_run_athena_with_dq2_copy() {

    retcode=0

    # Create generic input.py
    cat - >input.py <<EOF
ic = []
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
    EventSelector.InputCollections = ic
    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1
EOF
    if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ]; then 
        sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
        mv input.py.new input.py
    fi
    
    if [ -e PoolFileCatalog.xml ]; then
        rm PoolFileCatalog.xml
    fi
    
    # Setup new dq2- tools
    if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]; then
        source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
    else
        echo 'ERROR: DQ2Clients with dq2-get are not installed at the site - please contact Ganga support mailing list.'
        echo '1'>retcode.tmp
    fi

    # Set DQ2_LOCAL_SITE_ID to dataset location
    if [ -e dq2localid.txt ]; then
        export DQ2_LOCAL_SITE_ID=`cat dq2localid.txt`
        export DQ2_LOCAL_ID_BACKUP=$DQ2_LOCAL_ID
    fi
    
    if [ n$GANGA_SETYPE = n'DCACHE' ]; then
        export DQ2_LOCAL_PROTOCOL='dcap'
    elif [ n$GANGA_SETYPE = n'DPM' ] || [ n$GANGA_SETYPE = n'CASTOR' ]; then
        export DQ2_LOCAL_PROTOCOL='rfio'
    fi
    
    # File counter
    I=0
    
    # Parse jobs jobOptions and set timing command
	prepare_athena

    if [ -z $AMA_LOG_LEVEL ]; then
        export AMA_LOG_LEVEL=INFO
    fi

    cat input_files | while read filespec; do
        for file in $filespec; do
	        echo "Downloading input file $file ..."
            let "I += 1"
            # use dq2-get to download input file
            if [ -e $VO_ATLAS_SW_DIR/ddm/latest/setup.sh ]; then
                for ((i=1;i<=3;i+=1)); do
                    echo Copying $file, attempt $i of 3
                    $timecmd dq2-get -d --automatic --timeout=300 --files=$file $DATASETNAME;  echo $? > retcode.tmp
                    if [ -e $DATASETNAME/$file ]; then
                        mv $DATASETNAME/* .
                        echo $file > input.txt
                        echo successfully retrieved $file
                        break
                    else
                        echo 'ERROR: dq2-get of inputfile failed !' 1>&2
                        echo '1' > retcode.tmp
                    fi
                done
            else
                echo 'ERROR: DQ2Clients with dq2-get are not installed at the site - please contact Ganga support mailing list.' 1>&2
                echo '1'>retcode.tmp
            fi

	        retcode=`cat retcode.tmp`
            rm -f retcode.tmp
            ls -rtla
	
            if [ $retcode -eq 0 ] && [ -e $file ]; then
                # Create PoolFileCatalog.xml
                pool_insertFileToCatalog $file; echo $? > retcode.tmp
                retcode=`cat retcode.tmp`
                rm -f retcode.tmp
            fi

            if [ $retcode -eq 0 ] && [ -e $file ]; then
	            echo "Running Athena ..."
		        $timecmd athena.py -l $AMA_LOG_LEVEL $ATHENA_OPTIONS AMAConfigFile.py input.py; echo $? > retcode.tmp
		        retcode=`cat retcode.tmp`
		        rm -f retcode.tmp

                # Rename output files
                cat output_files.new | while read outfiles; do
                    for ofile in $outfiles; do
  	     	        mv $ofile ${ofile}.$I
                        echo "${ofile}.$I" >> output_files.copy
                    done
                done
  	         
                if [ n$ATHENA_MAX_EVENTS != n'-1' ] && [ n$ATHENA_MAX_EVENTS != n'' ]; then 
                    break
                fi
  	        else
  	            echo "Problems with input file $file"
  	        fi
            rm $file
        done

        if [ n$ATHENA_MAX_EVENTS != n'-1' ] && [ n$ATHENA_MAX_EVENTS != n'' ]; then 
            break
        fi
    done
    
    if [ -e output_files.copy ]; then
        mv output_files.new output_files.new.old
        mv output_files.copy output_files.new
    fi

    return $retcode
}
