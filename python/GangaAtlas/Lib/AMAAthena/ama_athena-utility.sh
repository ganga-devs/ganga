#!/bin/sh

## function for defining exitcode of the AMAAthena/Ganga job wrapper
define_ama_exitcode() {
    EC_ATLAS_SOFTWARE_UNAVAILABLE=410103
    EC_ATHENA_COMPILATION_ERROR=410104
    EC_ATHENA_RUNTIME_ERROR=410105
    EC_STAGEIN_ERROR=410106
    EC_STAGEOUT_ERROR=410107
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

## function for running DQ2_COPY mode 
run_athena_with_dq2_copy() {

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

    retcode=0

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
		        $timecmd athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
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
}

# generate file stager job option
#  - sampleFile: the name of the file composed by AMAAthenaDriver
#  - sampleList: the name of the file containing simply a list of files for FileStager
#
# the routine generates a job option file "input_stager.py" to be appended after "input.py"
make_FileStager_jobOption() {

    if [ ! -f input_stager.py ]; then
        cat - >input_stager.py << EOF

#################################################################################################
# Provide input for the FileStager here
#################################################################################################

## import filestager tool
from FileStager.FileStagerTool import FileStagerTool

## File with input collections
if (not 'sampleFile' in dir()):
  sampleFile = "../samples/top.def"

if ('sampleList' in dir()):
  stagetool = FileStagerTool(sampleList=sampleList)
elif ('sampleFile' in dir()):
  stagetool = FileStagerTool(sampleFile=sampleFile)

## Configure rf copy command used by the stager; default is 'lcg-cp -v --vo altas -t 1200'
#stagetool.CpCommand = "rfcp"
#stagetool.CpArguments = []
#stagetool.OutfilePrefix = ""
#stagetool.checkGridProxy = False

#################################################################################################
# Configure the FileStager -- no need to change these lines
#################################################################################################

## get Reference to existing Athena job
from AthenaCommon.AlgSequence import AlgSequence
thejob = AlgSequence()

## check if collection names begin with "gridcopy"
print "doStaging?", stagetool.DoStaging()

## Import file stager algorithm
from FileStager.FileStagerConf import FileStagerAlg

## filestageralg needs to be the first algorithm added to the thejob.
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

#################################################################################################
# Pass collection names to EventSelector
#################################################################################################

## set input collections
ic = []
if stagetool.DoStaging():
  ic = stagetool.GetStageCollections()
else:
  ic = stagetool.GetSampleList()

## get a handle on the ServiceManager
svcMgr = theApp.serviceMgr()
svcMgr.EventSelector.InputCollections = ic
#svcMgr.EventSelector.SkipBadFiles = True
EOF

}
