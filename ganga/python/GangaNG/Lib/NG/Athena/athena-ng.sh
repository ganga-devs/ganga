#! /bin/sh -x
#
# Run Athena on the Grid
#
# Following environment settings are required
#
# ATLAS_RELEASE     ... the required ATLAS release
# USER_AREA         ... the tar file of the user area (optional)
# ATHENA_OPTIONS    ... Options to run Athena
# ATHENA_MAX_EVENTS ... Limit the events to be processed by Athena
# OUTPUT_LOCATION   ... Place to store the results
#
# ATLAS/ARDA - Dietrich.Liko@cern.ch

retcode=0

echo $ATLAS_RELEASE

env

echo 'host info'
env | grep HOST

if [ ! -z $ATLAS_USERSETUPFILE ]
then
    . $ATLAS_USERSETUPFILE
else                                                                                                             
   mkdir work

   if [ ! -z $GROUP_AREA_REMOTE ] ; then
       # group area already on the node, doenloaded by the gridmanager
       FILENAME=`echo ${GROUP_AREA_REMOTE} | sed -e 's/.*\///'`
       tar xzf $FILENAME -C work
       ls work
       NUMFILES=`ls work | wc -l`
       DIRNAME=`ls work`
       if [ $NUMFILES -eq 1 ]
       then
           mv work/$DIRNAME/* work
           rmdir work/$DIRNAME
	   ls work
	   chmod -R +w work
       else
           echo 'no group area clean up necessary'
       fi
   elif [ ! -z $GROUP_AREA ]
   then
       tar xzf $GROUP_AREA -C work
       ls work
       chmod -R +w work
   fi

   tar xzf $USER_AREA -C work
   cd work
   echo '############################################'
   cat install.sh
   echo '############################################'
   echo 'Calling install in '
   pwd
   ls -l
   source install.sh
   if [ $? -ne 0 ]
   then
      echo "***************************************************************"
      echo "***      Compilation warnings. Return Code $?               ***"
      echo "***************************************************************"
   fi
   cd ..
fi

cmt show macro_value cmt_compiler_version

get_files PDGTABLE.MeV

## Preparing job

inputfiles=
numinfiles=$1
shift
export INDIR=`pwd`

if [ $numinfiles -ge 1 ]; then
  rm -f PoolFileCatalog.xml
  echo "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" >> PoolFileCatalog.xml
  echo "<!-- Edited by POOL --><!DOCTYPE POOLFILECATALOG SYSTEM \"InMemory\">" >> PoolFileCatalog.xml
  echo "<POOLFILECATALOG>" >> PoolFileCatalog.xml

  for i in `seq 1 $numinfiles`;
  do
    lfn=$1
    shift
    guid=$1
    shift
    echo "  <File ID=\"$guid\">" >> PoolFileCatalog.xml
    echo "    <physical>" >> PoolFileCatalog.xml
    echo "       <pfn filetype=\"ROOT_All\" name=\"$lfn\"/>" >> PoolFileCatalog.xml
    echo "    </physical>" >> PoolFileCatalog.xml
    echo "    <logical/>" >> PoolFileCatalog.xml
    echo "  </File>" >> PoolFileCatalog.xml
  done
  echo "</POOLFILECATALOG>" >> PoolFileCatalog.xml
fi

echo "PoolFileCatalog.xml"
cat PoolFileCatalog.xml

cat input_files

# write to input_file for next step
# prepare input data
if [ -e input_files ]
then 
    echo "Preparing input data ..."
    # DQ2Dataset
cat - >input.py <<EOF
ic = []
if os.path.exists('input_files'):
    for lfn in file('input_files'):
        name = os.path.basename(lfn.strip())
        pfn = os.path.join(os.getcwd(),name)
        if (os.path.exists(pfn)) and (os.stat(pfn).st_size>0):
            print 'Input: %s' % name
            ic.append('%s' % name)
    EventSelector.InputCollections = ic
    if os.environ.has_key('ATHENA_MAX_EVENTS'):
        theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])
    else:
        theApp.EvtMax = -1

print 'ic ', ic

EOF

else
# no input_files
cat - >input.py <<EOF
if os.environ.has_key('ATHENA_MAX_EVENTS'):
   theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])

EOF
fi


if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] 
then
  sed 's/EventSelector/ServiceMgr.EventSelector/' input.py > input.py.new
  mv input.py.new input.py
fi

cat input.py

ls -rtla

numopt=$1
shift
echo 'number of options ' $numopt
if [ $numopt -ge 1 ]; then
    for n in `seq 1 $numopt`
      do
	ATHENA_OPTIONS="$ATHENA_OPTIONS $1"
	shift
    done
fi

echo 'ATHENA_OPTIONS '$ATHENA_OPTIONS

#   run athena
dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

echo $OUTPUT_JOBID
echo $GROUP_AREA_REMOTE
echo $GROUP_AREA

#if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
#    then
#    chmod +x ganga-joboption-parse.py
#    ./ganga-joboption-parse.py
#fi

if [ $retcode -eq 0 ]
then
    echo "Running Athena ..."
    athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
fi

# Making OutpuFiles.xml
echo 'Making OutputFiles.xml'

numoutfiles=$1
shift

if [ $numoutfiles -ge 1 ]; then
    for n in `seq 1 $numoutfiles`
      do
      outfile[$n]=$1
      outfilerls[$n]=$2
      dataset[$n]=$3
      shift
      shift
      shift
      tryit=`echo ${outfile[$n]} | grep "log.tgz"`
      if test \! -z $tryit; then
	  logfile=$tryit
      fi
    done
    
    for n in `seq 1 $numoutfiles`
      do
      echo "Output: ${outfile[$n]} ${lcn[$n]} ${dataset[$n]}"
    done
fi

if [ $numoutfiles -ge 1 ]; then
  echo "<outputfiles>" > OutputFiles.xml
  
  for i in `seq 1 $numoutfiles`
  do
    file=${outfile[$i]}
    if [ -e "$file" ]; then
      poolid=
      poolid=`grep $file -B 100 PoolFileCatalog.xml 2>/dev/null | grep "File ID" | tail -n 1 | cut -f2 -d'"'`
      if test -z $poolid; then
	  echo 'guid not found'
	  poolid=`pool_gen_uuid $file`
	  returncode=$?
	  if [ $returncode -ne 0 ] ; then
	      echo "pool_gen_uuid does not work or is missing"
	      echo "pool_gen_uuid does not work or is missing" >| wrapper.log
	      exit $returncode
	  fi
      fi
      md5sum=`md5sum $file 2>/dev/null | cut -f1 -d' '`
      returncode=$?
      if [ $returncode -ne 0 ] ; then
	  echo "md5sum does not work or is missing"
	  echo "md5sum does not work or is missing" >| wrapper.log
	  exit $returncode
      fi
      date=`find $file -printf "%TY-%Tm-%Td %TT" 2>/dev/null`
      size=`find $file -printf "%s"`

      echo "<file>" >> OutputFiles.xml
      echo "  <size>$size</size>" >> OutputFiles.xml
      echo "  <guid>$poolid</guid>" >> OutputFiles.xml
      echo "  <lfn>${outfilerls[$i]}</lfn>" >> OutputFiles.xml
      echo "  <date>$date</date>" >> OutputFiles.xml
      echo "  <md5sum>$md5sum</md5sum>" >> OutputFiles.xml
      echo "  <lcn>${lcn[$i]}</lcn>" >> OutputFiles.xml
      echo "  <dataset>${dataset[$i]}</dataset>" >> OutputFiles.xml
      echo "</file>" >> OutputFiles.xml
    fi
  done
  echo "</outputfiles>" >> OutputFiles.xml
fi

echo "OutputFiles.xml"
cat OutputFiles.xml

exit $retcode
