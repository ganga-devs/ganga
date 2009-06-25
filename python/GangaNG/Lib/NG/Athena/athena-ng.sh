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

GANGATIME1=`date +'%s'`

echo $ATLAS_RELEASE

env

echo 'host info'
env | grep HOST

echo 'USER_AREA'
echo $USER_AREA
echo 'ATHENA_USERSETUPFILE'
echo $ATHENA_USERSETUPFILE
echo 'SITEROOT'
echo $SITEROOT

# move links here if requested (temp feature)
echo 'MOVE_LINKS_HERE'
echo $MOVE_LINKS_HERE
if [ ! -z $MOVE_LINKS_HERE ]
then
    echo "Moving all symbolic links containting .root in the name to local area"
    python move-linked-files-here.py
fi


# if not defined URER_AREA or ATHENA_USERSETUPFILE
if [ -z $USER_AREA ] && [ -z $ATHENA_USERSETUPFILE ]
then
    if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
    then
	source $SITEROOT/dist/$ATLAS_RELEASE/Control/AthenaRunTime/AthenaRunTime-*/cmt/setup.sh
    elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 15.` ]
    then
	echo 'sourcing AtlasOfflineRunTime'
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

elif [ ! -z $ATLAS_USERSETUPFILE ]
then
    . $ATLAS_USERSETUPFILE

else                                                                                                
   echo 'Make local USER_AREA'             
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

# Determine PYTHON executable in ATLAS release
if [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 15.` ]
then
    export pybin=$(ls -r $SITEROOT/sw/lcg/external/Python/*/*/bin/python | head -1)
else
    export pybin=$(ls -r $SITEROOT/sw/lcg/external/Python/*/slc3_ia32_gcc323/bin/python | head -1)
fi

# Determine python32 executable location 
# Set python32bin only if athena v14 is NOT setup
#which python32; echo $? > retcode.tmp
#retcode=`cat retcode.tmp`
#rm -f retcode.tmp
#if [ $retcode -eq 0 ] && [ -z `echo $ATLAS_RELEASE | grep 14.` ] ; then
#    export python32bin=`which python32`
#fi

which python
echo $pybin

cmt show macro_value cmt_compiler_version

# Unpack and set up any user-defined db release
pwd
if [ ! -z $DBFILENAME ]
then
    if [ ! -e $DBFILENAME ]
    then
	echo "ERROR: User-requested database file not found!"
    else
	tar xzf $DBFILENAME
	cd DBRelease/current/
	python setup.py | grep = | sed -e 's/^/export /' > dbsetup.sh
	source dbsetup.sh
	cd ../../
	ln -s DBRelease/current/geomDB/ .
	ln -s DBRelease/current/sqlite200/ .
   fi
fi


get_files PDGTABLE.MeV
# Make a local copy of requested geomDB if none already available
if [ ! -e geomDB ]; then
 mkdir geomDB
 cd geomDB
 get_files -data geomDB/larHV_sqlite
 get_files -data geomDB/geomDB_sqlite
 cd ..
fi
if [ ! -e sqlite200 ]; then
 mkdir sqlite200
 cd sqlite200
 get_files -data sqlite200/ALLP200.db
 cd ..
fi


GANGATIME2=`date +'%s'`

## Preparing job

inputfiles=
numinfiles=$1
shift
export INDIR=`pwd`

if [ $numinfiles -ge 1 ]; then
  echo 'Mating  PoolFileCatalog.xml'
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
        if lfn.find("gsidcap://")>-1:
          lfn = lfn.strip()
          print 'gsidcap input: %s' % lfn
          ic.append('%s' % lfn)
        else:
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

# Also copy the input file list to something recognized by AthenaROOTAccess
cp input_files input.txt

else
# no input_files
cat - >input.py <<EOF
if os.environ.has_key('ATHENA_MAX_EVENTS'):
   theApp.EvtMax = int(os.environ['ATHENA_MAX_EVENTS'])

EOF
fi


if [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 15.` ]
then
  echo 'Modify input.py'
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


GANGATIME3=`date +'%s'` 

#   run athena
dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

echo 'OUTPUT_JOBID'
echo $OUTPUT_JOBID
echo 'GROUP_AREA_REMOTE'
echo $GROUP_AREA_REMOTE
echo 'GROUP_AREA'
echo $GROUP_AREA

# Set up for dcap access (32bit is OK within athena for now)
# Note the '*' - we assume there's only one dcache version in athena, but it does change with the release...
export DCAP_PATH=$SITEROOT/sw/lcg/external/dcache_client/$(ls $SITEROOT/sw/lcg/external/dcache_client)/slc4_ia32_gcc34/dcap/lib/
export LD_LIBRARY_PATH=$DCAP_PATH:$LD_LIBRARY_PATH
export LD_PRELOAD=$SITEROOT/sw/lcg/external/dcache_client/*/slc4_ia32_gcc34/dcap/lib/libpdcap.so
export LD_PRELOAD_32=$SITEROOT/sw/lcg/external/dcache_client/*/slc4_ia32_gcc34/dcap/lib/libpdcap.so
export DCACHE_IO_TUNNEL=$SITEROOT/sw/lcg/external/dcache_client/*/slc4_ia32_gcc34/dcap/lib/libgsiTunnel.so

echo 'LD_LIBRARY_PATH'
echo $LD_LIBRARY_PATH
echo 'LD_PRELOAD'
echo $LD_PRELOAD
echo 'LD_PRELOAD_32'
echo $LD_PRELOAD_32
echo 'DCACHE_IO_TUNNEL'
echo $DCACHE_IO_TUNNEL


#if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
#    then
#    chmod +x ganga-joboption-parse.py
#    ./ganga-joboption-parse.py
#fi

echo 'retcode'
echo $retcode

if [ $retcode -eq 0 ]
then
    
    echo "Running Athena ..."
    if [ ! -z `echo $ATHENA_EXE_TYPE | grep PYARA` ]
    then
	/usr/bin/time -v athena.py $ATHENA_OPTIONS ; echo $? > retcode.tmp
    else
        /usr/bin/time -v athena.py $ATHENA_OPTIONS input.py; echo $? > retcode.tmp
    fi
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
fi

GANGATIME4=`date +'%s'`

# Making OutpuFiles.xml
echo 'Making OutputFiles.xml'

cat <<EOF > adler32.py
#!/usr/bin/python

import sys
import zlib
import getopt

def help():
    print 'adler32.py [OPTIONS] [FILE] [[FILE2] [FILE3]...]'
    print ''
    print 'Prints the adler32 checksum(s) of one or more files.'
    print 'Note that adler32 is nearly as reliable as CRC32'
    print 'and much faster to compute.'

short_options='h'
long_options=['help']

try:
    opts, args = getopt.getopt(sys.argv[1:],
                               short_options,
                               long_options,)
except getopt.GetoptError,x:
    print x
    help()
    sys.exit(2)

quiet = False    
if len(opts) > 0:
    for cmd, arg in opts:
        if cmd in ('--help','-h'):
            help()
            sys.exit(0)

if len(args) == 0:
    help()
    sys.exit(1)
else:
    filenames = args

mb = 1024*1024

for filename in filenames:
    try:
        file=open(filename,'r')
        ad32=1
        while True:
            str = file.read(mb)
            if len(str) > 0:
                ad32=zlib.adler32(str,ad32)
            else:
                break
        # correct for bug 32 bit zlib
        if ad32 < 0 :
            ad32 += 2**32
        res = '%8s' % (hex(ad32).lstrip('0x'))
        res = res.replace(' ','0')
        print res
        file.close()
    except Exception,x:
        print x
EOF


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
      #
      # use the python in the Atlas release and not the system python!
      #
      ad32=`python adler32.py $file`
      returncode=$?
      if [ $returncode -ne 0 ] ; then
	  echo "adler32 does not work or is missing"
	  echo "adler32 does not work or is missing" >| wrapper.log
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
      echo "  <ad32>$ad32</ad32>" >> OutputFiles.xml
      echo "  <lcn>${lcn[$i]}</lcn>" >> OutputFiles.xml
      echo "  <dataset>${dataset[$i]}</dataset>" >> OutputFiles.xml
      echo "</file>" >> OutputFiles.xml
    fi
  done
  echo "</outputfiles>" >> OutputFiles.xml
fi

echo "OutputFiles.xml"
cat OutputFiles.xml

GANGATIME5=`date +'%s'` 

echo "GANGATIME1=$GANGATIME1"
echo "GANGATIME2=$GANGATIME2" 
echo "GANGATIME3=$GANGATIME3"   
echo "GANGATIME4=$GANGATIME4" 
echo "GANGATIME5=$GANGATIME5" 

exit $retcode
