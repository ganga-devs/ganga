#!/bin/sh

timeout() {
    touch tmpflag
    icount=0
    ntry=0
    maxntry=$1
    TIMEOUT=$2
    shift 2
    while [ $ntry -lt $maxntry ]; do
	echo "executing $@"
	($@ ; echo $? > tmpflag )&
	PID=$!
	while [ $icount -lt $TIMEOUT ]; do
	    isalive=`ps -p $PID | grep $PID`
	    echo $isalive
	    if [ -z "$isalive" ] ; then
		echo "process $PID is finished, leaving"
		stgstatus=`cat tmpflag`
		break
	    fi
	    sleep 30
	    let icount=$icount+30
	done
	if [ ! -z "$isalive" ] ; then
	    echo "Stage-out timeout"
	    kill -9 $PID
	    let ntry=$ntry+1
	    let icount=0
	else
	    rm tmpflag
	    return $stgstatus
	fi
    done
    rm tmpflag
    echo "tried $ntry times, aborting now"
    return 1
}

stageOutLCG(){
    dir=`dirname $1`
    lcn=`echo $dir | sed -e 's:/::'`
    file=`basename $1`
    # check DATASETLOCATION
    if [ -z "$OUTLFC" -o -z "$OUTPUT_LOCATION" ]; then
       # save as before, T_SE must be a proper storage element...
       export LFC_HOST="prod-lfc-atlas-local.cern.ch"
       DEST=$T_SE # do not know a priori if the se is a classic se (sfn://) or srm (srm://). So we cannot specify the destination area, just the SE. Use the lfn to get the pfn.
       OUTSITE="CERNCAF"    
    else
       export LFC_HOST=$OUTLFC
       DEST=$OUTPUT_LOCATION$lcn/$file.$TIMESTAMP.$OUTPUT_JOBID
    fi

    stflag=""
    if [ ! -z "$SPACETOKEN" ]; then
	echo "Using srmv2 user space token"
	stflag="-s $SPACETOKEN"
    fi
    bstflag=""
    if [ ! -z "$BACKUPTOKEN" ]; then
	echo "Using srmv2 user space token"
	bstflag="-s $BACKUPTOKEN"
    fi
    LFN="/grid/atlas/dq2/$lcn/$file.$TIMESTAMP.$OUTPUT_JOBID"
    # cannot use FClistGUID as the athena setup has been removed. Try something different...
    guid=`FClistGUID $file` # ensure that the guid from the pool catalog is used for the LFC registration
    guidflag="-g $guid"
    if [ -z "$guid" ]; then
	echo "no guid. Hope this is a log file: $file"
        guidflag=""
    fi
       
    filesize=$(stat -c%s "$file")
    let timelim=($filesize/50000)+10
    if [ $timelim -lt 900 ]; then
	timelim=900
    fi
    echo "TIMEOUT set to $timelim"

    lfc-mkdir -p /grid/atlas/dq2/$lcn

    stageoutcmd="lcg-cr --vo atlas -v $stflag -d $DEST -l $LFN $guidflag file:$PWD/$file"
    timeout 1 $timelim $stageoutcmd
    status=$?
    if [ $status -ne 0 -a ! -z "$BACKUP" ]; then
	echo "Failed to upload to initial target destination $DEST, trying back up $BACKUP";
        if [ ! -z "$OUTLFC2" ]; then
           export LFC_HOST=$OUTLFC2
        fi
	stageoutcmd="lcg-cr --vo atlas -v $bstflag -d $BACKUP -l $LFN $guidflag file:$PWD/$file"
	timeout 1 $timelim $stageoutcmd
	status=$?
     fi
    if [ $status -ne 0 ]; then
	echo "Stage-out attempt(s) failed, bailing out of here..."
	return 431303;
    fi

    oldguid=$guid
    guid=`lcg-lg --vo atlas lfn:$LFN | grep "guid\:" | sed -e "s:.*guid\:::" `
    echo "GUIDS: $oldguid vs guid:$guid"
    export dataset=`echo $lcn | sed -e 's:/:\.:g'`
    filesize=`ls -l $file | awk '{ print $5}'`
    md5sumfile=`md5sum $file | awk '{ print $1}'`
    chmod +x adler32.py
    adler32=`./adler32.py $file`
    if [ -z "$adler32" ]; then
	echo "using local adler32"
	adler32=`adler32 $file | tail -1`
    fi
    echo $guid >> output_guids
    echo $OUTSITE >> output_location
    if [ -z "$guid" ]; then
	echo "file was not registered in lfc, aborting"
	return 113
    fi
    if [ -z "$dataset" -o -z "$file" -o -z "$guid" -o -z "$filesize" -o -z "$adler32" -o -z "$OUTSITE" ]; then
	echo "Missing data, will have to skip registration on this one!"
	echo "one of the following is not set properly:"
	echo "DATASET=$dataset"
	echo "FILE=$file.$TIMESTAMP.$OUTPUT_JOBID"
	echo "guid=$guid"
	echo "file size=$filesize"
	echo "checksum (adler32)=$adler32"
	echo "site=$OUTSITE"
	return 431303;
    fi
    echo "$dataset,$file.$TIMESTAMP.$OUTPUT_JOBID,$guid,$filesize,$adler32,$OUTSITE" >> output_data
    return 0;
}
stageOutNG(){
    echo "Nordugrid stage out handled by backend. Doing nothing here..." 
    return 0;
}
stageOutOSG(){
    echo "OSG stage out performed by backend. Doing nothing here..."
    return 0;
}
stageOutLocal(){
    # args: inputfile name and its location (full path).
    file=`basename $1`
    dir=`dirname $1`
    mkdir -p $dir
    cp $file $1.$TIMESTAMP.$OUTPUT_JOBID;
    return $?;
}
stageOutCastor(){
    # args: inputfile name and its location (full path).
    file=`basename $1`
    dir=`dirname $1`
    rfmkdir $dir
    rfcp $file $1.$TIMESTAMP.$OUTPUT_JOBID;
    return $?;
}

FClistGUID(){
    # poor man's FClistGUID...
    path=$1
    if [ -z "$path" ]; then
	return 0
    fi
    ispoolfile=`cat PoolFileCatalog.xml | grep $path`
    if [ -z "$ispoolfile" ]; then
	return 0   # file is not listed in PoolFileCatalog: exit now so we do not print any random guid for it.
    fi
    for line in `cat PoolFileCatalog.xml`; do
        filename=`echo $line | grep "name"`
        testguid=`echo $line | grep "ID="`
	if [ ! -z "$testguid" ]; then 
	    guid=`echo $testguid | sed -e 's:ID=\":guid\::' | sed -e 's:">::'`
        fi
	foundit=`echo $filename | grep $path`
	if [ ! -z "$foundit" ]; then
	    notfound=0;
	    break;
	fi
     done
     echo $guid
     return 0
}
echo "==============================="
echo "   REGISTERING OUTPUT FILES    "
echo "==============================="

export TIMESTAMP=`/usr/bin/env date +%d%m%y`

touch output_location output_guids output_data
retcode=0

# modifying the "BACKEND" according to T_SE value:
TESTSE=`echo $T_SE | tr 'A-Z' 'a-z'`

case "$TESTSE" in
    "local")
    export BACKEND_DATA="batch"
    ;;
    "castor")
    export BACKEND_DATA="castor"
    ;;
    *)
    ;;
esac 
if [ -z "$BACKEND_DATA" ]; then
   export BACKEND_DATA=$BACKEND
fi

cat PoolFileCatalog.xml
for path in $OUTPUTFILES; do
    echo $path
    dir=`dirname $path`
    lcn=`echo $dir | sed -e 's:/::'`
    file=`basename $path`
    echo ">>> STAGE-OUT $file"

    if [ ! -f "$file" ]; then
	echo "$file not found, skipping"
	continue
    fi
    if [ ! -s "$file" ]; then
	echo "$file is empty, skipping"
	continue
    fi
    case "$BACKEND_DATA" in
	'LCG')
	    stageOutLCG $path
	    ;;
	'NG')
	    stageOutNG;
	    ;;
	'Panda')
	    stageOutOSG;
	    ;;
	'batch')
	    stageOutLocal $path;
	    ;;
	'Local')
	    stageOutLocal $path;
	    ;;
	'castor')
	    stageOutCastor $path;
	    ;;
	 *)
	    echo "Error,  wrong value for BACKEND: $BACKEND. Must abort";
	    return 11;
	;;
    
    esac
    status=$?
    echo "Return Status code: $status"
    echo "output data:"
    cat output_data
    echo
    /usr/bin/env date
    echo

done

return 0
