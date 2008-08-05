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
	    echo "Stage-in timeout"
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

stageInLCG(){
    # args: inputfile, guid and list of potential lfcs.
    # loop over lfc list, resolve guid into list of replicas, then try each replica until a download is successful, using timeout.
    LFN=$1
    GUID=$2
    shift 2;
    LFCS=$@
    for lfc in $LFCS; do
	export LFC_HOST=$lfc
	turls=`lcg-lr --vo atlas $GUID`
	echo "found the following replicas in $lfc: $turls"
	for turl in $turls; do
	    echo $turl
	    timeout 1 600 "lcg-cp --vo atlas $turl file:$PWD/$LFN"
	    # note to self: timeout value (second arg) should be proportional to requested file size...
	    status=$?
	    if [ $status -eq 0 ]; then
		echo "got file $LFN from $lfc, leaving loop"
		return 0;
	    else
	        echo "failed to download $turl, looping to next replica"
	    fi
	done
    done
    return 410302; # failed to get any replica...
}
stageInNG(){
    echo "Nordugrid stage-in handled by backend. Just checking that the requested files are available";
    return 0;
}
stageInOSG(){
    echo "OSG stage-in performed by backend. Just checking that the requested files are available";
    return 0;
}
stageInLocal(){
    # args: inputfile name and its location (full path).
    cp $2 $1;
    return 0;
}
stageInCastor(){
    # args: inputfile name and its location (full path).
    # Protect download with timeout as staging from tape might take a while...
    stager_get -M $2 
    sleep 60
    stager_qry -M $2
    sleep 60
    timeout 1 600 "rfcp $2 $1"
}


######
# new staging...
######

# Stage-in is backend dependant, 


eval $INPUTTURLS
eval $INPUTFILES
eval $INPUTLFCS
echo ${lfn[@]}
bash --version

# main loop

for ((i=0;i<${#lfn[@]};i++)); do
#for i in ${!lfn[@]}; do # not everybody is up to date with bash, which is a shame really...
 echo "entry:"$i", lfn is "${lfn[${i}]}", turl is "${turl[${i}]}" from lfc "${lfc[${i}]}

 INPUTFILE=${lfn[${i}]}
 if [ -z $INPUTFILE ] ; then
    echo "no lfn found, potential error here..."
    continue
 fi
 INPUTTURL=${turl[${i}]}
 if [ -z $INPUTTURL ]; then
    echo "no transport string for the file, cannot get it. Skipping"
    continue
 fi
 echo "==============================="
 echo "   STAGING INPUT FILES    "
 echo "==============================="
 echo
  /usr/bin/env date
 echo
 # now checking the backend
 case "$BACKEND" in
    'LCG')
    stageInLCG $INPUTFILE $INPUTTURL $lfc;
    ;;
    'NG')
    stageInNG;
    ;;
    'Panda')
    stageInOSG;
    ;;
    'batch')
    stageInLocal $INPUTFILE `echo $INPUTTURL | cut -d ":" -f 2`;
    ;;
    'castor')
    stageInCastor $INPUTFILE  `echo $INPUTTURL | cut -d ":" -f 2`;
    ;;
    *)
    echo "Error, wrong value for BACKEND: $BACKEND. Must abort";
    return 11;
    ;;
    
 esac
 if [ ! -s $INPUTFILE ] ; then
    echo "Unable to stage-in input file $file"
    exit 410302
 fi
 echo "doing pool insert"
    pool_insertFileToCatalog $INPUTFILE
    cat PoolFileCatalog.xml 2> /dev/null

done
