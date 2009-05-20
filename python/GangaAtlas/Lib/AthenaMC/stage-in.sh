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
    LFNS=$1   
    INPUTDSET=$2
    shift 2;
    SITES=$@
    dq2get=`which dq2-get`
    py32=`which python32`
    matchDB=`echo $LFNS | grep DBRelease`
    echo "test matching",$LFNS,$matchDB,$newDB_location
    if [ ! -z "$newDB_location" -a ! -z "$matchDB" ]; then
	echo "DBRelease already set, NOT downloading the archive: $LFNS"
	return 0
    fi

    for site in $SITES; do
        echo "Attempting getting data from $site"
	cmd="$dq2get --client-id=ganga -a -d -s $site -D -f $LFNS $INPUTDSET" 
	if [ ! -z "$py32" ]; then
	    cmd="python32 "$cmd
	fi
	echo $cmd
	$cmd
	status=$?
	if [ $status -eq 0 -a -s "$LFNS" ]; then
	    echo "$LFNS downloaded succesfully"
	    #mv $INPUTDSET*/* .  # -D flag should make this block irrelevant and trigger an harmless error message
	    break
	fi
	echo "attempt failed."
    done
    ls -l 

    if [ ! -s "$LFNS" ]; then
	echo "Missing LFN: $LFNS"
	return 111; # failed to get any replica...
    fi

    return 0
#    GUID=$2
#    shift 2;
#    LFCS=$@
#    for lfc in $LFCS; do
#	export LFC_HOST=$lfc
#	turls=`lcg-lr --vo atlas $GUID`
#	echo "found the following replicas in $lfc: $turls"
#	for turl in $turls; do
#	    echo $turl
#	    timeout 1 800 "lcg-cp -t 600 --vo atlas $turl file:$PWD/$LFN"
#	    # note to self: timeout value (second arg) should be proportional to requested file size...
#	    status=$?
#	    if [ $status -eq 0 ]; then
#		echo "got file $LFN from $lfc, leaving loop"
#		return 0;
#	    fi
#	       #echo "failed to download $turl, looping to next replica"
#	    echo "failed to download $turl, trying with srmv2 token:"
#	    timeout 1 800 "lcg-cp -t 600 -T srmv2 --vo atlas $turl file:$PWD/$LFN"
#	    if [ $status -eq 0 ]; then
#		echo "got file $LFN from $lfc, leaving loop"
#		return 0;
#	    fi
#	    echo "failed to download $turl, looping to next replica"
#	done
#    done

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
eval $INPUTDATASETS
eval $INPUTSITES
echo ${lfn[@]}
bash --version
##
echo "Setting up DQ2 tools"
source ${VO_ATLAS_SW_DIR}/ddm/latest/setup.sh
echo "site's SE is $DQ2_LOCAL_SITE_ID"

# main loop

for ((i=0;i<${#lfn[@]};i++)); do
#for i in ${!lfn[@]}; do # not everybody is up to date with bash, which is a shame really...
 echo "entry:"$i", lfn is "${lfn[${i}]}", from dataset "${dset[${i}]}" from site "${site[${i}]}

 INPUTFILE=${lfn[${i}]}
 if [ -z "$INPUTFILE" ] ; then
    echo "no lfn found, potential error here..."
    continue
 fi

 INPUTDSET=${dset[${i}]}
 if [ -z "$INPUTDSET" ] ; then
    echo "no dataset found, potential error here..."
    continue
 fi
 SITE=${site[${i}]}
 if [ -z "$SITE" ] ; then
    echo "no site found, potential error here..."
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
#    stageInLCG $INPUTFILE $INPUTTURL ${lfc[${i}]};
    stageInLCG $INPUTFILE $INPUTDSET $SITE
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

# echo "doing pool insert"
#    pool_insertFileToCatalog $INPUTFILE
#    cat PoolFileCatalog.xml 2> /dev/null

done
