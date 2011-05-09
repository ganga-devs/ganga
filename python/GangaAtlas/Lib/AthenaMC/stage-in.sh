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

stageInDQ2(){
    LFNS=$1   
    INPUTDSET=$2
    shift 2;
    dq2get=`which dq2-get`
#    py32=`which python32`
 
    matchDB=`echo $LFNS | grep DBRelease`
    echo "test matching",$LFNS,$matchDB,$newDB_location
    if [ ! -z "$newDB_location" -a ! -z "$matchDB" ]; then
	echo "DBRelease already set, NOT downloading the archive: $LFNS"
	return 0
    fi
    if [ -z "$DQ2_LOCAL_SITE_ID" ]; then
	echo "Error in local dq2 setup, DQ2_LOCAL_SITE_ID not set. Aborting"
        return 12
    fi

    cmd="$dq2get --client-id=ganga -a -d -D -f $LFNS $INPUTDSET" 
    echo $cmd
    $cmd
    status=$?
    if [ $status -eq 0 -a -s "$LFNS" ]; then
       echo "$LFNS downloaded succesfully"
    else 
       echo "attempt failed. Trying alternative command line:"
       # checking presence of atlas python:
       atlasPythonBin=`ls -d ${VO_ATLAS_SW_DIR}/prod/releases/*/sw/lcg/external/Python/2.5.4/slc4_ia32_gcc34/bin | head -1`
       if [ ! -z "$atlasPythonBin" ]; then
          echo "Atlas python2.5 detected, setting it up"
          export oldPATH=$PATH
	  export oldLDLBPATH=$LD_LIBRARY_PATH
	  export PATH=$atlasPythonBin:$PATH
	  atlasPythonLib=`echo $atlasPythonBin | sed -e "s:bin:lib:"`
	  export LD_LIBRARY_PATH=$atlasPythonLib:$LD_LIBRARY_PATH
	  cmd="python "$cmd
          echo $cmd
	  $cmd
          status=$?
        else
          echo "Could not find atlas python, aborting."
	fi
    fi
    if [ ! -z "$oldPATH" ]; then
	# restore defaults
	export PATH=$oldPATH
	export LD_LIBRARY_PATH=$oldLDLBPATH
    fi
    if [ $status -eq 0 -a -s "$LFNS" ]; then
	echo "$LFNS downloaded succesfully"
    fi
    ls -l 
    if [ ! -s "$LFNS" ]; then
	echo "Missing LFN: $LFNS"
	if [ -e "$LFNS" ]; then
	    echo "empty leftover from failed attempt detected, removing it"
	    rm $LFNS 
	fi
	return 111; # failed to get any replica...
    fi
    return 0
}

stageInLCG(){
    # args: inputfile, guid and list of potential lfcs.
    # loop over lfc list, resolve guid into list of replicas, then try each replica until a download is successful, using timeout.
    LFN=$1   
    GUID=$2
    lfc=$3

    export LFC_HOST=$lfc
    turls=`lcg-lr --vo atlas $GUID`
    if [ -z "$turls" ]; then
       echo "error: no replicas found for guid: $GUID at LFC $lfc. Aborting" 
       return 11
    fi
    echo "found the following replicas in $lfc: $turls"
    for turl in $turls; do
	echo $turl
	timeout 1 800 "lcg-cp -t 600 -T srmv2 --vo atlas $turl file:$PWD/$LFN"
	if [ $status -eq 0 ]; then
           echo "got file $LFN from $lfc, leaving loop"
	   return 0;
	fi
	echo "failed to download $turl, looping to next replica"
    done

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

##
if [ [ -z "$DQ2_HOME" -o -z "$DQ2_LOCAL_SITE_ID" ] -a [ "$BACKEND" != 'Local' ] ]; then
    echo "Setting up DQ2 tools"
    source ${VO_ATLAS_SW_DIR}/ddm/latest/setup.sh
    echo "site's DQ2 ID is $DQ2_LOCAL_SITE_ID"
fi
# main loop
echo "LFC is $OUTLFC"
for ((i=0;i<${#lfn[@]};i++)); do
#for i in ${!lfn[@]}; do # not everybody is up to date with bash, which is a shame really...

 INPUTFILE=${lfn[${i}]}
 INPUTDSET=${dset[${i}]}
# SITE=${site[${i}]}
 TURL=${turl[${i}]}
 echo "entry: $i , lfn is $INPUTFILE , from dataset $INPUTDSET, TURL is $TURL ."

   
 echo "==============================="
 echo "   STAGING INPUT FILES    "
 echo "==============================="
 echo
  /usr/bin/env date
 echo
 # now checking the backend
 case "$BACKEND" in
    'LCG')
    if [ -z "$INPUTFILE" ]; then
	echo "input file name missing, Abort"
	return 12
    fi
    status=1
    if [ ! -z "$INPUTDSET" ]; then
        echo "Using DQ2 to get input files" 
	stageInDQ2 $INPUTFILE $INPUTDSET # uses dq2
	status=$?
    else
       echo "Missing data , cannot use dq2"
    fi
    if [ $status -ne 0 ];then
        if [ ! -z "$DQ2_LOCAL_SITE_ID" ]; then
	    echo "DQ2 download failed"
        fi
        if [ ! -z "$TURL" ]; then
	    echo "reverting to lcg-cp"
	    #    stageInLCG $INPUTFILE $GUID ${lfc[${i}]};
	    stageInLCG $INPUTFILE $TURL $OUTLFC;
	    status=$?
        else
           echo "missing guids, cannot use lcg-cp"
	fi
    fi
    if [ $status -ne 0 ]; then
	echo "Completely failed to download one input file, aborting"
	exit $status
    fi
    ;;
    'NG')
    stageInNG;
    ;;
    'Panda')
    stageInOSG;
    ;;
    'batch')
    stageInLocal $INPUTFILE `echo $TURL | cut -d ":" -f 2`;
    status=$?
    ;;
    'Local')
    stageInLocal $INPUTFILE $INPUTDSET/$INPUTFILE;
    ;;
    'castor')
    stageInCastor $INPUTFILE  `echo $TURL | cut -d ":" -f 2`;
    ;;
    *)
    echo "Error, wrong value for BACKEND: $BACKEND. Must abort";
    return 11;
    ;;
    
 esac
echo $status
if [ $status -ne 0 ];then
   echo "Error in stage-in, aborting"
   exit $status
fi



done
