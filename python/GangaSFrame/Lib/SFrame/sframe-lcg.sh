#! /bin/sh -x
#
# Run SFrame on the Grid
#
# Following environment settings are required
#
# ATLAS/ARDA - Dietrich.Liko@cern.ch

retcode=0

#timestamping
python -c "import time; print time.gmtime()" >> timestamps.txt

# setup ATLAS software

echo "Setting up ATLAS sw release ..."

if [ -z $VO_ATLAS_SW_DIR ]
then
   echo "No ATLAS Software found." 1>&2
   # step exits with an error
   # WRAPLCG_WNCHEKC_SWENV
   exit 410103
fi

# save paths
save_path=${PATH}
save_ld_path=${LD_LIBRARY_PATH}

if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
then
    source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/setup.sh
elif [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ]
then
    source $VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE
fi

# helper routines to create the PoolFileCatalog

if [ ! -z $ATLASOutputDatasetLFC ]
then
    export LFC_HOST=$ATLASOutputDatasetLFC
else
    export LFC_HOST='prod-lfc-atlas-local.cern.ch'
fi
export LCG_CATALOG_TYPE=lfc

# improve dcap reading speed
export DCACHE_RAHEAD=TRUE
export DCACHE_RA_BUFFER=196608

# restore paths?
#export PATH=$save_path
#export LD_LIBRARY_PATH=$save_ld_path

# Remove /lib and /usr/lib from LD_LIBRARY_PATH

dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

# use ATLAS ROOT
export  ROOTSYS=`ls -d $SITEROOT/sw/lcg/external/root/*/*/root`
export  PATH=$ROOTSYS/bin:$PATH
export  LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH

# use ATLAS Python
pyth=`ls -d $SITEROOT/sw/lcg/external/Python/*/*`
export  PATH=$pyth/bin:$PATH
export  LD_LIBRARY_PATH=$pyth/lib:$LD_LIBRARY_PATH

#unpack SFrame

if [ ! -z $SFRAME_ARCHIVE ]
then
    if [ -z $SFRAME_COMPILE ]
    then  # simple extraction
	tar xzf $SFRAME_ARCHIVE
	rm -rf  $SFRAME_ARCHIVE
	ln -s dev/JobConfig.dtd .
    else
	./compile_archive.py $SFRAME_ARCHIVE
    fi

    export  PATH=./bin:$PATH
    export  LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH    
fi

# Determine SE type
if [ -e ganga_setype.py ]
then
    chmod +x ganga_setype.py
    export GANGA_SETYPE=`./ganga_setype.py`
fi

#export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH

#echo `python -V`

# Unpack dq2info.tar.gz
if [ -e dq2info.tar.gz ]
then
    tar xzf dq2info.tar.gz
fi

# prepare input data
if [ -e input_files ]
then 
    echo "Preparing input data ..."
    # DQ2Dataset
    if [ -e input_guids ] && [ -e ganga-stage-in-out-dq2.py ]
    then
	chmod +x ganga-stage-in-out-dq2.py
	chmod +x dq2_get
	./ganga-stage-in-out-dq2.py; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp	

    # ATLASDataset
    elif [ -e ganga-stagein-lfc.py ]
    then
	chmod +x ganga-stagein-lfc.py
	./ganga-stagein-lfc.py -v -i input_files; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp
    elif [ -e ganga-stagein.py ]
    then
	chmod +x ganga-stagein.py
	./ganga-stagein.py -v -i input_files; echo $? > retcode.tmp
	retcode=`cat retcode.tmp`
        rm -f retcode.tmp

    # Configuration error
    else
        # WRAPLCG_WNCHECK_UNSPEC
	retcode=410100
    fi
fi

#printenv

# run SFrame

if [ $retcode -eq 0 ]
then
    echo "Running SFrame ..."
    # Parse jobOption file
    if [ ! -z $OUTPUT_JOBID ] && [ -e ganga-joboption-parse.py ] && [ -e output_files ]
    then
	chmod +x ganga-joboption-parse.py
	./ganga-joboption-parse.py
    fi
    #./pool2sframe.py -s $SFRAME_XML -p PoolFileCatalog.xml; echo $? > retcode.tmp
    ./pool2sframe.py $SFRAME_XML PoolFileCatalog.xml; echo $? > retcode.tmp
    sframe_main ganga_$SFRAME_XML; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`
    rm -f retcode.tmp
fi

# timestamping
python -c "import time; print time.gmtime()" >> timestamps.txt

ls -rtla

# store output


if [ $retcode -eq 0 ]
then
    echo "Storing output data ..."
    if [ -e ganga-stage-in-out-dq2.py ] && [ -e output_files ] && [ ! -z $OUTPUT_DATASETNAME ]
    then
	chmod +x ganga-stage-in-out-dq2.py
	export DATASETTYPE=DQ2_OUT
	./ganga-stage-in-out-dq2.py --output=output_files.new	

    elif [ -n "$OUTPUT_LOCATION" -a -e output_files ]
    then
	cat output_files | while read filespec
	do
	  for file in $filespec
	    do
	    lcg-cr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
	    retcode=`cat retcode.tmp`
	    rm -f retcode.tmp
	    if [ $retcode -ne 0 ]
	    then
		sleep 120
		lcg-cr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
		retcode=`cat retcode.tmp`
		rm -f retcode.tmp
		if [ $retcode -ne 0 ]
		then
		    sleep 120
		    lcg-cr --vo atlas -d $OUTPUT_LOCATION/$file file:$PWD/$file >> output_guids; echo $? > retcode.tmp
		    retcode=`cat retcode.tmp`
		    rm -f retcode.tmp
		    if [ $retcode -ne 0 ]
		    then
			# WRAPLCG_STAGEOUT_LCGCR
			retcode=410403
		    fi
		fi
	    fi
	  done
	done
    fi
fi

exit $retcode
