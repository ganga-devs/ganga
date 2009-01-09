#! /bin/sh
#
# Utility functions to configure SFrame
#
#
# marcello.barisonzi@desy.de

mail_User() {
    if [ ! -z $USER_EMAIL ]
	then
	echo "Job " $JOBID ": " $1 | mail -s "GangaSFrame job $JOBID status" $USER_EMAIL
    fi   

}

compile_SFrame() {

    mail_User "Compiling SFrame"

    if [ ! -z $SFRAME_ARCHIVE ]
	then
	if [ -z $SFRAME_COMPILE ]
	    then  # simple extraction
	    tar xzf $SFRAME_ARCHIVE	
	    ln -s dev/JobConfig.dtd .
	else
	    ./compile_archive.py $SFRAME_ARCHIVE

	    rm -rf  $SFRAME_ARCHIVE

	    export SFRAME_DIR=`cat sfdir.tmp`
	    export SFRAME_BIN_PATH=$SFRAME_DIR/bin
	    export SFRAME_LIB_PATH=$SFRAME_DIR/lib
	    
	    export  PATH=$SFRAME_BIN_PATH:$PATH
	    export  LD_LIBRARY_PATH=$SFRAME_LIB_PATH:$LD_LIBRARY_PATH
	    
	    rm -rf sfdir.tmp

	fi
    fi

    echo "SFrame base directory: " $SFRAME_DIR

}

make_XML() {

    #mail_User "Modifying XML file"

    if [ -e PoolFileCatalog.xml ]
	then
	./pool2sframe.py $SFRAME_XML PoolFileCatalog.xml; echo $? > retcode.tmp
    elif [ -e input_files ]
	then
	./input2sframe.py $SFRAME_XML input_files; echo $? > retcode.tmp
    else
	echo "ERROR: cannot prepare xml config file"
    fi
}

run_SFrame() {

    mail_User "Running SFrame"

    sframe_main ganga_$SFRAME_XML; echo $? > retcode.tmp
    retcode=`cat retcode.tmp`

    if [ $retcode -eq 0 ]
	then
	mail_User "End of job. Job succeeded."
    else
	mail_User "End of job. Job failed."
    fi

}
