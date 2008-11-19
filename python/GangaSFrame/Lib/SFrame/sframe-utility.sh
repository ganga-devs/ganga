#! /bin/sh
#
# Utility functions to configure SFrame
#
#
# marcello.barisonzi@desy.de

compile_SFrame() {
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
    sframe_main ganga_$SFRAME_XML; echo $? > retcode.tmp
}
