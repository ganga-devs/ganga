# Set environment for db_browser.py

setenv PATH ${PATH}:/opt/glite/bin:~/utils
       
if ( $?LD_LIBRARY_PATH ) then
	setenv LD_LIBRARY_PATH /afs/cern.ch/sw/ganga/external/Python/2.3.4/slc3_gcc323/lib:${LD_LIBRARY_PATH}
else
	setenv LD_LIBRARY_PATH /afs/cern.ch/sw/ganga/external/Python/2.3.4/slc3_gcc323/lib
endif

setenv QTDIR /afs/cern.ch/sw/lcg/external/pyqt/3.13_python234/slc3_gcc323
setenv LD_LIBRARY_PATH $QTDIR/lib:${LD_LIBRARY_PATH}
setenv PATH ${QTDIR}/bin:${PATH}

if ( $?PYTHONPATH ) then
	setenv PYTHONPATH ${QTDIR}/lib/python2.3.4/site-packages:${PYTHONPATH}
else
	setenv PYTHONPATH ${QTDIR}/lib/python2.3.4/site-packages
endif
