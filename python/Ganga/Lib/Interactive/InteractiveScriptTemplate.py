#!/usr/bin/env python
from __future__ import print_function
# Interactive job wrapper created by Ganga
# ###CONSTRUCT_TIME###

###WNSANDBOX_SOURCE###

import os
import sys
import time
import glob
import mimetypes

sys.path.insert( 0, '###GANGA_PYTHONPATH###' )

statfileName = os.path.join( '###OUTPUTDIR###', '__jobstatus__' )
try:
   statfile = open( statfileName, 'w' )
except IOError, x:
   print('ERROR: Unable to write status file: %s' % statfileName)
   print('ERROR: ',x)
   raise

idfileName = os.path.join( '###OUTPUTDIR###', '__id__' )
try:
   idfile = open( idfileName, 'w' )
except IOError, x:
   print('ERROR: Unable to write id file: %s' % idfileName)
   print('ERROR: ',x)
   raise
finally:
   idfile.close()

timeString = time.strftime( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )
statfile.write( 'START: ' + timeString + os.linesep )

if not os.path.exists( '###WORKDIR###'):
    os.makedirs('###WORKDIR###')
os.chdir( '###WORKDIR###' )
for inFile in ###IN_BOX###:
    if mimetypes.guess_type(inFile)[1] in ['gzip', 'bzip2']:
        getPackedInputSandbox( inFile )

###WN_INPUTFILES###

###WN_INPUTDATA###

for key, value in ###JOBCONFIG_ENV###.iteritems():
   os.environ[ key ] = value

pyCommandList = [
   'import os',
   'idfileName = \'%s\'' % idfileName,
   'idfile = open( idfileName, \'a\' )',
   'idfile.write( \'PID: \' + str( os.getppid() ) )',
   'idfile.flush()',
   'idfile.close()' ]
pyCommandString = ';'.join( pyCommandList )

commandStr = '\'' + str('###EXE_STRING### ###ARG_STRING###') + '\''
#commandStr = commandStr.replace('"', '\'')

commandList = [
   'python -c "%s' % pyCommandString,
   'os.system(%s)"' % commandStr  ]
commandString = ';'.join( commandList )

result = os.system( '%s' % commandString )

###WN_POSTPROCESSING###
for patternToZip in ###PATTERNS_TO_ZIP###:
   for currentFile in glob.glob(patternToZip):
      os.system('gzip %s' % currentFile)

createOutputSandbox( ###OUTPUT_SANDBOX_PATTERNS###, None, '###OUTPUTDIR###' )

statfile.write( 'EXITCODE: ' + str( result >> 8 ) + os.linesep )
timeString = time.strftime( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )
statfile.write( 'STOP: ' + timeString + os.linesep )
statfile.flush()
statfile.close()


