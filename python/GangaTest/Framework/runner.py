################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: runner.py,v 1.3 2008-11-26 08:31:33 moscicki Exp $
# runner.py is a Python module used to run Ganga test-cases
#
# Copyright (C) 2003-2007 The Ganga Project
#
# This file is part of Ganga. 
#
# Ganga is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Ganga is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################

__version__ = "1.0"
__author__="Adrian.Muraru[at]cern[dot]ch"

import os
import sys

## Configuration
from Ganga.Utility.Config import getConfig, makeConfig
from Ganga.Utility.files import previous_dir
from Ganga.Utility.logging import getLogger

myFullPath =  os.path.abspath(os.path.dirname(__file__))
gangaReleaseTopDir = previous_dir(myFullPath,3)

# testing framework configuration properties
myConfig = makeConfig('TestingFramework','Configuration section for internal testing framework')

#Release testing mode:
# - xml/html reports are generated
# - coverage analysis tool enabled
myConfig.addOption('ReleaseTesting', False, '')

#enable/disable test-runner
myConfig.addOption('EnableTestRunner', True, 'enable/disable test-runner') 
#enable/disable html reporter
myConfig.addOption('EnableHTMLReporter', False, 'enable/disable html reporter')
#enable/disable xml differencer
myConfig.addOption('EnableXMLDifferencer','False', 'enable/disable xml differencer')
#search for local tests lookup
myConfig.addOption('SearchLocalTests', True, 'search for local tests lookup')
#search for tests packaged in release (PACKAGE/old_test dir) lookup
myConfig.addOption('SearchReleaseTests', True, 'search for tests packaged in release (PACKAGE/old_test dir) lookup')
#Coverage output
myConfig.addOption('CoverageReport', '', 'The file used to save the testing coverage statistics')

myConfig.addOption('timeout', 600, 'timeout when the test is forcibly stopped ')
myConfig.addOption('AutoCleanup',True,'cleanup the job registry before running the testcase')

myConfig.addOption('SchemaTesting', '', 'Set to True to enable Schema testing mode.')
myConfig.addOption('SchemaTest_ignore_obj', [], 'Objects to ignore when in Schema testing mode.')

if myConfig['SchemaTesting'] == '':
    myConfig.addOption('Config', 'default.ini', 'ganga configuration(s)')
else:
    myConfig.addOption('Config', 'Schema.ini', 'ganga configuration(s)')
    

if not myConfig['ReleaseTesting']:
    myConfig.addOption('OutputDir', os.path.expanduser('~/gangadir_testing'), '')
    myConfig.addOption('LogOutputDir', os.path.join(myConfig['OutputDir'],'output'), '')
    myConfig.addOption('ReportsOutputDir', os.path.join(myConfig['OutputDir'],'reports'), '')
    myConfig.addOption('RunID', '', '')
else:
    #for release testing
    myConfig.addOption('ReportsOutputDir', os.path.join(gangaReleaseTopDir,'reports'), '')
    myConfig.addOption('RunID', 'latest', '')
    myConfig.addOption('LogOutputDir', os.path.join(myConfig['ReportsOutputDir'],myConfig['RunID'],'output'), '')

#GangaTest logging config
loggerConfig = getConfig('Logging')
loggerConfig.addOption('GangaTest.Framework', "INFO", '')

#logger
myLogger=getLogger()

def start( config = myConfig , test_selection='Ganga.test.*', logger=myLogger):
    """
    """    
    import os
    #rtconfig = getConfig('TestingFramework')
    my_full_path =  os.path.abspath(os.path.dirname(__file__))
    #sys.stdout = UnbufferedStdout(sys.stdout)
    #sys.stderr = UnbufferedStdout(sys.stderr)    
       
    ##configure Ganga TestLoader

    # enable XML reporting in release mode
    pytf_reporting_opts=""
    if config['ReleaseTesting']:
        pytf_reporting_opts="--report-xml --report-outputdir=%(ReportsOutputDir)s --runid=%(RunID)s" % config        

    # output dir 
    if not os.path.exists(config['LogOutputDir']):
        os.makedirs(config['LogOutputDir'])
        
    # loader path
    global gangaReleaseTopDir
    pytf_loader_path = os.path.join(gangaReleaseTopDir,'python','GangaTest','Framework','loader.py')
    
    # loader args
    pytf_loader_args =[]   
    pytf_loader_args.append( '--loader-args=%s' % config['Config'])
    #pytf_loader_args.append( '--loader-args=%s/python' %  gangaReleaseTopDir)
    pytf_loader_args.append( '--loader-args=%s' %  gangaReleaseTopDir)
    pytf_loader_args.append( '--loader-args=%s' % int(config['ReleaseTesting']))    
    #output_dir
    pytf_loader_args.append( '--loader-args=%s' % config['LogOutputDir'])
    #unit-testing: on/off
    pytf_loader_args.append( '--loader-args=%s' % int(config['SearchLocalTests'])) 
    #system-testing: on/off
    pytf_loader_args.append( '--loader-args=%s' % int(config['SearchReleaseTests'])) 
    #Pass the report(report path + runid) path
    pytf_loader_args.append( '--loader-args=%s' % os.path.join(config['ReportsOutputDir'],config['RunID']))
    #pass the schmema version (if any) to test
    pytf_loader_args.append( '--loader-args=%s' % config['SchemaTesting'])
    
    #print("PYTF path %s config: %s" % (pytf_loader_path, pytf_loader_args))
    import sys
    sys.path.append(os.getenv('PYTF_TOP_DIR','').split(':')[0])
    sys.path.append(os.path.join(os.getenv('PYTF_TOP_DIR','').split(':')[0],'pytf'))
    import runTests
    runner_args = []
    
    runner_args.extend(pytf_reporting_opts.split())
    runner_args.extend(['--loader-path=%s' % pytf_loader_path])
    runner_args.extend(pytf_loader_args)
    runner_args.extend([test_selection])

    try:
        rc = runTests.main(logger,runner_args)
    except:
        rc = -9999
    return rc
                

def __getExternalLibPath( current_path ):
    """
    Get the external lib path needed to start individual test-cases
    This can be achieved also by setting RUNTIME_PATH=GangaTest in the configuration
    but in case of local tests this cannot be enforced, so we set the PYTHONPATH manually
    """
    
    external_path = []
    ganga_external_dir = previous_dir(current_path,5)
   
    ganga_external_dir = os.path.join(ganga_external_dir,'external')    

    ##set the libraries path to include external packages and test directory
    pyth_path = os.path.join(ganga_external_dir,'PYTF',PYTF_VERSION,'slc3_gcc323')
    if not os.path.isdir(pyth_path):
        pyth_path = '/afs/cern.ch/sw/ganga/external/PYTF/%s/slc3_gcc323' % PYTF_VERSION      
    if pyth_path not in sys.path:
        sys.path.insert(0, pyth_path)
    external_path.append(pyth_path)
                    
    figleaf_path = os.path.join(ganga_external_dir,'figleaf',FIGLEAF_VERSION,'slc3_gcc323')
    if not os.path.isdir(figleaf_path):
        figleaf_path = '/afs/cern.ch/sw/ganga/external/figleaf/%s/slc3_gcc323' % FIGLEAF_VERSION
    if figleaf_path not in sys.path:
        sys.path.insert(0, figleaf_path)
    external_path.append(figleaf_path)
    return external_path
                

if __name__ == '__main__':
    start()
    
#$Log: not supported by cvs2svn $
#Revision 1.2  2008/09/05 09:53:18  uegede
#New features:
#- Remote testing of Ganga thorugh the use of the GangaRobot
#- Ability to create test reports showing differences between releases.
#
#Revision 1.6.4.6  2007/12/18 13:12:48  amuraru
#stream-line the test-case execution code (use a single entry-point : Framework/driver.py)
#symplified the interface with Ganga bootstrap
#
#Revision 1.6.4.5  2007/11/08 13:23:02  amuraru
#added CoverageReport option definition
#
#Revision 1.6.4.4  2007/11/02 14:47:24  moscicki
#moved options from utils.py
#
#Revision 1.6.4.3  2007/10/31 13:37:21  amuraru
#updated to the new config subsystem in Ganga 5.0
#
#Revision 1.6.4.2  2007/10/30 14:20:22  moscicki
#makeConfig imported
#
#Revision 1.6.4.1  2007/10/12 13:56:31  moscicki
#merged with the new configuration subsystem
#
#Revision 1.6.6.1  2007/10/09 07:23:23  roma
#Migration to new Config
#
#Revision 1.6  2007/09/04 09:48:13  amuraru
#restructed code to allow two modes of running: developer mode and release mode
#
#Revision 1.5  2007/08/13 12:33:15  amuraru
#added verbosity control, avoid purging the used reporisotry when running local tests
#
#Revision 1.4  2007/05/21 16:01:20  amuraru
#use default website css style in test/coverage reports;
#disabled per test-case coverage report generation;
#other fixes
#
#Revision 1.3  2007/05/16 10:15:52  amuraru
#use ganga logger
#
#Revision 1.2  2007/05/15 09:58:36  amuraru
#html reporter updated
#
