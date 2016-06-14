#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ============================================================================
## @file
#  Application handler for Ostap
#
# The user specifies:
#  - script file(s) which contain Ostap scripts,
#  - set of command line arguments
#  
# At least one 'script' or non-empty list of commands is required 
#
# The application executes the following line:
# @code   
# % ostap {scripts} {arguments} --import {imports} --no-color --batch
# @endcode    
# e.g.
# @code 
# % bender script.py  --with-context -p5 --no-color --no-canvas --batch
# @endcode
#
# 
# @code
# my_script  = "~/cmtuser/tests/my_batch.py"
# my_app     = Ostap (
#    version      = 'v28r3'       ,
#    scripts      =   my_script   ,
#    arguments    = [
#      ] , 
#    commands     = [ 'print dir()']
#      ) 
# @endcode
#
# @author Vladimir ROMANOVSKY  Vladimir.Romanovskiy@cern.ch
# @author Vanya BELYAEV        Ivan.Belyaev@itep.ru
# @fate 2016-03-16
#
# Version           $Revision:$
# Last Modification $Date:$
#                by $Author:$
# =============================================================================
"""The application handler for Ostap

The user specifies:
- script file(s) which contain Ostap scripts,
- set of command line arguments

At least one 'script' or non-empty list of commands is required 

The application executes the following line:

% ostap {scripts} {arguments} --import {imports} --no-color --batch

e.g.
% ostap script.py  --with-context -p5 --no-color --no-canvas --batch

>>> my_script  = '~/cmtuser/tests/my_batch.py'
>>> my_app     = Ostap (
...    version      = 'v28r3'       ,
...    scripts      =   my_script   ,
...    arguments    = [
...      '--with-context'           , ## execute the script files ``with-context''
...      '--no-canvas'              , ## do not create canvas 
...      ] , 
...    commands     = [ 'print dir()']
... ) 
"""
# =============================================================================
__date__    = '2016-03-16'
__version__ = '$Revision:$'
__author__  = 'Vladimir ROMANOVSKY, Vanya BELYAEV'
# =============================================================================
import os
from   os.path import split, join
from   Ganga.GPIDev.Schema.Schema             import FileItem, SimpleItem
from   Ganga.GPIDev.Lib.File                  import File
from   Ganga.Utility.util                     import unique
from   Ganga.Core                             import ApplicationConfigurationError
from   Ganga.GPIDev.Lib.File.FileBuffer       import FileBuffer
from   GangaGaudi.Lib.Applications.GaudiBase  import GaudiBase
from   GangaGaudi.Lib.Applications.GaudiUtils import fillPackedSandbox, gzipFile
from   Ganga.Utility.files                    import expandfilename, fullpath
from   Ganga.Utility.Config                   import getConfig
from   AppsBaseUtils                          import guess_version
#
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

# Added for XML PostProcessing
from GangaLHCb.Lib.Applications import XMLPostProcessor

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

# =============================================================================
## the actual wrapper script to execute 
# =============================================================================
layout = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
from distutils.spawn import find_executable
ostap_script = find_executable('ostap')
import sys
# =============================================================================
## redefine arguments 
# =============================================================================
sys.argv += {scripts}        
sys.argv += {arguments}
sys.argv += [ '--no-color'  , '--batch' ]
sys.argv += [ '--command' ] + {command} 
##
# =============================================================================
## execute bender script 
# =============================================================================
execfile(ostap_script)
# =============================================================================
# The END
# =============================================================================
"""
# =============================================================================
# @class Ostap
# Application handler for Ostap
#
# The user specifies:
#  - script file(s) which contain BenderScript scripts,
#  - configuration/Configurables files, to be used for 'importOptions'
#  - set of command-line arguments
# 
# At least one 'script' or 'import' file is required.
#
# The application executes the following line:
# @code    
# % ostap {scripts} {arguments} --no-color --no-castor --batch
# @endcodce    
# e.g.
# @code 
# % ostap script1.py  --no-color --batch
# @endcode 
#
# @author Vladimir ROMANOVSKY  Vladimir.Romanovskiy@cern.ch
# @author Vanya BELYAEV        Ivan.Belyaev@itep.ru 
class Ostap(GaudiBase):    
    """The application handler for Ostap
    
    The user specifies:
    - script file(s) which contain Ostap scripts,
    - set of command line arguments
    
    At least one 'script' or non-empty list of commands is required 
    
    The application executes the following line:
    
    % ostap {scripts} {arguments} --import {imports} --no-color --batch
    
    e.g.
    % ostap script.py -p5 --no-color --no-canvas --batch
    
    >>> my_script  = '~/cmtuser/tests/my_batch.py'
    >>> my_app     = Ostap (
    ...    project      = 'Analysis'    , ## 
    ...    version      = 'v16r1'       , ## version of Analysis
    ...    scripts      =   my_script   ,
    ...    arguments    = [
    ...      '--no-canvas'              , ## do not create canvas 
    ...      ] , 
    ...    commands     = [ 'print dir()']
    ... ) 
    """
    
    _name           = 'Ostap'
    _category       = 'applications'
    _exportmethods  = GaudiBase._exportmethods[:]
    _exportmethods += ['prepare', 'unprepare']
    
    _schema = GaudiBase._schema.inherit_copy()

    _schema.datadict['package'] = SimpleItem(
        defvalue = None,
        typelist = ['str', 'type(None)'],
        doc      = """The package the application belongs to (e.g. 'Sim', 'Phys')
        """
        )
    _schema.datadict['masterpackage'] = SimpleItem (
        defvalue = None,
        typelist = [ 'str', 'type(None)' ],
        doc      = """The package where your top level requirements file is read from.
        Can be written either as a path 'Tutorial/Analysis/v6r0' or in traditional notation 
        'Analysis v6r0 Tutorial'
        """
        )
    
    _schema.datadict['setupProjectOptions'] = SimpleItem(
        defvalue = ''     ,
        typelist = [ 'str', 'type(None)'],
        doc      = """Extra options to be passed onto the SetupProject command
        used for configuring the environment. As an example 
        setting it to '--dev' will give access to the DEV area. 
        For full documentation of the available options see 
        https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject
        """
        )
    
    _schema.datadict['scripts'] = FileItem(
        preparable      = 1      ,
        sequence        = 1      ,
        strict_sequence = 0      ,
        defvalue        = []     ,
        doc             = """The names of the script files to execute.
        A copy will be made at submission time
        """
        )
    
    _schema.datadict['commands'] = SimpleItem(
        defvalue = []      ,
        typelist = ['str'] ,
        sequence =  1      ,
        doc      = """The commands to be executed,
        e.g. [ 'run(10)' , 'print ls()' , 'print dir()' ]
        """
        )
    
    _schema.datadict['arguments'] = SimpleItem(
        defvalue = []      ,
        typelist = ['str'] ,
        sequence =  1      ,
        doc      = """List of command-line arguments for bender script,
        e.g. ['-w','-p5'], etc.
        For python scripts and configuration/Configurable files for 'importOptions'
        it is much better to use the separate options 'scripts' and 'imports'
        Following arguments will be appended automatically:  --no-color, --no-castor and --batch
        """
        )
    
    _schema.version.major += 2
    _schema.version.minor += 0
    
    def _get_default_version(self, gaudi_app):
        return guess_version(self, gaudi_app)

    def _auto__init__(self):
        if not self.appname : self.appname = 'Bender'  # default
        self._init()

    def _getshell(self):

        import EnvironFunctions
        return EnvironFunctions._getshell(self)

    def prepare(self, force=False):

        super(Ostap, self).prepare(force)
        self._check_inputs()

        
        share_dir = os.path.join (
            expandfilename ( getConfig('Configuration')['gangadir'] ) ,
            'shared'                            ,
            getConfig('Configuration')['user']  ,
            self.is_prepared.name               )
        
        input_sandbox_tar = os.path.join ( share_dir , 'inputsandbox',
                                           '_input_sandbox_%s.tar' % self.is_prepared.name ) 
        input_sandbox_tgz = os.path.join ( share_dir , 'inputsandbox',
                                           '_input_sandbox_%s.tgz' % self.is_prepared.name ) 
        
        fillPackedSandbox ( self.scripts      , input_sandbox_tar        ) 
        gzipFile          ( input_sandbox_tar , input_sandbox_tgz , True )
        
        # add the newly created shared directory into the metadata system if
        # the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()
        logger.debug("Finished Preparing Application in %s" % share_dir)

    def master_configure(self):
        return (None, StandardJobConfig())

    def configure(self, master_appconfig):
        
        ## strip leading and trailing blanks from arguments 
        self.arguments = [ a.strip() for a in self.arguments ]

        ## strip leading and trailing blanks from the command 
        self.commands  = [ a.strip() for a in self.commands  ]
        
        ## the script layout
        the_script    = layout.format (
            scripts   = [ os.path.join ( f.subdir , os.path.basename ( f.name ) ) for f in self.scripts ] , 
            arguments = self.arguments  ,
            command   = self.commands    
            )

        # add summary.xml
        outputsandbox_temp  = XMLPostProcessor._XMLJobFiles()
        outputsandbox_temp += unique(self.getJobObject().outputsandbox)
        outputsandbox       = unique(outputsandbox_temp)
        
        input_files  = []
        input_files += [ FileBuffer('gaudipython-wrapper.py', the_script ) ]
        logger.debug("Returning StandardJobConfig")
        return (None, StandardJobConfig(inputbox=input_files,
                                        outputbox=outputsandbox))
    
    def _check_inputs(self):
        """Checks the validity of user's entries for Ostap schema"""
        
        if not self.scripts and not self.commands and not self.arguments : 
            raise ApplicationConfigurationError(None, "Application scripts are not defined")
        
        if isinstance ( self.scripts , str ) : self.scripts = [ File ( self.scripts ) ]        
        for f in self.scripts : f.name = fullpath ( f.name )

    def postprocess(self):
        XMLPostProcessor.postprocess(self, logger)

# =============================================================================
# Associate the correct run-time handlers to GaudiPython for various backends.
# =============================================================================

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler

for backend in ['LSF', 'Interactive', 'PBS', 'SGE', 'Local', 'Condor', 'Remote']:
    allHandlers.add('Ostap', backend, LHCbGaudiRunTimeHandler)
allHandlers.add('Ostap', 'Dirac', LHCbGaudiDiracRunTimeHandler)


# =============================================================================
if '__main__' == __name__ :

    print 80*'*'  
    print __doc__ 
    print ' Author  : %s ' %  __author__   
    print ' Version : %s ' %  __version__  
    print ' Date    : %s ' %  __date__     
    print 80*'*'  

# =============================================================================
# The END 
# =============================================================================
