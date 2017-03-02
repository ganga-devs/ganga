# -*- coding: utf-8 -*-
# =============================================================================
## @file BenderBox.py
#  Bender application for Ganga  (CMAKE-based version)
#  Actually there are three applications under the single hat
#  - BenderModule : classic Bender application: use a full power of bender 
#  - BenderScript : execute scripts in the context of ``bender'' environment
#  - Ostap : execute scripts in the context of ``ostap'' environment
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru       
# =============================================================================
"""Bender application for Ganga  (CMAKE-based version)
Actually there are three applications under the single hat
- BenderModule : classic Bender application: use a full power of bender 
- BenderScript : execute scripts in the context of ``bender'' environment
- Ostap : execute scripts in the context of ``ostap'' environment
"""
# =============================================================================
import os
from   Ganga.GPIDev.Lib.File.File      import ShareDir,File
from   Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from   Ganga.GPIDev.Schema             import Schema, Version, SimpleItem, GangaFileItem, FileItem, ComponentItem
from   Ganga.GPIDev.Base               import GangaObject

# =============================================================================
## @class Bender_module
#  Helper class to define the main properties of Bender applicatiom
#  - the name of Bender module to run
#  - dictionary of parameters to be forwarder to <code>configure</code> method
#  - number of event to process
#
#  Usage:
#  @code 
#  j.application = BenderBox ( tool = BenderModule ( module = 'the_path/the_module.py' , events = 1000 ) )
#  @endcode 
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru       
class BenderModule(GangaObject):
    """Helper class to define the main properties of Bender applicatiom
    - the name of Bender module to run
    - dictionary of parameters to be forwarder to <code>configure</code> method
    - number of event to process

    ======
    Usage:
    ======
    
    j.application = BenderBox ( tool = BenderModule ( module = 'the_path/the_module.py' , events = 1000 ) )

    """    
    _schema = Schema(Version(1, 0), {
        'module'    : FileItem   (
        defvalue  = File () ,
        doc       = """The file with Bender module. It is expected that module contains methods ``configure'' & ``run'' with the proper signatures""") , 
        'params'    : SimpleItem (
        defvalue  = {} ,
        typelist  = ['dict', 'str', 'int', 'bool', 'float'],
        doc       = """The dictionary of parameters to be forwarded to ``configure'' method of the supplied Bender module""") , 
        'events'    : SimpleItem ( defvalue=-1, typelist=['int'], doc= "Number of events to process"),
        })
    _category = 'BenderTool'
    _name     = 'BenderModule'
    
    layout    = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
importOptions('{datafile}')
from Gaudi.Configuration import EventSelector,FileCatalog
from copy import deepcopy
inputdata    = deepcopy( EventSelector() .Input    )
filecatalogs = deepcopy( FileCatalog  () .Catalogs )
EventSelector().Input    = [] ## reset it 
FileCatalog  ().Catalogs = [] ## reset it 
# =============================================================================
# configure Bender and run it 
# =============================================================================
import  sys
sys.path = sys.path + ['.']
import {modulename} as USERMODULE
USERMODULE.configure( inputdata , filecatalogs {paramstring})
USERMODULE.run({events})
# =============================================================================
# The END
# =============================================================================
"""

    ## Returns file names which should be copyed to shared area
    def inputfiles(self):
        "Returns file names which should be copyed to shared area"
        return [LocalFile(self.module.name),]

    ## Returns  wrapper file which run at WN
    def getWrapper(self, data_file):
        " Returns  wrapper file which run at WN"
        
        module_name  = os.path.split(self.module.name)[-1].split('.')[0]
        param_string =  ',params=%s' % self.params if self.params else ''
        
        the_script      = self.layout.format (
            datafile    = data_file ,
            modulename  = module_name ,
            paramstring = param_string ,
            events      = self.events
            )

        from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript
        the_script += getXMLSummaryScript()
        
        return the_script

# =============================================================================
## @class bender
#  Helper class to define the main properties of BenderScript application
#  - the scripts to be executed 
#  - the configuration scripts (aka ``options'') to be imported 
#  - bender commands to be executed
#  - other arguments
#
#  The scripts are executed within ``bender'' context
#  The configuration scripts (aka ``options'') are ``imported'' using <code>importOptions</code> costruction
# 
#  The actual command to be executed is:
#  @code 
#  > bender [ scripts [ scripts ...  --no-color [ arguments ] --import [ imports [ imports [ ... --no-castor --import=data.py --batch   [ --command  [ commands [ ... 
#  @endcode
#  Usage:
#  @code 
#  j.application = BenderBox ( tool = bender ( scripts   = ['path_to_script/the_script.py']  ,
#                                              commands  = [ 'print ls()' ]  ) )
#  @endcode 
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru       
class bender(GangaObject):
    """  Helper class to define the main properties of BenderScript application
    - the scripts to be executed 
    - the configuration scripts (``options'') to be imported 
    - bender commands to be executed
    - other arguments
    
    The scripts are executed within ``bender'' context
    The configuration scripts (aka ``options'') are ``imported'' using ``importOptions'' construction
    The auto-generated file data.py is imported after all other imports.

    The actual command to be executed is:
    > bender [ scripts [ scripts ...  --no-color [ arguments ] --import [ imports [ imports [ ... --no-castor --import=data.py --batch   [ --command  [ commands [ ...
    
    ======
    Usage:
    ======
    j.application = Bender ( tool = bender ( scripts   = ['path_to_script/the_script.py']  ,
                                             commands  = [ 'print ls()' ]  ) )                                                 
    """
    _schema = Schema(Version(1, 0), {
        'scripts'   : FileItem   (
        defvalue        = []      ,
        sequence        = 1       ,
        strict_sequence = 0       , 
        doc             = """The names of the script files to execute. A copy will be made at submission time. The script are executed within ``bender'' context"""),
        'imports'   : FileItem   (
        defvalue        = []      ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The names of the configurtaion scripts (ana ``options'') to be imported via ``importOptions''. A copy will be made at submission time"""),
        'commands'  : SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        =  1      ,
        doc             = """The bender commands to be executed, e.g. [ 'run(10)' , 'print ls()' , 'print dir()' ]"""), 
        'arguments' : SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        =  1      ,
        doc             = """The list of command-line arguments for bender script, e.g. ['-w','-p5'], etc. Following arguments will be appended automatically:  --no-color, --no-castor and --batch.""")
        })
    _category = 'BenderTool'
    _name     = 'bender'  

    layout = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
from distutils.spawn import find_executable
bender_script = find_executable('bender')
# =============================================================================
## redefine arguments 
# =============================================================================
sys.argv   += {scripts}        
sys.argv   += [ '--no-color' ] ## automatically added argument 
sys.argv   += {arguments}
sys.argv   += [ '--import'   ] + {imports}
sys.argv   += [ '--no-castor'] ## automaticlaly added argument 
sys.argv   += [ '--import={datafile}' ] 
sys.argv   += [ '--batch'    ] ##  automatically added argument 
sys.argv   += [ '--command'  ] + {command} 
##
# =============================================================================
## execute bender script 
# =============================================================================
import runpy
runpy.run_path ( bender_script, init_globals = globals() , name = '__main__' )
# =============================================================================
# The END
# =============================================================================
"""

    ## Returns file names which should be copyed to shared area
    def inputfiles(self):
        "Returns file names which should be copyed to shared area"
        return [ LocalFile(f.name) for f in self.scripts ] + [ LocalFile(f.name) for f in self.imports ]
    
    ## Returns  wrapper file which run at WN
    def getWrapper(self, data_file):
        "Returns  wrapper file which run at WN"

        the_script    = self.layout.format (
            scripts   = [ os.path.join ( f.subdir , os.path.basename ( f.name ) ) for f in self.scripts ] , 
            arguments = self.arguments  ,
            imports   = [ os.path.join ( f.subdir , os.path.basename ( f.name ) ) for f in self.imports ] ,
            datafile  = data_file ,
            command   = self.commands    
            )
        return the_script


# =============================================================================
## @class ostap
#  Helper class to define the main properties of Ostap application
#  - the scripts to be executed 
#  - bender commands to be executed
#  - other arguments
#
#  The scripts are executed within ``ostap'' context
#  Command line arguments ``--no-color'' and ``--batch'' are added automatically
#
#  The actual command to be executed is:
#  @code 
#  > ostap [ scripts [scripts [scripts ...  [ arguments ] --no-color --batch   [ --command  [ commands [ commands [ commands
#  @endcode
#  Usage:
#  @code
#  j.application = BenderBox ( tool = Ostap( scripts   = ['path_to_script/the_script.py']  ,
#                                            arguments = [ '--no-canvas' ]  ,
#                                            commands  = [ 'print dir()' ]  ) ) 
#  @encode
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru       
class ostap(GangaObject):
    """Helper class to define the main properties of Ostap application
    - the scripts to be executed 
    - ostap commands to be executed
    - other arguments
    
    The scripts are executed within ``ostap'' context
    Command line arguments ``--no-color'' and ``--batch'' are added automatically

    The actual command to be executed is:
    > ostap [ scripts [scripts [scripts ...  [ arguments ] --no-color --batch   [ --command  [ commands [ commands [ commands

    ======
    Usage:
    ======
    j.application = BenderBox ( tool = ostap( scripts   = ['path_to_script/the_script.py']  ,
                                              arguments = [ '--no-canvas' ]  ,
                                              commands  = [ 'print dir()' ]  ) )                                                 
    """
    _schema = Schema(Version(1, 0), {
        'scripts'   : FileItem   (
        defvalue        = []      ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The names of ostap script files to be executed. The files are executed within ``ostap'' context. A copy will be made at submission time"""),
        'commands'  : SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        =  1      ,
        doc             = """The ostap commands to be executed, e.g. [ 'print dir()' ]"""), 
        'arguments' : SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        =  1      ,
        doc             = "The list of command-line arguments for ``ostap'' script, e.g. ['-w','-p5'], etc. Following arguments are appended automatically:  --no-color and --batch""")
        })
    _category      = 'BenderTool'
    _name          = 'ostap'
    _exportmethods = [ ]

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
sys.argv += [ '--no-color' ] ## automatically added argument
sys.argv += {arguments}
sys.argv += [ '--batch'    ] ## automatically added argument
sys.argv += [ '--command'  ] + {command} 
##
# =============================================================================
## execute bender script 
# =============================================================================
import runpy
runpy.run_path ( ostap_script, init_globals = globals() , name = '__main__' )
# =============================================================================
# The END
# =============================================================================
"""
    ## Returns file names which should be copyed to shared area
    def inputfiles(self):
        "Returns file names which should be copyed to shared area"
        return [ LocalFile(f.name) for f in self.scripts ]

    ## Returns  wrapper file which run at WN 
    def getWrapper(self,data_file):
        "Returns  wrapper file which run at WN "
        
        the_script    = self.layout.format (
            scripts   = [ os.path.join ( f.subdir , os.path.basename ( f.name ) ) for f in self.scripts ] , 
            arguments = self.arguments  ,
            command   = self.commands    
            )
        return the_script

## import the base class 
from .GaudiExec import GaudiExec
# =============================================================================
## @class BenderBox
#  Box with Bender-based applications
#  - Simple Usage:
#  @code 
#  j = Job( application = prepareBender ( version = 'v30r1' ) )
#  j.application.tool   = BenderModule  ( module = 'pat_to_module/the_module.py' )
#  @endcode
#  Usage: 
#  - For Bender module, the native Bender applictaion:
#  @code 
#  j1 = Job()
#  j1.application             = BenderBox ( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
#  j1.application.tool.module = 'path_to_module/the_module.py' )
#  @endcode
#  Note:
#  1. The ``module'' is required to have configure&run methods  with the proper signature
#  2. ``summary.xml'' file is not added automatically to the output sandbox
# 
#  - For Bender script (the batch analogue of interactive ``bender'' environment):
#  @code 
#  j2 = Job()
#  j2.application = BenderBox ( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
#  j2.application.tool = bender ( scripts = [ 'path_to_script/the_script.py' ]  )
#  @endcode
#  Note:
#  1. The ``scripts'' are executed within ``bender'' context
#  2. ``--no-color'', ``--no-castor'' and ``--batch'' options are added automatically
#  3. ``data.py'' file is autoimported after other ``import'' scripts 
#  4. ``summary.xml'' file is *not* added automatically  to the output sandbox
#
#  - For Ostap scripts (the batch analogue of interactive ``ostap'' environment):
#  @code 
#  j3 = Job()
#  j3.application = BenderBox ( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
#  j3.application.tool = ostap ( scripts = [ 'path_to_script/the_script.py' ]  )
#  @endcode
#  Note:
#  1. The ``scripts'' are executed within ``ostap'' context
#  2. ``--no-color'' and ``--batch'' options are added automatically
#
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru       
class BenderBox(GaudiExec):
    """ Box with Bender-based Applications

    =============
    Simple Usage:
    =============
    j = Job( application = prepareBender ( version = 'v30r1' ) )
    j.application.tool   = BenderModule  ( module = 'pat_to_module/the_module.py' )
    
    ======
    Usage:
    ======
    
    - For Bender module, the native Bender applictaion:
    j1 = Job()
    j1.application = BenderBox ( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
    j1.application.tool.module = 'path_to_module/the_module.py' )

    Note:
    1. The ``module'' is required to have configure&run methods  with the proper signature
    2. ``summary.xml'' file is not added automatically to the output sandbox
    
    - For Bender script (the batch analogue of interactive ``bender'' environment):
    j2 = Job()
    j2.application = BenderBox ( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
    j2.application.tool = bender ( scripts = [ 'path_to_script/the_script.py' ]  )
    
    Note:
    1. The ``scripts'' are executed within ``bender'' context
    2. ``--no-color'', ``--no-castor'' and ``--batch'' options are added automatically
    3. ``data.py'' file is autoimported after other ``import'' scripts 
    4. ``summary.xml'' file is *not* added automatically  to the output sandbox

     - For Ostap scripts (the batch analogue of interactive ``ostap'' environment):
    j3 = Job()
    j3.application = BenderBox( directory = '$HOME/cmtuser/BenderDev_v30r1' ) 
    j3.application.tool = ostap ( scripts = [ 'path_to_script/the_script.py' ]  )

    Note:
    1. The ``scripts'' are executed within ``ostap'' context
    2. ``--no-color'' and ``--batch'' options are added automatically

    """
    _schema = Schema(Version(1, 0), {
        # Options created for constructing/submitting this app
        'tool'             : ComponentItem (
        category   = 'BenderTool' ,
        defvalue   = BenderModule()      ,
        doc        = """Bender ``tool'' to be used. See plugins[``BenderTool''] for allowed values"""
        ),
        'directory'        : SimpleItem    (
        defvalue   = ''          ,
        typelist   = [None, str] ,
        comparable = 1           ,
        doc        ="""A path/directory to the project that you\'re wanting to run.""" ) ,
        'platform'         : SimpleItem(defvalue='x86_64-slc6-gcc49-opt', typelist=[str], doc='Platform the application was built for'),
        ## from GaudiExec 
        'uploadedInput'    : GangaFileItem(defvalue=None, hidden=1, doc='This stores the input for the job which has been pre-uploaded so that it gets to the WN'),
        'jobScriptArchive' : GangaFileItem(defvalue=None, hidden=1, copyable=0, doc='This file stores the uploaded scripts which are generated fron this app to run on the WN'),
        'useGaudiRun'      : SimpleItem(defvalue=False, hidden = 1, doc='Should be False to call getWNPythonContents function'),
        'extraOpts'        : SimpleItem(defvalue='', typelist=[str], hidden=1, doc='DO NOT USE IT; An additional string which is to be added to \'options\' when submitting the job'),
        'options'          : GangaFileItem ( defvalue=[], sequence=1, hidden = 1,doc='List of files which contain the options to pass to gaudirun.py'),
        'extraArgs'        : SimpleItem(defvalue=[], typelist=[list], sequence=1, hidden=1, doc=' Do NOT USE IT; Extra runtime arguments which are passed to the code running on the WN'),
        # Prepared job object
        'is_prepared'      : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1,
                                        doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash'             : SimpleItem(defvalue=None, typelist=[None, str], hidden=1, doc='MD5 hash of the string representation of applications preparable attributes'),
        })
    _category      = 'applications'
    _name          = 'BenderBox'
    _exportmethods = ['prepare', 'unprepare' ]

    def configure(self, masterjobconfig):	
        self.options = self.tool.inputfiles()
        return (None,None)

    # =======================================================================
    def getWNPythonContents(self):
        """Return the wrapper script which is used to run Bender on the WN
        """
        from ..RTHandlers.GaudiExecRTHandlers import GaudiExecDiracRTHandler
        data_file = GaudiExecDiracRTHandler.data_file
        return self.tool.getWrapper(data_file)


# =============================================================================
## prepare Bender application
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' ) ) 
#  @endcode
#  One can specify the directory explicitely:
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , path = '$HOME/mydir' ) ) 
#  @endcode
#  or use the temporary directory 
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , use_tmp = True  ) ) 
#  @endcode
#  One can also use the configuration parameters:
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_module.py' , params = { ...} ) ) 
#  @endcode
def prepareBender ( version  , path = '$HOME/cmtuser', use_tmp = False , **kwargs ) :
    """Prepare Bender application
    >>> j = Job ( application = prepareBender ( 'v30r1' ) )
    One can specify the directory explicitely:
    >>> j = Job ( application = prepareBender ( 'v30r1' , path = '$HOME/mydir' ) ) 
    or use temporary directory: 
    >>> j = Job ( application = prepareBender ( 'v30r1' , use_tmp = True  ) ) 
    One can also use the configuration parameters:
    >>> j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_module.py' , params = { ...} ) ) 
    """
    from .GaudiExecUtils import prepare_cmake_app
    if use_tmp :
        import tempfile
        path = tempfile.mkdtemp ( prefix = 'GANGA_BENDER_%s_' % version )
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.info('Bender application will be prepared in the temporary directory %s' % path )
        
    the_path   = prepare_cmake_app ( 'Bender' , version , path )
    the_module = kwargs.pop ( 'module' , None )
    if not the_module :
        from Ganga.GPI import BenderBox as _BB ## kind of black magic 
        return _BB ( directory = the_path , **kwargs )
    
    from Ganga.GPI import BenderModule as _BM  ## black magic 
    the_tool = _BM ( module = the_module , 
                     params = kwargs.pop ( 'params' , {} ) , 
                     events = kwargs.pop ( 'events' , -1 ) ) 
    
    from Ganga.GPI import BenderBox as _BB         ## black magic 
    return _BB ( directory = the_path , 
                 tool      = the_tool , **kwargs ) 

## export it! 
from Ganga.Runtime.GPIexport import exportToGPI
exportToGPI('prepareBender', prepareBender, 'Functions')

## 
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from ..RTHandlers.GaudiExecRTHandlers                 import GaudiExecDiracRTHandler, GaudiExecRTHandler
allHandlers.add('BenderBox', 'Dirac', GaudiExecDiracRTHandler)
for backend in ("Local","Interactive","LSF","PBS","SGE","Condor"):
    allHandlers.add('Bender', backend, GaudiExecRTHandler)
    
# =============================================================================
# The END 
# =============================================================================
