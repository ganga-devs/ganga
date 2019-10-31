# -*- coding: utf-8 -*-
# =============================================================================
## @file BenderBox.py
#  Bender application for Ganga  (CMAKE-based version)
#  Actually there are three applications under the single hat
#  - BenderModule : classic Bender application: use a full power of bender
#  - BenderRun    : execute scripts in the context of ``bender'' environment
#  - OstapRun     : execute scripts in the context of ``ostap'' environment
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
import tempfile 
from   GangaCore.GPIDev.Lib.File.File                       import ShareDir  
from   GangaCore.GPIDev.Schema                              import Schema , Version , SimpleItem , GangaFileItem
from   GangaCore.Utility.logging                            import getLogger
from   GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers


from   GangaLHCb.Lib.Applications.GaudiExecUtils        import prepare_cmake_app
from   GangaLHCb.Lib.RTHandlers.GaudiExecRTHandlers     import GaudiExecDiracRTHandler, GaudiExecRTHandler
from   GangaLHCb.Lib.Applications.GaudiExec             import GaudiExec

logger = getLogger()
# =============================================================================
## wrapper python script for Bender module
_script_ = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
from Gaudi.Configuration import importOptions
importOptions('{datafile}')
from Configurables import LHCbApp
LHCbApp().XMLSummary='summary.xml'
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
# =============================================================================
## wrapper python script for Bender script
_script_bender_ = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
from distutils.spawn import find_executable
bender_script = find_executable('bender')
import sys
from Configurables import LHCbApp
LHCbApp().XMLSummary='summary.xml'
# =============================================================================
## redefine arguments
# =============================================================================
sys.argv   += {scripts}
sys.argv   += [ '--no-color' ] ## automatically added argument
sys.argv   += {arguments}
sys.argv   += [ '--import'   ] + {imports}
sys.argv   += [ '--no-castor'] ## automatically added argument
sys.argv   += [ '--import={datafile}' ]
sys.argv   += [ '--batch'    ] ##  automatically added argument
sys.argv   += [ '--command'  ] + {command}
##
# =============================================================================
## execute bender script
# =============================================================================
import runpy
runpy.run_path ( bender_script, init_globals = globals() , run_name = '__main__' )
# =============================================================================
# The END
# =============================================================================
"""
# =============================================================================
## wrapper python script for Ostap script
_script_ostap_ = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
from distutils.spawn import find_executable
ostap_script = find_executable('ostap')
import sys
from Configurables import LHCbApp
LHCbApp().XMLSummary='summary.xml'
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
runpy.run_path ( ostap_script, init_globals = globals() , run_name = '__main__' )
# =============================================================================
# The END
# =============================================================================
"""
# =============================================================================

# =============================================================================
## @class BenderModule
#  The main application to run ``classic'' Bender module with the proper
#  ``configure'' and ``run'' methods
#
#  User needs to supply:
#  - the name of Bender module to run
#  - dictionary of parameters to be forwarder to <code>configure</code> method
#  - number of event to process
#
#  Usage:
#  @code
#  j.application = BenderModule ( module    = 'the_path/the_module.py' ,
#                                 events    = 1000  ,
#                                 params    = {...} ,
#                                 directory =  ...  )
#  @endcode
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV       Ivan.Belyaev@itep.ru
class BenderModule(GaudiExec):
    """ The main application to run ``classic'' Bender (module with the proper
    ``configure'' and ``run'' methods)

    User needs to supply:
    - the name of Bender module to run
    - dictionary of parameters to be forwarder to <code>configure</code> method
    - number of event to process

    ======
    Usage:
    ======

    j.application = BenderModule ( module    = 'the_path/the_module.py' ,
                                   events    = 1000  ,
                                   params    = {...} ,
                                   directory =  ...  )

    """
    ##
    _schema = GaudiExec._schema.inherit_copy()
    _schema.version.major += 0
    _schema.version.minor += 0
    ## make entries
    for key,val in _schema.datadict.items() :
        if key == 'useGaudiRun'  : val._update( { 'defvalue' : False } )
        if key == 'getMetadata'  : val._update( { 'defvalue' : True } )
        if not key in ( 'platform' , 'directory', 'getMetadata') :
            if not val['hidden'] : val._update( { 'hidden'   : 1 } )
    ## add new entries
    _schema.datadict [ 'module' ] = GangaFileItem (
        optional  = 0 ,
        doc       = """The file with Bender module. It is expected that module contains the methods ``configure'' & ``run'' with the proper signatures""")
    _schema.datadict [ 'params' ] = SimpleItem    (
        defvalue  = {} , typelist = ['dict', 'str', 'int', 'bool', 'float'] ,
        doc       = """The dictionary of parameters to be forwarded to ``configure'' method of the supplied Bender module""")
    _schema.datadict [ 'events' ] = SimpleItem    ( defvalue=-1, typelist=['int'], doc= "Number of events to process" )
    ##
    _category      = 'applications'
    _name          = 'BenderModule'
    _exportmethods = ['prepare', 'unprepare' ]

    # =========================================================================
    def configure(self, masterjobconfig):
        self.options = [ self.module ]
        return (None,None)

    # =========================================================================
    def getWNPythonContents(self):
        """Return the wrapper script which is used to run Bender on the WN
        """
        f = self.module
        file_name       = os.path.basename ( os.path.join  ( f.localDir , f.namePattern  ) )
        module_name     = file_name.split('.')[0]
        param_string    = ',params=%s' % self.params if self.params else ''
        data_file       = GaudiExecDiracRTHandler.data_file

        return _script_.format (
            datafile    = data_file ,
            modulename  = module_name ,
            paramstring = param_string ,
            events      = self.events
            )

# =============================================================================
## @class BenderRun
#  The main application to run ``BenderScript''
#
#  User needs to supply:
#  - the scripts to be executed
#  - the configuration scripts (aka ``options'') to be imported  (optional)
#  - bender interactive commands to be executed                  (optional)
#  - other arguments for script ``bender''                       (optional)
#
#  Usage:
#  @code
#  j.application = BenderRun    ( scripts   = [ 'the_path/the_module.py' ] ,
#                                 directory =  ...  )
#  @endcode
#
#  The actual command to be executed is:
#  @code
#  > bender [ scripts [ scripts ...  --no-color [ arguments ] --import [ imports [ imports [ ... --no-castor --import=data.py --batch   [ --command  [ commands [ ...
#  @endcode
#
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV       Ivan.Belyaev@itep.ru
class BenderRun(GaudiExec):
    """The main application to run ``BenderScript''

    User needs to supply:
    - the scripts to be executed
    - the configuration scripts (aka ``options'') to be imported  (optional)
    - bender interactive commands to be executed                  (optional)
    - other arguments for script ``bender''                       (optional)

    The actual command to be executed is:

    > bender [ scripts [ scripts ...  --no-color [ arguments ] --import [ imports [ imports [ ... --no-castor --import=data.py --batch   [ --command  [ commands [ ...

    ======
    Usage:
    ======

    j.application = BenderRun    ( scripts   = [ 'the_path/the_module.py'     ] ,
                                   imports   = [ 'some_miport_file.py'        ] ,
                                   commands  = [ 'ls()' , 'run(10)'  , 'ls()' ] ,
                                   arguments = [ ... ] ,
                                   directory =  ...  )

    """
    _schema = GaudiExec._schema.inherit_copy()
    _schema.version.major += 0
    _schema.version.minor += 0
    ## make entries
    for key,val in _schema.datadict.items() :
        if key == 'useGaudiRun'  : val._update( { 'defvalue' : False } )
        if key == 'getMetadata'  : val._update( { 'defvalue' : True } )
        if not key in ( 'platform' , 'directory', 'getMetadata') :
            if not val['hidden'] : val._update( { 'hidden'   : 1 } )
    ## add new entries
    _schema.datadict [ 'scripts'   ] = GangaFileItem   (
        optional        = 0       ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The names of the script files to execute. A copy will be made at submission time. The script are executed within ``bender'' context""")
    _schema.datadict [ 'imports'   ] = GangaFileItem   (
        defvalue        = []      ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The names of the configurtaion scripts (ana ``options'') to be imported via ``importOptions''. A copy will be made at submission time""")
    _schema.datadict [ 'commands'  ] = SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The bender commands to be executed, e.g. [ 'run(10)' , 'print ls()' , 'print dir()' ]""")
    _schema.datadict [ 'arguments' ] = SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The list of command-line arguments for bender script, e.g. ['-w','-p5'], etc. Following arguments will be appended automatically:  --no-color, --no-castor and --batch.""")
    ##
    _category      = 'applications'
    _name          = 'BenderRun'
    _exportmethods = ['prepare', 'unprepare' ]

    # =========================================================================
    def configure(self, masterjobconfig):
        self.options = [ f for f in self.scripts ] + [ f for f in self.imports ]
        return (None,None)

    # =========================================================================
    def getWNPythonContents(self):
        """Return the wrapper script which is used to run BenderScript on the WN
        """
        data_file       = GaudiExecDiracRTHandler.data_file
        return _script_bender_.format (
            scripts   = [ os.path.basename ( os.path.join ( f.localDir , f.namePattern  ) ) for f in self.scripts ] ,
            arguments = self.arguments ,
            imports   = [ os.path.basename ( os.path.join ( f.localDir , f.namePattern  ) ) for f in self.imports ] ,
            datafile  = data_file ,
            command   = self.commands
            )

# =============================================================================
## @class OstapRun
#  The main application to run ``Ostap''
#
#  User needs to supply:
#  - the scripts to be executed
#  - bender interactive commands to be executed                  (optional)
#  - other arguments for script ``ostap''                        (optional)
#
#  The actual command to be executed is:
#  @code
#  > ostap [ scripts [scripts [scripts ...  [ arguments ] --no-color --batch   [ --command  [ commands [ commands [ commands
#  @endcode
#  Usage:
#  @code
#  j.application = OstapRun ( scripts   = ['path_to_script/the_script.py']  ,
#                             arguments = [ '--no-canvas' ]  ,
#                             commands  = [ 'getMetadataprint dir()' ]  ) )
#  @encode
#  @author Vladimir ROMANOVSKY Vladimir.Romanovskiy@cern.ch
#  @author Vanya BELYAEV  Ivan.Belyaev@itep.ru
class OstapRun(GaudiExec):
    """The main application to run ``Ostap''

    User needs to supply:
    - the scripts to be executed
    - ostap interactive commands to be executed                  (optional)
    - other arguments for script ``ostap''                       (optional)

    The actual command to be executed is:
    > ostap [ scripts [scripts [scripts ...  [ arguments ] --no-color --batch   [ --command  [ commands [ commands [ commands

    ======
    Usage:
    ======

    j.application = OstapRun ( scripts   = ['path_to_script/the_script.py']  ,
                               arguments = [ '--no-canvas' ]  ,
                               commands  = [ 'print dir()' ]  ) )
    """
    _schema = GaudiExec._schema.inherit_copy()
    _schema.version.major += 0
    _schema.version.minor += 0
    ## make entries
    for key,val in _schema.datadict.items() :
        if key == 'useGaudiRun'  : val._update( { 'defvalue' : False } )
        if key == 'getMetadata'  : val._update( { 'defvalue' : True } )
        if not key in ( 'platform' , 'directory', 'getMetadata') :
            if not val['hidden'] : val._update( { 'hidden'   : 1 } )
    ## add new entries
    _schema.datadict [ 'scripts'   ] = GangaFileItem   (
        optional        = 0       ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The names of ostap script files to be executed. The files are executed within ``ostap'' context. A copy will be made at submission time""")
    _schema.datadict [ 'commands'  ] = SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = """The ostap commands to be executed, e.g. [ 'print dir()' ]""")
    _schema.datadict [ 'arguments' ] = SimpleItem (
        defvalue        = []      ,
        typelist        = ['str'] ,
        sequence        = 1       ,
        strict_sequence = 0       ,
        doc             = "The list of command-line arguments for ``ostap'' script, e.g. ['-w','-p5'], etc. Following arguments are appended automatically:  --no-color and --batch""")
    ##
    _category      = 'applications'
    _name          = 'OstapRun'
    _exportmethods = ['prepare', 'unprepare' ]

    # =========================================================================
    def configure(self, masterjobconfig):
        self.options = [ f for f in self.scripts ]
        return (None,None)

    # =========================================================================
    def getWNPythonContents(self):
        """Return the wrapper script which is used to run Ostap on the WN
        """
        data_file       = GaudiExecDiracRTHandler.data_file
        return _script_ostap_.format (
            scripts   = [ os.path.basename ( os.path.join ( f.localDir , f.namePattern  ) ) for f in self.scripts ] ,
            arguments = self.arguments ,
            command   = self.commands
            )


# =============================================================================
## prepare Bender application
#  - specify the path:
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_module.py' , path = '$HOME/cmtuser' ) )
#  @endcode
#  - use the temporary directory
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_modue.py' , use_tmp = True  ) )
#  @endcode
#  - other parameters  can be added as keyword arguments
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_modue.py' , use_tmp = True , params = {} , events = 100 ) )
#  @endcode
def prepareBender ( version , module , path = '$HOME/cmtuser', use_tmp = False , **kwargs ) :
    """Prepare Bender application
    - specify the path:
    >>> j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_module.py' , path = '$HOME/cmtuser' ) )
    - use the temporary directory
    >>> j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_modue.py' , use_tmp = True  ) )
    - other parameters  can be added as keyword arguments
    >>> j = Job ( application = prepareBender ( 'v30r1' , module = 'the_path/the_modue.py' , use_tmp = True , params = {} , events = 100 ) )
    """
    if use_tmp or not path :
        path = tempfile.mkdtemp ( prefix = 'GANGA_' )
        logger.info ( 'Bender application will be prepared in the temporary directory %s' % path )

    the_path = prepare_cmake_app ( 'Bender' , version , path )

    from GangaCore.GPI import BenderModule as _BM  ## black magic
    return _BM ( module = module , directory = the_path , **kwargs )

# =============================================================================
## prepare BenderRun application
#  - specify the script
#  @code
#  j = Job ( application = prepareBenderRun ( 'v30r1' , script = 'the_path/the_script.py') )
#  @endcode
#  - specify the scripts
#  @code
#  j = Job ( application = prepareBenderRun ( 'v30r1' , scripts = ['the_path/the_script.py' , 'another_script.py' ] ) )
#  @endcode
#  - specify the directory explicitely:
#  @code
#  j = Job ( application = prepareBenderRun ( 'v30r1' , path = '$HOME/mydir' , script = 'the_script.py' )
#  @endcode
#  - use the temporary directory
#  @code
#  j = Job ( application = prepareBenderRun ( 'v30r1' , use_tmp = True , script = 'the_script.py' )
#  @endcode
#  - one can also use the configuration parameters:
#  @code
#  j = Job ( application = prepareBender ( 'v30r1' , script = 'the_path/the_script.py' , commands = [] , imports = []  ) )
#  @endcode
def prepareBenderRun ( version , script = '' , path = '$HOME/cmtuser', use_tmp = False , **kwargs ) :
    """Prepare BenderRun application
    - specify the script
    >>> j = Job ( application = prepareBenderRun ( 'v30r1' , script = 'the_path/the_script.py') )
    - specify the scripts
    >>> j = Job ( application = prepareBenderRun ( 'v30r1' , scripts = ['the_path/the_script.py' , 'another_script.py' ] ) )
    - specify the directory explicitely:
    >>> j = Job ( application = prepareBenderRun ( 'v30r1' , path = '$HOME/mydir' , script = 'the_script.py' )
    - use the temporary directory
    >>> j = Job ( application = prepareBenderRun ( 'v30r1' , use_tmp = True , script = 'the_script.py' )
    One can also use the configuration parameters:
    >>> j = Job ( application = prepareBender ( 'v30r1' , script = 'the_path/the_script.py' , commands = [] , imports = []  ) )
    """
    if use_tmp or not path :
        path = tempfile.mkdtemp ( prefix = 'GANGA_' )
        logger.info ( 'Bender application will be prepared in the temporary directory %s' % path )

    the_path = prepare_cmake_app ( 'Bender' , version , path )
    #
    scripts = kwargs.pop('scripts', [] )
    if script : scripts.append ( script )
    #

    from GangaCore.GPI import BenderRun as _BR  ## black magic    
    return _BR ( scripts = scripts , directory = the_path , **kwargs )


# =============================================================================
## prepare OstapRun application
#  - specify the script
#  @code
#  j = Job ( application = prepareOstapRun ( 'v30r1' , script = 'the_path/the_script.py') )
#  @endcode
#  - specify the scripts
#  @code
#  j = Job ( application = prepareOstapRun ( 'v30r1' , scripts = ['the_path/the_script.py' , 'another_script.py' ] ) )
#  @endcode
#  - specify the directory explicitely:
#  @code
#  j = Job ( application = prepareOstapRun ( 'v30r1' , path = '$HOME/mydir' , script = 'the_script.py' )
#  @endcode
#  - use the temporary directory
#  @code
#  j = Job ( application = prepareOstapRun ( 'v30r1' , use_tmp = True , script = 'the_script.py' )
#  @endcode
#  - one can also use the configuration parameters:
#  @code
#  j = Job ( application = prepareOstapRun ( 'v30r1' , script = 'the_path/the_script.py' , commands = [] , imports = []  ) )
#  @endcode
#  - use another project  ('Analysis' is minimal one... , 'Bender' is default one)
#  @code
#  j = Job ( application = prepareOstapRun ( project = 'Analysis' , version = 'v18r0' , script = 'the_path/the_script.py' , commands = [] ) )
#  @endcode
def prepareOstapRun ( version , script = '' , path = '$HOME/cmtuser', use_tmp = False , project = 'Bender' , **kwargs ) :
    """Prepare OstapRun application
    - specify the script
    >>> j = Job ( application = prepareOstapRun ( 'v30r1' , script = 'the_path/the_script.py') )
    - specify the scripts 
    >>> j = Job ( application = prepareOstapRun ( 'v30r1' , scripts = ['the_path/the_script.py' , 'another_script.py' ] ) )
    - specify the directory explicitely:
    >>> j = Job ( application = prepareOstapRun ( 'v30r1' , path = '$HOME/mydir' , script = 'the_script.py' )
    - use the temporary directory
    >>> j = Job ( application = prepareOstapRun ( 'v30r1' , use_tmp = True , script = 'the_script.py' )
    - one can also use the configuration parameters:
    >>> j = Job ( application = prepareOstapRun ( 'v30r1' , script = 'the_path/the_script.py' , commands = [] , imports = []  ) )
    - use another project  ('Analysis' is minimal one...)
    >>> j = Job ( application = prepareOstapRun ( project = 'Analysis' , version = 'v18r0' , script = 'the_path/the_script.py' , commands = []  ) )
    """
    if use_tmp or not path :
        path = tempfile.mkdtemp ( prefix = 'GANGA_' )
        logger.info ( 'Bender application will be prepared in the temporary directory %s' % path )

    the_path = prepare_cmake_app ( project , version , path )
    #
    scripts = kwargs.pop('scripts', [] )
    if script : scripts.append ( script )
    #
    from GangaCore.GPI import OstapRun as _OR  ## black magic    
    return _OR ( scripts = scripts , directory = the_path , **kwargs )

## export it! 
from GangaCore.Runtime.GPIexport import exportToGPI
exportToGPI ( 'prepareBender'      , prepareBender       , 'Functions' )
exportToGPI ( 'prepareBenderRun'   , prepareBenderRun    , 'Functions' )
exportToGPI ( 'prepareOstapRun'    , prepareOstapRun     , 'Functions' )

##
for app in ( 'BenderModule' , 'BenderRun' , 'OstapRun' ) :
    allHandlers.add ( app , 'Dirac', GaudiExecDiracRTHandler)
    for backend in ("Local","Interactive","LSF","PBS","SGE","Condor"):
        allHandlers.add ( app , backend, GaudiExecRTHandler)

# =============================================================================
# The END
# =============================================================================
