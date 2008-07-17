################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
################################################################################

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File

class Executable(IApplication):
    """
    Executable application -- running arbitrary programs.
    
    When you want to run on a worker node an exact copy of your script you should specify it as a File object. Ganga will
    then ship it in a sandbox:
       app.exe = File('/path/to/my/script')

    When you want to execute a command on the worker node you should specify it as a string. Ganga will call the command
    with its full path on the worker node:
       app.exe = '/bin/date'

    A command string may be either an absolute path ('/bin/date') or a command name ('echo').
    Relative paths ('a/b') or directory paths ('/a/b/') are not allowed because they have no meaning
    on the worker node where the job executes.

    The arguments may be specified in the following way:
       app.args = ['-v',File('/some/input.dat')]

    This will yield the following shell command: executable -v input.dat
    The input.dat will be automatically added to the input sandbox.

    If only one argument is specified the the following abbreviation may be used:
       apps.args = '-v'
    
    """
    _schema = Schema(Version(2,0), {
        'exe' : SimpleItem(defvalue='echo',typelist=['str','Ganga.GPIDev.Lib.File.File.File'],doc='A path (string) or a File object specifying an executable.'), 
        'args' : SimpleItem(defvalue=["Hello World"],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings or File objects."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment')
        } )
    _category = 'applications'
    _name = 'Executable'
    _GUIPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                  { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                          { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(Executable,self).__init__()
        
    def configure(self,masterappconfig):
        from Ganga.Core import ApplicationConfigurationError
        import os.path
        
        # do the validation of input attributes, with additional checks for exe property

        def validate_argument(x,exe=None):
            if type(x) is type(''):
                if exe:
                    if not x:
                        raise ApplicationConfigurationError(None,'exe not specified')
                        
                    if len(x.split())>1:
                        raise ApplicationConfigurationError(None,'exe "%s" contains white spaces'%x)

                    dirn,filen = os.path.split(x)
                    if not filen:
                        raise ApplicationConfigurationError(None,'exe "%s" is a directory'%x)
                    if dirn and not os.path.isabs(dirn):
                        raise ApplicationConfigurationError(None,'exe "%s" is a relative path'%x)


            else:
              try:
                  if not x.exists():
                      raise ApplicationConfigurationError(None,'%s: file not found'%x.name)
              except AttributeError:
                  raise ApplicationConfigurationError(None,'%s (%s): unsupported type, must be a string or File'%(str(x),str(type(x))))
                
        validate_argument(self.exe,exe=1)

        for a in self.args:
            validate_argument(a)
        
        return (None,None)

# disable type checking for 'exe' property (a workaround to assign File() objects)
# FIXME: a cleaner solution, which is integrated with type information in schemas should be used automatically
config = getConfig('defaults_Executable') #_Properties
#config.setDefaultOption('exe',Executable._schema.getItem('exe')['defvalue'], type(None),override=True)
config.options['exe'].type = type(None)

# not needed anymore: 
#   the backend is also required in the option name
#   so we need a kind of dynamic options (5.0)
#mc = getConfig('MonitoringServices')
#mc['Executable'] = None

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

        #c.monitoring_svc = mc['Executable']

        return c
        

class DiracRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Lib.File import File
        rth=RTHandler()
        prep=rth.prepare(app,appconfig)
        ## Modify result in order to run on Dirac

        result={}
        result['vers']=''
        result['opts']=''
        result['app']=prep['jobscript']
        result['inputbox']=prep['inputbox']
        result['dlls']=''
        return result

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Executable','LSF', RTHandler)
allHandlers.add('Executable','Local', RTHandler)
allHandlers.add('Executable','PBS', RTHandler)
allHandlers.add('Executable','SGE', RTHandler)
allHandlers.add('Executable','Condor', RTHandler)
allHandlers.add('Executable','LCG', LCGRTHandler)
allHandlers.add('Executable','gLite', gLiteRTHandler)
allHandlers.add('Executable','TestSubmitter', RTHandler)
allHandlers.add('Executable','Interactive', RTHandler)
allHandlers.add('Executable','Batch', RTHandler)
allHandlers.add('Executable','Cronus', RTHandler)
allHandlers.add('Executable','Remote', LCGRTHandler)

#
#
# $Log: not supported by cvs2svn $
# Revision 1.29.24.6  2008/06/24 15:10:57  moscicki
# added Remote
#
# Revision 1.29.24.5  2007/12/18 16:40:03  moscicki
# removed unneccessary 'list' from the typelist
#
# Revision 1.29.24.4  2007/12/18 09:07:42  moscicki
# integrated typesystem from Alvin
#
# Revision 1.29.24.3  2007/12/10 17:51:22  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.29.24.2  2007/10/12 14:41:49  moscicki
# merged from disabled jobs[] syntax and test migration
#
# Revision 1.29.24.1  2007/10/12 13:56:25  moscicki
# merged with the new configuration subsystem
#
# Revision 1.29.26.1  2007/09/25 09:45:12  moscicki
# merged from old config branch
#
# Revision 1.29.6.1  2007/06/18 07:44:55  moscicki
# config prototype
#
# Revision 1.29.22.1  2007/09/26 08:42:44  amuraru
# *** empty log message ***
#
# Revision 1.31  2007/09/26 08:39:17  amuraru
# *** empty log message ***
#
# Revision 1.30  2007/09/25 15:16:45  amuraru
# removed MonitoringServices configuration
#
# Revision 1.32  2007/10/22 11:52:57  amuraru
# removed job[] syntax intended for 4.4.X series
#
#
# Revision 1.30  2007/09/25 15:16:45  amuraru
# removed MonitoringServices configuration
#
# Revision 1.29.22.1  2007/09/26 08:42:44  amuraru
# *** empty log message ***
#
# Revision 1.31  2007/09/26 08:39:17  amuraru
# *** empty log message ***
#
# Revision 1.30  2007/09/25 15:16:45  amuraru
# removed MonitoringServices configuration
#
# Revision 1.29  2007/03/12 15:45:47  moscicki
# cronus added
#
# Revision 1.28  2007/03/07 09:52:37  moscicki
# Executable: args non-strict, i.e. a.args = 'abcd' => a.args = ['abcd']
#
# Revision 1.27  2007/02/15 10:20:04  moscicki
# added SGE backend (merged from branch)
#
# Revision 1.26.2.1  2007/02/15 10:14:31  moscicki
# added SGE to runtime handlers list
#
# Revision 1.26  2006/10/06 10:25:13  moscicki
# removed DIRAC-Executable binding
#
# Revision 1.25  2006/08/24 16:50:19  moscicki
# MonitoringServices added
#
# Revision 1.24  2006/08/03 10:28:16  moscicki
# added Interactive
#
# Revision 1.23  2006/07/27 20:23:26  moscicki
# moved default values to the schema
# removed the explicit configuration unit
#
# Revision 1.22  2006/06/21 11:44:31  moscicki
# moved ExeSplitter to Lib/Splitters
#
# Revision 1.21  2006/06/13 12:25:59  moscicki
# make a entire copy of a master job for each subjob (instead of just copying the backend)
#
# Revision 1.20  2006/03/21 16:50:02  moscicki
# added Condor
#
# Revision 1.19  2006/03/09 08:34:48  moscicki
# - ExeSplitter fix (job copy)
#
# Revision 1.18  2006/02/10 14:28:16  moscicki
# validation of arguments on configure (fixes:  bug #13685 overview: cryptic message if executable badly specified in LCG handler)
#
# Revision 1.17  2006/01/09 16:40:09  moscicki
# Executable_default config: echo Hello World
#
# Revision 1.16  2005/12/02 15:36:43  moscicki
# adapter to new base classes, added a simple splitter
#
# Revision 1.15  2005/11/25 12:59:37  moscicki
# added runtime handler for TestSubmitter
#
# Revision 1.14  2005/11/23 13:39:19  moscicki
# added PBS, removed obsolteted getRuntimeHandler() method
#
# Revision 1.13  2005/11/14 10:35:20  moscicki
# GUI prefs
#
# Revision 1.12.2.3  2005/10/28 17:32:46  ctan
# *** empty log message ***
#
# Revision 1.12.2.2  2005/10/27 15:04:03  ctan
# *** empty log message ***
#
# Revision 1.12.2.1  2005/10/26 09:02:13  ctan
# *** empty log message ***
#
# Revision 1.12  2005/09/21 12:41:26  andrew
# Changes made to include a gLite handler.
#
# Revision 1.11  2005/09/02 12:49:13  liko
# Include LCG Handler, extend application with environment
#
# Revision 1.10  2005/08/30 08:03:05  andrew
# Added Dirac as a possible backend. Not that this would really work mind you
#
# Revision 1.9  2005/08/24 08:13:41  moscicki
# using StandardJobConfig
#
#
#
