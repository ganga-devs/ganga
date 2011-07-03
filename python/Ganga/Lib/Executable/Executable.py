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
from Ganga.GPIDev.Lib.File import SharedDir
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.Core.GangaRepository import getRegistry

import os, shutil

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
        'args' : SimpleItem(defvalue=["Hello World"],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SharedItem(defvalue=None, strict_sequence=0, visitable=1, typelist=['type(None)','str'],protected=1,doc='Save file for prepared state test. Presence of this attribute implies the application is prepared.')
        } )
    _category = 'applications'
    _name = 'Executable'
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                  { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                          { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(Executable,self).__init__()
        
    def prepare(self,force=False):

        if (self.is_prepared is None) or (force is True):
            logger.info('Preparing %s application.'%(self._name))

            #does the application contains any File items
            #because of bug #82818 the next 2 lines don't properly work
            #if isinstance(self.exe, File):
            #    addToSharedDir.append(self.exe)
    
            #difficult to distinguish between, say, /bin/echo and /home/user/echo; we don't necessarily
            #want to copy the former into the shareddir, but we would the latter. 
            #if we simply test for the existence of the self.exe file, then a system file won't be found, but a 
            #"local" user-space file will, because it should have an absolute path or be in the pwd (is this true?)
            #lets use the same criteria as the configure() method for checking file existence & sanity
            #this will bail us out of prepare if there's somthing odd with the job config - like the executable
            #file is unspecified or has a space
            self.configure(self)

            #should we check for blank "" and/or None type self.exes? or does self.configure() do that for us?

            if type(self.exe) is str:
                self.is_prepared = SharedDir()
                shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                shareref.add(self.is_prepared.name)
                shared_dirname = self.is_prepared.name
                addToSharedDir = self.exe
                #we have a file. if it's an absolute path, copy to shared dir
                if os.path.abspath(addToSharedDir) == addToSharedDir:
                    logger.info('Adding executable file to shared directory %s'%(shared_dirname))
                    shutil.copy2(addToSharedDir, shared_dirname)
                #else assume it's a system binary, and create a wrapper to call this on the WN
                else:
                    logger.info('Adding executable wrapper to shared directory %s'%(shared_dirname))
                    os.system('touch ' + os.path.join(shared_dirname,os.path.basename(addToSharedDir)))

                #if it's not a string, and is a File(), then that should be copied to the shared directory
                #though this doesn't yet work, because of bug #82818
            elif type(self.exe) is File:
                self.is_prepared = SharedDir()
                shared_dirname = self.is_prepared.name
                addToSharedDir = self.exe.name
                print "Adding " + addToSharedDir + " to shared dir " + str(shared_dirname)
                shutil.copy2(addToSharedDir,shared_dirname)
                if os.path.isfile(os.path.join(shared_dirname,os.path.basename(addToSharedDir))):
                    print 'Successfully added ' + os.path.join(shared_dirname,os.path.basename(addToSharedDir))
        else:
            logger.info('Job %s , %s application was already prepared; not prepared again.'%(self.id,self.application._name))
            logger.info('If you really want to prepare the job, use \'job.prepare(force=True)\'')
            print force
        return 1


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
                  #int arguments are allowed -> later converted to strings      
                  if isinstance(x,int):
                      return
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

def convertIntToStringArgs(args):

    result = []
    
    for arg in args:
        if isinstance(arg,int):
            result.append(str(arg))
        else:
            result.append(arg)

    return result

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app.exe,app._getParent().inputsandbox,convertIntToStringArgs(app.args),app._getParent().outputsandbox,app.env)

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

        return LCGJobConfig(app.exe,app._getParent().inputsandbox,convertIntToStringArgs(app.args),app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app.exe,app._getParent().inputsandbox,convertIntToStringArgs(app.args),app._getParent().outputsandbox,app.env)
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
allHandlers.add('Executable','CREAM', LCGRTHandler)

