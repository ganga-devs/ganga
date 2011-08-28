################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
################################################################################

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import *
#from Ganga.GPIDev.Lib.File import File
#from Ganga.GPIDev.Lib.File import SharedDir
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.Core import ApplicationConfigurationError

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
        'exe' : SimpleItem(defvalue='echo',typelist=['str','Ganga.GPIDev.Lib.File.File.File'],comparable=1,doc='A path (string) or a File object specifying an executable.'), 
        'args' : SimpleItem(defvalue=["Hello World"],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','str'],protected=1,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.')
        } )
    _category = 'applications'
    _name = 'Executable'
    _exportmethods = ['prepare','unprepare']
    _GUIPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                  { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                          { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(Executable,self).__init__()

    def _readonly(self):
        """An application is read-only once it has been prepared."""
        if self.is_prepared is None:
            return 0
        else:
            logger.error("Cannot modify a prepared application's attributes. First unprepare() the application.")
            return 1

        
    def prepare(self,force=False):
        """
        A method to place the Executable application into a prepared state.

        The application wil have a Shared Directory object created for it. 
        If the application's 'exe' attribute references a File() object or
        is a string equivalent to the absolute path of a file, the file 
        will be copied into the Shared Directory.

        Otherwise, it is assumed that the 'exe' attribute is referencing a 
        file available in the user's path (as per the default "echo Hello World"
        example). In this case, a wrapper script which calls this same command 
        is created and placed into the Shared Directory.

        When the application is submitted for execution, it is the contents of the
        Shared Directory that are shipped to the execution backend. 

        The Shared Directory contents can be queried with 
        shareref.ls('directory_name')
        
        See help(shareref) for further information.
        """
        if self._getRegistry() is None:
            raise ApplicationConfigurationError(None,'Applications not associated with a persisted object (Job or Box) cannot be prepared.')
    
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))


        self.configure(self)
        #does the application contains any File items
        #because of bug #82818 they don't work properly
        #difficult to distinguish between, say, /bin/echo and /home/user/echo; we don't necessarily
        #want to copy the former into the sharedir, but we would the latter. 
        #lets use the same criteria as the configure() method for checking file existence & sanity
        #this will bail us out of prepare if there's somthing odd with the job config - like the executable
        #file is unspecified, has a space or is a relative path

        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        #get hold of the metadata object for storing shared directory reference counts
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shared_dirname = self.is_prepared.name
        #add the newly created shared directory into the metadata system
        shareref.increase(self.is_prepared.name)



        #should we check for blank "" and/or None type self.exes? or does self.configure() do that for us?
        send_to_sharedir = []
        if type(self.exe) is str:
            send_to_sharedir = self.exe
            #we have a file. if it's an absolute path, copy it to the shared dir
            if os.path.abspath(send_to_sharedir) == send_to_sharedir:
                logger.info('Sending executable file %s to shared directory.'%(send_to_sharedir))
            #else assume it's a system binary, and create a wrapper to call this on the WN
            else:
                logger.info('Sending executable wrapper to shared directory.')
                # We'll need a tmpdir to momentarily store the wrapper in.
                if os.environ.has_key('TMPDIR'):
                    tmpDir = os.environ['TMPDIR']
                else:
                    cn = os.path.basename( os.path.expanduser( "~" ) )
                    tmpDir = os.path.realpath('/tmp/' + cn )
                if not os.access(tmpDir,os.W_OK):
                    os.makedirs(tmpDir)
                send_to_sharedir = os.path.join(tmpDir,os.path.basename(send_to_sharedir)+'.gangawrapper.sh')
                wrap_cmd='''#!/bin/bash
%s $*
''' %(self.exe)
                file(send_to_sharedir,'w').write(wrap_cmd)
        elif type(self.exe) is File:
            send_to_sharedir = self.exe.name
            logger.info('Sending file object %s to shared directory'%send_to_sharedir)
        logger.info('Copying %s to %s' %(send_to_sharedir, self.is_prepared.name))
        shutil.copy2(send_to_sharedir, self.is_prepared.name)

        self.exe=File(os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir)))
        os.chmod(self.exe.name, 0755)

        #return [os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir))]
        return 1

    def unprepare(self, force=False):
        """
        Revert an Executable() application back to it's unprepared state.
        """
        if self.is_prepared is not None:
            shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
            shareref.decrease(self.is_prepared.name)
            self.is_prepared = None
            if isType(self.exe, File):
                if len(os.path.basename(self.exe.name).split('.gangawrapper')) > 1:
                    self.exe = ''.join(os.path.basename(self.exe.name).split('.gangawrapper')[:-1])
                else:
                    self.exe.name = os.path.basename(self.exe.name)
    
    

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
                    if dirn and not os.path.isabs(dirn) and self.is_prepared is None:
                        raise ApplicationConfigurationError(None,'exe "%s" is a relative path'%x)
                    if not os.path.basename(x) == x:
                        if not os.path.isfile(x):
                            raise ApplicationConfigurationError(None,'%s: file not found'%x)

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

