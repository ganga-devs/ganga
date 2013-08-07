#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Parent for all Gaudi and GaudiPython applications in LHCb.'''

import os
import tempfile
import gzip
import shutil
from Ganga.GPIDev.Schema import *
#from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
#import CMTscript
#from GangaLHCb.Lib.Gaudi.CMTscript import parse_master_package
import Ganga.Utility.logging
from Ganga.Utility.files import expandfilename, fullpath
#from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
#from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from GaudiUtils import *
from Ganga.GPIDev.Lib.File import File
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
#from GaudiAppConfig import *
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Lib.File import ShareDir
from Ganga.Utility.Config import getConfig
import copy
logger = Ganga.Utility.logging.getLogger()


class GaudiBase(IPrepareApp):
    '''Parent for all Gaudi and GaudiPython applications, should not be used
    directly.'''

    schema = {}
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(preparable=1,defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(preparable=1,defvalue=None,
                                    typelist=['str','type(None)'],doc=docstr)
    docstr = 'The user path to be used. After assigning this'  \
             ' you can do j.application.getpack(\'Phys DaVinci v19r2\') to'  \
             ' check out into the new location. This variable is used to '  \
             'identify private user DLLs by parsing the output of "cmt '  \
             'show projects".'
    schema['user_release_area'] = SimpleItem(preparable=1,defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['appname'] = SimpleItem(preparable=1,defvalue=None,typelist=['str','type(None)'],
                                   hidden=1,doc=docstr)
    docstr = 'Location of shared resources. Presence of this attribute implies'\
             'the application has been prepared.'
    schema['is_prepared'] = SimpleItem(defvalue=None,
                                       strict_sequence=0,
                                       visitable=1,
                                       copyable=1,
                                       typelist=['type(None)','str'],
                                       protected=1,
                                       doc=docstr)
    docstr = 'The env'
    schema['env'] = SimpleItem(preparable=1,transient=1,defvalue=copy.deepcopy(os.environ),
                                   hidden=1,doc=docstr)
    docstr = 'MD5 hash of the string representation of applications preparable attributes'
    schema['hash'] = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=1)

    _name = 'GaudiBase'
    _exportmethods = ['getenv','getpack', 'make', 'cmt']
    _schema = Schema(Version(0, 1), schema)
    _hidden = 1
    
    def _get_default_version(self,gaudi_app):
        raise NotImplementedError

        #return guess_version(gaudi_app)

    def _get_default_platform(self):
        return get_user_platform()
        
    def _init(self,set_ura):
        if self.appname is None:
            raise ApplicationConfigurationError(None,"appname is None")
        if (not self.version): self.version = self._get_default_version(self.appname)
        if (not self.platform): self.platform = self._get_default_platform()
        if not set_ura: return
        if not self.user_release_area:
            expanded = os.path.expandvars("$User_release_area")
            if expanded == "$User_release_area": self.user_release_area = ""
            else:
                self.user_release_area = expanded.split(os.pathsep)[0]

##     def _check_gaudi_inputs(self,optsfiles): ##essentially expands the arg values to absolute paths
##         """Checks the validity of some of user's entries."""
##         for fileitem in optsfiles:
##             fileitem.name = os.path.expanduser(fileitem.name)
##             fileitem.name = os.path.expandvars(fileitem.name)
##             fileitem.name = os.path.normpath(fileitem.name)
            
    def _getshell(self):

        self.env = copy.deepcopy(os.environ)

        self.env['User_release_area'] = self.user_release_area
        self.env['CMTCONFIG'] = self.platform

    def getenv(self):
        '''Returns a copy of the environment used to flatten the options, e.g.
        env = DaVinci().getenv(), then calls like env[\'DAVINCIROOT\'] return
        the values.
        
        Note: Editing this does not affect the options processing.
        '''
        if not hasattr(self,'env'):
            try:
                job = self.getJobObject()
            except:
                self._getshell()
                return copy.deepcopy(self.env)
            env_file_name = job.getDebugWorkspace().getPath() + '/gaudi-env.py.gz'
            if not os.path.exists(env_file_name):
                self._getshell()
                return copy.deepcopy(self.env)
            in_file = gzip.GzipFile(env_file_name,'rb')
            exec(in_file.read())
            in_file.close()
            return gaudi_env            
        return copy.deepcopy(self.env)
        
##         try:
##             job = self.getJobObject()
##         except:
##             self._getshell()
##             return copy.deepcopy(self.env)
##         env_file_name = job.getInputWorkspace().getPath() + '/gaudi-env.py.gz'
##         if not os.path.exists(env_file_name):
##             self._getshell()
##             return copy.deepcopy(self.env)
##         else:
##             in_file = gzip.GzipFile(env_file_name,'rb')
##             exec(in_file.read())
##             in_file.close()
##             return gaudi_env
    
    def getpack(self, options=''):
        """Execute a getpack command. If as an example dv is an object of
        type DaVinci, the following will check the Analysis package out in
        the cmt area pointed to by the dv object.

        dv.getpack('Tutorial/Analysis v6r2')
        """
        # Make sure cmt user area is there
        cmtpath = expandfilename(self.user_release_area)
        if cmtpath:
            if not os.path.exists(cmtpath):
                try:
                    os.makedirs(cmtpath)
                except Exception, e:
                    logger.error("Can not create cmt user directory: "+cmtpath)
                    return

        if not hasattr(self,'env'): self._getshell()
#        shellEnv_cmd('getpack %s %s'%(self.appname, self.version),
        shellEnv_cmd('getpack %s' % options,
                     self.env,
                     self.user_release_area)
           
    def make(self, argument=''):
        """Build the code in the release area the application object points
        to. The actual command executed is "cmt broadcast make <argument>"
        after the proper configuration has taken place."""
        #command = '###CMT### broadcast -global -select=%s cmt make ' \
        #          % self.user_release_area + argument
        config = Ganga.Utility.Config.getConfig('GAUDI')
        if not hasattr(self,'env'): self._getshell()
        shellEnv_cmd('cmt broadcast %s %s' % (config['make_cmd'],argument),
                     self.env,
                     self.user_release_area)

    def cmt(self, command):
        """Execute a cmt command in the cmt user area pointed to by the
        application. Will execute the command "cmt <command>" after the
        proper configuration. Do not include the word "cmt" yourself."""
        if not hasattr(self,'env'): self._getshell()
        shellEnv_cmd('cmt %s' % command,
                     self.env,
                     self.user_release_area)

    def unprepare(self):
        self._unregister()

    def _unregister(self):
        if self.is_prepared is not None:          
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        self._register(force)
        if (not self.is_prepared): raise ApplicationConfigurationError(None,"Could not establish sharedir")

        #self.extra = GaudiExtras()
        self._getshell()
        #send_to_share=[]

        if (not self.user_release_area): return #GaudiPython and Bender dont need the following.
        if (not self.appname): raise ApplicationConfigurationError(None,"appname is None")
        if (not self.version): raise ApplicationConfigurationError(None,"version is None")
        if (not self.platform): raise ApplicationConfigurationError(None,"platform is None")
  
        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                 'shared',
                                 getConfig('Configuration')['user'],
                                 self.is_prepared.name)

        dlls, pys, subpys = get_user_dlls(self.appname, self.version,
                                          self.user_release_area,self.platform,
                                          self.env)
        InstallArea=[]
        #self.appconfig.inputsandbox += [File(f,subdir='lib') for f in dlls]
        for f in dlls:
##             if not os.path.isdir(os.path.join(share_dir,'inputsandbox','lib')): os.makedirs(os.path.join(share_dir,'inputsandbox','lib')) 
##             share_path = os.path.join(share_dir,'inputsandbox','lib',f.split('/')[-1])
##             shutil.copy(expandfilename(f),share_path)
            InstallArea.append(File(name=expandfilename(f),subdir='lib'))
        for f in pys:
            tmp = f.split('InstallArea')[-1]
            subdir = 'InstallArea' + tmp[:tmp.rfind('/')+1]
##             if not os.path.isdir(os.path.join(share_dir,'inputsandbox',subdir)): os.makedirs(os.path.join(share_dir,'inputsandbox',subdir))
##             share_path = os.path.join(share_dir,'inputsandbox',subdir,f.split('/')[-1])
##             shutil.copy(expandfilename(f),share_path)
            #File(f).create(share_path)
            #share_file = File(name=share_path,subdir=subdir)
            #share_file.subdir = subdir
            #self.prep_inputbox.append(share_file)
            #self.prep_inputbox.append(File(name=share_path,subdir=subdir))
            InstallArea.append(File(name=expandfilename(f),subdir=subdir))
        for dir, files in subpys.iteritems():
            for f in files:
                tmp = f.split('InstallArea')[-1]
                subdir = 'InstallArea' + tmp[:tmp.rfind('/')+1]
##                 if not os.path.isdir(os.path.join(share_dir,'inputsandbox',subdir)): os.makedirs(os.path.join(share_dir,'inputsandbox',subdir))
##                 share_path = os.path.join(share_dir,'inputsandbox',subdir,f.split('/')[-1])
##                 shutil.copy(expandfilename(f),share_path)
                #File(f).create(share_path)
                #share_file = File(name=share_path,subdir=subdir)
                #share_file.subdir = subdir
                #self.prep_inputbox.append(share_file)
                #self.prep_inputbox.append(File(name=share_path,subdir=subdir))
                InstallArea.append(File(name=expandfilename(f),subdir=subdir))
        # add the newly created shared directory into the metadata system if the app is associated with a persisted object
        # also call post_prepare for hashing
        # commented out here as inherrited from this class with extended perpare
        
##         self.checkPreparedHasParent(self)
##         self.post_prepare()
        fillPackedSandbox(InstallArea,
                          os.path.join( share_dir,
                                        'inputsandbox',
                                        '_input_sandbox_%s.tar' % self.is_prepared.name))

    def _register(self, force):
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        #self.incrementShareCounter(self.is_prepared.name)#NOT NECESSARY, DONE AUTOMATICALLY
        #self.prepare(force)

    def master_configure(self):
        '''Handles all common master_configure actions.'''
        raise NotImplementedError
        ## job=self.getJobObject()                
##         if job.inputdata: self.appconfig.inputdata = job.inputdata
##         if job.outputdata: self.appconfig.outputdata = job.outputdata

    def configure(self, appmasterconfig):
        raise NotImplementedError
        # return self.appconfig.inputdata.optionsString()




    #def postprocess(self):
        #from Ganga.GPIDev.Adapters.IApplication import PostprocessStatusUpdate
        #job = self.getJobObject()
        #if job:
        #raise PostprocessStatusUpdate("failed")
        
