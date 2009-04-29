#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Parent for all Gaudi and GaudiPython applications in LHCb.'''

import tempfile
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IApplication import IApplication
import CMTscript
from GangaLHCb.Lib.Gaudi.CMTscript import parse_master_package
import Ganga.Utility.logging
from Ganga.Utility.files import expandfilename, fullpath
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from GaudiUtils import *
from Ganga.GPIDev.Lib.File import File

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def get_common_gaudi_schema():
    schema = {}
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(defvalue=None,
                                    typelist=['str','type(None)'],doc=docstr)
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    schema['package'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The user path to be used. After assigning this'  \
             ' you can do j.application.getpack(\'Phys DaVinci v19r2\') to'  \
             ' check out into the new location. This variable is used to '  \
             'identify private user DLLs by parsing the output of "cmt '  \
             'show projects".'
    schema['user_release_area'] = SimpleItem(defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read '  \
             'from. Can be written either as a path '  \
             '\"Tutorial/Analysis/v6r0\" or in a CMT style notation '  \
             '\"Analysis v6r0 Tutorial\"'
    schema['masterpackage'] = SimpleItem(defvalue=None,
                                         typelist=['str','type(None)'],
                                         doc=docstr)
    docstr = 'Extra options to be passed onto the SetupProject command '  \
             'used for configuring the environment. As an example '  \
             'setting it to \'--dev\' will give access to the DEV area. '  \
             'For full documentation of the available options see '  \
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    schema['setupProjectOptions'] = SimpleItem(defvalue='',
                                               typelist=['str','type(None)'],
                                               doc=docstr)
    return schema

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Francesc(IApplication):
    '''Parent for all Gaudi and GaudiPython applications, should not be used
    directly.'''    
    _name = 'Francesc'
    _exportmethods = ['getenv','getpack', 'make', 'cmt']
    _schema = Schema(Version(1, 1), {})

    def get_gaudi_appname(self):
        '''Handles the (unfortunate legacy) difference between Gaudi and
        GaudiPython schemas wrt this attribute name.'''
        appname = ''
        try: appname = self.appname
        except AttributeError:
            appname = self.project
        return appname

    def _init(self,gaudi_app,set_ura):        
        if (not self.version): self.version = guess_version(gaudi_app)
        if (not self.platform): self.platform = get_user_platform()
        if (not self.package): self.package = available_packs(gaudi_app)
        if not set_ura: return
        if not self.user_release_area:
            expanded = os.path.expandvars("$User_release_area")
            if expanded == "$User_release_area": self.user_release_area = ""
            else:
                self.user_release_area = expanded.split(os.pathsep)[0]

    def _check_gaudi_inputs(self,optsfiles,appname):
        """Checks the validity of some of user's entries."""
        for fileitem in optsfiles:
            fileitem.name = os.path.expanduser(fileitem.name)
            fileitem.name = os.path.normpath(fileitem.name)
    
        if appname is None:
            msg = "The appname is not set. Cannot configure."
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)
    
        if appname not in available_apps():
            if appname is 'Bender': return
            msg = "Unknown application %s. Cannot configure." % appname
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)
    
    def _getshell(self):
        appname = self.get_gaudi_appname()
        ver  = self.version
        opts = self.setupProjectOptions

        fd = tempfile.NamedTemporaryFile()
        script = '#!/bin/sh\n'
        if self.user_release_area:
            script += 'User_release_area=%s; export User_release_area\n' % \
                      expandfilename(self.user_release_area)
        useflag = ''
        if self.masterpackage:
            (mpack, malg, mver) = parse_master_package(self.masterpackage)
            useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
        script +='. SetupProject.sh %s %s %s %s\n'\
                  % (useflag, opts, appname, ver)
        fd.write(script)
        fd.flush()
        logger.debug(script)

        self.shell = Shell(setup=fd.name)
        logger.debug(pprint.pformat(self.shell.env))    
        fd.close()

    def getenv(self):
        '''Returns a copy of the environment used to flatten the options, e.g.
        env = DaVinci().getenv(), then calls like env[\'DAVINCIROOT\'] return
        the values.
        
        Note: Editing this does not affect the options processing.
        '''
        self._getshell()
        return self.shell.env.copy()
    
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
                
        command = 'getpack ' + options + '\n'
        CMTscript.CMTscript(self,command)

    def make(self, argument=''):
        """Build the code in the release area the application object points
        to. The actual command executed is "cmt broadcast make <argument>"
        after the proper configuration has taken place."""
        command = '###CMT### config \n ###CMT### broadcast make '+argument
        CMTscript.CMTscript(self,command)

    def cmt(self, command):
        """Execute a cmt command in the cmt user area pointed to by the
        application. Will execute the command "cmt <command>" after the
        proper configuration. Do not include the word "cmt" yourself."""
        command = '###CMT### config \n ###CMT### '+command
        CMTscript.CMTscript(self,command)

    def _master_configure(self):
        '''Handles all common master_configure actions.'''
        self.extra = GaudiExtras()
        debug_dir = self.getJobObject().outputdir
        debug_dir = debug_dir.rstrip('/')
        debug_dir = debug_dir.rstrip('/output')
        debug_dir += '/debug'
        if os.path.exists(debug_dir): os.system('rm -f %s/*' % debug_dir)
        self._getshell()
        
        job=self.getJobObject()                
        if job.inputdata:
            self.extra.inputdata = job.inputdata
            self.extra.inputdata.datatype_string=job.inputdata.datatype_string

        if job.outputdata:
            self.extra.outputdata = collect_lhcb_filelist(job.outputdata)
                        
        if not self.user_release_area: return debug_dir

        appname = self.get_gaudi_appname()
        dlls, pys, subpys = get_user_dlls(appname, self.version,
                                          self.user_release_area,self.platform,
                                          self.shell)

        self.extra.master_input_files += [File(f,subdir='lib') for f in dlls]
        self.extra.master_input_files += [File(f,subdir='python') for f in pys]
        for dir, files in subpys.iteritems():
            input_files = [File(f,subdir='python'+os.sep+dir) for f in files]
            self.extra.master_input_files += input_files

        return debug_dir

    def _configure(self):
        data_str = dataset_to_options_string(self.extra.inputdata)
        self.extra.input_buffers['data.opts'] += data_str
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiExtras:
    '''Used to pass extra info from Gaudi apps to the RT-handler.'''
    _name = "GaudiExtras"
    _category = "extras"

    def __init__(self):
        self.master_input_buffers = {}
        self.master_input_files = []
        self.input_buffers = {}
        self.input_files = []
        self.inputdata = LHCbDataset()
        self.outputsandbox = []
        self.outputdata = []
        self.input_buffers['data.opts'] = ''

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
