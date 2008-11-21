#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Gaudi applications in LHCb.'''

__author__ = 'Andrew Maier, Greig A Cowan'
__date__ = "$Date: 2008-11-13 10:02:53 $"
__revision__ = "$Revision: 1.17 $"

import os
import re
import sys
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename, fullpath
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from Splitters import *
import Ganga.Utility.logging
from GaudiUtils import *
import CMTscript
from GaudiLSFRunTimeHandler import * 
from GangaLHCb.Lib.Dirac.GaudiDiracRunTimeHandler import *
from PythonOptionsParser import PythonOptionsParser

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def GaudiDocString(appname):
    "Provide the documentation string for each of the Gaudi based applications"
    
    doc="""The Gaudi Application handler

    The Gaudi application handler is for running LHCb GAUDI framework
    jobs. For its configuration it needs to know the version of the application
    and what options file to use. More detailed configuration options are
    described in the schema below.

    An example of submitting a Gaudi job to Dirac could be:

    app = Gaudi(version='v99r0')

    # Give absolute path to options file. If several files are given, they are
    # just appended to each other.
    app.optsfile = ['/afs/...../myopts.opts']

    # Append two extra lines to the python options file
    app.extraopts=\"\"\"
    ApplicationMgr.HistogramPersistency ="ROOT"
    ApplicationMgr.EvtMax = 100
    \"\"\"

    # Define dataset
    ds = LHCbDataset(['LFN:foo','LFN:bar'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac())
    j.submit()

    """
    return doc.replace( "Gaudi", appname )


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Gaudi(IApplication):
    _name = 'Gaudi'
    __doc__ = GaudiDocString(_name)

    schema = {}
    docstr = 'The name of the optionsfile. Import statements in the file ' + \
             'will be expanded at submission time and a full copy made'
    schema['optsfile'] =  FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                   doc=docstr)
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' + \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(defvalue=None,
                                    typelist=['str','type(None)'],doc=docstr)
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    schema['package'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['appname'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],hidden=1,
                                   doc=docstr)
    docstr = 'The user path to be used. By default the value of the ' + \
             'User_release_area environment variable. After assigning this' + \
             ' you can do j.application.getpack(\'Phys DaVinci v19r2\') to' + \
             ' check out into the new location. This variable is used to ' + \
             'identify private user DLLs by parsing the output of "cmt ' + \
             'show projects".'
    schema['user_release_area'] = SimpleItem(defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read ' + \
             'from. Can be written either as a path ' + \
             '\"Tutorial/Analysis/v6r0\" or in a CMT style notation ' + \
             '\"Analysis v6r0 Tutorial\"'
    schema['masterpackage'] = SimpleItem(defvalue=None,
                                         typelist=['str','type(None)'],
                                         doc=docstr)

    schema['configured'] = SimpleItem(defvalue=None,
                                      typelist=['str','type(None)'],hidden=0,
                                      copyable=0)
    docstr = 'A python configurable string that will be appended to the ' + \
             'end of the options file. Can be multiline by using a ' + \
             'notation like \nHistogramPersistencySvc().OutputFile = ' + \
             '\"myPlots.root"\\nEventSelector().PrintFreq = 100\n or by ' + \
             'using triple quotes around a multiline string'
    schema['extraopts'] = SimpleItem(defvalue=None,
                                     typelist=['str','type(None)'],doc=docstr)

    docstr = 'Extra options to be passed onto the SetupProject command ' + \
             'used for configuring the environment. As an example ' + \
             'setting it to \'--dev\' will give access to the DEV area. ' + \
             'For full documentation of the available options see ' + \
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    schema['setupProjectOptions'] = SimpleItem(defvalue='',
                                               typelist=['str','type(None)'],
                                               doc=docstr)

    _schema = Schema(Version(2, 1), schema)
    _category = 'applications'
    _exportmethods = ['getpack', 'make', 'cmt']

    def _auto__init__(self):
        self.configured=0
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        if not self.appname:
            logger.debug("_auto__init called without an appname. " + \
                         "Nothing to configure")
            return 
                
        if not self.user_release_area:
            expanded = os.path.expandvars("$User_release_area")
            if expanded == "$User_release_area":
                self.user_release_area = ""
            else:
                self.user_release_area = expanded.split(os.pathsep)[0]
        logger.debug("Set user_release_area to: %s",
                     str(self.user_release_area))        
        
        if not self.version:  
            self.version = guess_version(self.appname)
        self.package = available_packs(self.appname)
        if (not self.platform):
            self.platform = get_user_platform()
        
    def master_configure(self):
        '''The configure method configures the application. Here, the
        application handler simply flattens the options file. For this it has
        to use CMT and gaudirun.py.

        configure() returns a tuple (changed,extra). The first element tells
        the client, if the configure has modified anything in the application
        object (that is the content of the schema), the second element contains
        the object with the extra information returned from the application
        configuration. In this case this is the flattened options file (as a
        string) The extra information is its own class'''

        self.shell = gaudishell_setenv(self)
        inputs = self._check_inputs() 
        self.extra = GaudiExtras()
        optsfiles = [fileitem.name for fileitem in self.optsfile]
                        
        try:
            parser = PythonOptionsParser(optsfiles,self.extraopts,self.shell)
        except Exception, e:
            msg = 'Unable to parse the job options. Please check options ' + \
                  'files and extraopts.'
            raise ApplicationConfigurationError(None,msg)

        self.extra.opts_pkl_str = parser.opts_pkl_str
        inputdata = parser.get_input_data()
  
        # If the user has specified the data in a dataset, use it and
        # ignore the optionsfile, but warn the user.
        job=self.getJobObject()
        if len(inputdata.files) > 0:
            if job.inputdata:
                msg = 'You specified a dataset for this job, but have ' + \
                      'also defined a dataset\n in your options file. I ' + \
                      'am going to ignore the options file.\n I hope ' + \
                      'this is OK.'
                logger.warning(msg)            
                self.extra.inputdata = job.inputdata
            else:
                logger.info('Using the inputdata defined in your options file')
                self.extra.inputdata = inputdata
        else:
            # If no input data in options file
            if job.inputdata:
                logger.info('Using the inputdata defined in your job.')
                self.extra.inputdata = job.inputdata
            else:
                logger.info('No inputdata is specified for this job.')
         
        # create a separate options file with only data statements.
        self.extra.dataopts = dataset_to_options_string(self.extra.inputdata)
        
        userdlls, mergedpys, subdirpys = get_user_dlls(self.appname,
                                                       self.version,
                                                       self.user_release_area,
                                                       self.shell)
        self.extra._userdlls = userdlls
        self.extra._merged_pys, self.extra._subdir_pys = mergedpys, subdirpys

        # get ouput and separate into sandbox vs data
        self.extra.outputsandbox,self.extra.outputdata = parser.get_output(job)

        # Get the site and the access protocol from config
        config=Ganga.Utility.Config.getConfig('LHCb')
        self.extra._LocalSite = config['LocalSite']
        self.extra._SEProtocol = config['SEProtocol']
        
        return (inputs, self.extra)

    def configure(self,master_appconfig):
        return (None,self.extra)
            
    def _check_inputs(self):
        """Checks the validity of some of user's entries for Gaudi schema"""

        check_gaudi_inputs(self.optsfile,self.appname)
        
        if self.package is None:
            msg = "The 'package' attribute is not set for application. " + \
                  'Not possible to continue.'
            raise ApplicationConfigurationError(None,msg)

        inputs = None
        if len(self.optsfile)==0:
            # cannot set file
            logger.warning("The 'optsfile' is not set")
            logger.warning("I hope this is OK.")
            packagedir = self.shell.env[self.appname.upper()+'ROOT']
            opts = os.path.expandvars(os.path.join(packagedir,'options',
                                                   self.appname + '.py'))
            if opts:
                self.optsfile.append(opts)
            else:
                logger.error('Cannot find the default opts file for ' + \
                             self.appname + os.sep + self.version)
            inputs = ['optsfile']

        return inputs
   
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
                    logger.error("Can not create cmt user directory: %s",
                                 cmtpath)
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


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiExtras:
    '''The GaudiExtras class. This allows us to add more to the application
    object than is defined in the schema.'''

    opts_pkl = ''
    dataopts = ''
    _SEProtocol = ''
    _LocalSite = ''
    _userdlls = []
    _merged_pys = []
    _subdir_pys = []
    inputdata = LHCbDataset()
    outputsandbox = []
    outputdata = []
    _name = "GaudiExtras"
    _category = "extras"

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#
# Individual Gaudi applications. These are thin wrappers around the Gaudi base 
# class. The appname property is read protected and it tries to guess all the
# properties except the optsfile.
#

# Some generic stuff common to all classes
myschema = Gaudi._schema.inherit_copy()
myschema['appname']._meta['protected'] = 1

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Gauss(Gaudi):
    
    _name = 'Gauss'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Gauss, self).__init__()
        self.appname = "Gauss"
        
    def getpack(self,options=''):
        return super(Gauss,self).getpack(options)
        
    def make(self,argument=''):
        return super(Gauss,self).make(argument)
        
    def cmt(self,command):
        return super(Gauss,self).cmt(command)
   
    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Boole(Gaudi):
    
    _name = 'Boole'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Boole, self).__init__()
        self.appname = "Boole"
        
    def getpack(self,options=''):
        return super(Boole,self).getpack(options)
    
    def make(self,argument=''):
        return super(Boole,self).make(argument)
    
    def cmt(self,command):
        return super(Boole,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Brunel(Gaudi):
    
    _name = 'Brunel'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Brunel, self).__init__()
        self.appname = "Brunel"
        
    def getpack(self,options=''):
        return super(Brunel,self).getpack(options)
    
    def make(self,argument=''):
        return super(Brunel,self).make(argument)
    
    def cmt(self,command):
        return super(Brunel,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DaVinci(Gaudi):
    
    _name = 'DaVinci'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(DaVinci, self).__init__()
        self.appname = "DaVinci"
        
    def getpack(self,options=''):
        return super(DaVinci,self).getpack(options)
    
    def make(self,argument=''):
        return super(DaVinci,self).make(argument)
    
    def cmt(self,command):
        return super(DaVinci,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Euler(Gaudi):
    
    _name = 'Euler'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Euler, self).__init__()
        self.appname = "Euler"
        
    def getpack(self,options=''):
        return super(Euler,self).getpack(options)
    
    def make(self,argument=''):
        return super(Euler,self).make(argument)
    
    def cmt(self,command):
        return super(Euler,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Moore(Gaudi):
    
    _name = 'Moore'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Moore, self).__init__()
        self.appname = "Moore"
        
    def getpack(self,options=''):
        return super(Moore,self).getpack(options)
    
    def make(self,argument=''):
        return super(Moore,self).make(argument)
    
    def cmt(self,command):
        return super(Moore,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Vetra(Gaudi):
    
    _name = 'Vetra'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Vetra, self).__init__()
        self.appname = "Vetra"
        self.lhcb_release_area=os.path.expandvars("$Vetra_release_area")
        
    def getpack(self,options=''):
        return super(Vetra,self).getpack(options)
    
    def make(self,argument=''):
        return super(Vetra,self).make(argument)
    
    def cmt(self,command):
        return super(Vetra,self).cmt(command)

    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Panoptes(Gaudi):
    
    _name = 'Panoptes'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    
    def __init__(self):
        super(Panoptes, self).__init__()
        self.appname = 'Panoptes'
        
    def getpack(self,options=''):
        return super(Panoptes,self).getpack(options)
    
    def make(self,argument=''):
        return super(Panoptes,self).make(argument)
    
    def cmt(self,command):
        return super(Panoptes,self).cmt(command)

    # Copy documentation from Gaudi class
    for method in Gaudi._exportmethods:
        setattr(eval(method), "__doc__", getattr(Gaudi, method).__doc__)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#
# Associate the correct run-time handlers to Gaudi for various backends. 
#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

for app in available_apps():
    allHandlers.add(app, 'LSF', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Interactive', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'PBS', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'SGE', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Local', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRunTimeHandler)
    allHandlers.add(app, 'Condor', GaudiLSFRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
