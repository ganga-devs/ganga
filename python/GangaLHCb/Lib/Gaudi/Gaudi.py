#!/usr/bin/env python

'''
Application handler for Gaudi applications in LHCb.
Uses the GPI to implement an application handler
'''

__author__ = 'Andrew Maier, Greig A Cowan'
__date__ = 'June 2008'
__revision__ = 0.1

## Import the GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from Splitters import *
from Ganga.Utility.util import unique

import os, re
import sys

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import env # enables setting of the environment (unmodified from Ganga3)
import CMTscript
from GaudiLSFRunTimeHandler import * 
from GangaLHCb.Lib.Dirac.GaudiDiracRunTimeHandler import *

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

_available_apps = ["Gauss", "Boole", "Brunel", "DaVinci",
                   "Euler", "Moore", "Vetra", "Panoramix",
                   "Panoptes", "Gaudi"]

_available_packs={'Gauss'   : 'Sim',
                  'Boole'   : 'Digi',
                  'Brunel'  : 'Rec',
                  'DaVinci' : 'Phys',
                  'Euler'   : 'Trg',
                  'Moore'   : 'Hlt',
                  'Vetra'   : 'Velo',
                  'Panoptes': 'Rich'}

class Gaudi(IApplication):
    _name = 'Gaudi'
    __doc__=GaudiDocString(_name)

# Set up the schema for this application
    _schema = Schema(Version(2, 0), {
            'optsfile': FileItem(sequence=1,strict_sequence=0,defvalue=[],doc='''List of options files. Import 
            statements in the file will be expanded at submission time and a 
            full copy made'''),
            
            'version': SimpleItem(defvalue=None,typelist=['str'],doc='''The version of the 
            application (like "v19r2")'''),
            
            'platform': SimpleItem(defvalue = None, typelist=['str'], doc='''The 
            platform the application is configured for (e.g. "slc4_ia32_gcc34")'''),
            
            'package': SimpleItem(defvalue=None, typelist=['str'], doc='''The package the 
            application belongs to (e.g. "Sim", "Phys")'''),
            
            'appname': SimpleItem(defvalue = None, typelist=['str'],hidden = 1, doc='''The 
            name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'''),
            
            'lhcb_release_area': SimpleItem(defvalue = None, typelist=['str'], hidden = 0),
            
            'user_release_area': SimpleItem(defvalue=None, typelist=['str'],doc='''The  
            user path to be used. By default the value of the first element in the 
            CMTPROJECTPATH environment variable. After assigning this you can do 
            j.application.getpack(\'Phys DaVinci v19r2\') to check out into the new location.
            This variable is used to identify private user DLLs by parsing the output of
            "cmt show projects".'''),
            
            'masterpackage': SimpleItem(defvalue=None, typelist=['str'],doc='''The package 
            where your top level requirements file is read from. Can be written either as 
            a path "Tutorial/Analysis/v6r0" or in a CMT style notation "Analysis v6r0 Tutorial"'''),
            
            'configured': SimpleItem(defvalue = None, typelist=['str'],hidden = 0,copyable=0),
            
            'extraopts': SimpleItem(defvalue=None,typelist=['str'],doc='''A python configurable 
            string that will be appended to the end of the options file. Can be multiline by using 
            a notation like \nHistogramPersistencySvc().OutputFile = "myPlots.root"\\nEventSelector().PrintFreq = 
            100\n or by using triple quotes around a multiline string'''),
#            'outputdatatypes':SimpleItem(defvalue=['NTUPLE','DST','SIM', 'DIGI'],sequence=1,doc='''
#            list of data that will be returned as output data and not in the output sandbox. Possible values are 'HISTO', 'NTUPLE', 'DST', 'DIGI', 'SIM'. Note that some backends might not return large files if they are put into the output sandbox''')
            })
    _category = 'applications'
    _exportmethods = ['getpack', 'make', 'cmt']

    def _auto__init__(self):
        self.configured=0
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        if not self.appname:
            logger.debug("_auto__init called without an appname. Nothing to configure")
            return 
        
        if not self.lhcb_release_area:
            self.lhcb_release_area = os.path.expandvars("$LHCBRELEASES")
        else:
            self.lhcb_release_area = os.path.expandvars(self.lhcb_release_area)
        logger.debug( "self.lhcb_release_area: %s", str(self.lhcb_release_area))
        
        if not self.user_release_area:
            if os.path.expandvars("$User_release_area") == "$User_release_area":
                self.user_release_area = ""
            else:
                self.user_release_area = os.path.expandvars("$User_release_area").split(os.pathsep)[0]
        logger.debug("Set user_release_area to: %s",str(self.user_release_area))        
        
        if (not self.version) and self.lhcb_release_area:  
            self.version = self.guess_version(self.appname, self.lhcb_release_area)
        self.package = _available_packs[self.appname]
        #self.platform = self.list_choices("platform")
        # Using _get_user_platform not exactly the same as list_choices since it will
        # force _setUpEnvironment() to be called if CMTCONFIG not defined. 
        self.platform = self._get_user_platform()
      

    def master_configure(self):
        '''The configure method configures the application. Here, the application
        handler simply flattens the options file. For this it has to use CMT and gaudirun.py. 
        configure() returns a tuple (changed,extra). The first
        element tells the client, if the configure has modified anything in the 
        application object (that is the content of the schema), the second element
        contains the object with the extra information returned from the application
        configuration. In this case this is the flattened options file (as a string)
        The extra information is its own class'''

        inputs = self._checkInputs() # returns an empty list if all inputs are OK
        self.extra = GaudiExtras()

        self._setUpEnvironment()  

        optsfilelist = [fileitem.name for fileitem in self.optsfile]
                        
        try:
            import PythonOptionsParser
            parser = PythonOptionsParser.PythonOptionsParser( optsfilelist, self.extraopts, 
                                                              self.shell)
        except Exception, e:
            logger.error('Unable to parse the options file')

        self.extra.opts_pkl_str = parser.opts_pkl_str

        inputdata = parser.get_input_data()
  
        # If the user has specified the data in a dataset, use it and
        # ignore the optionsfile, but warn the user.
        job=self.getJobObject()
        if job.inputdata != None and inputdata:
            logger.warning("You specified a dataset for this job, but have also defined a dataset")
            logger.warning("in your options file. I am going to ignore the options file.")
            logger.warning("I hope this is OK")
            
            self.extra.inputdata = [x.name for x in job.inputdata.files]
        
        if job.inputdata == None and inputdata:
            # Strip off the Gaudi card stuff. This places the inputdata in the 
            # same form as the inputdata when defined as an LHCbDataset.
            self.extra.inputdata = [x.split('\'')[1] for x in inputdata]
        
        # create a separate options file with only data statements.
        self.extra.dataopts = self._dataset2optionsstring(self.extra.inputdata)

        self.extra._userdlls, self.extra._merged_confDBs, self.extra._subdir_confDBs = self._get_user_dlls()
        self.extra._outputfiles = parser.get_output_files()
        self.extra.outputdata = parser.get_output_data()
        
        if type(job.outputdata) == type(['a','b']):
            self.extra.outputdata += job.outputdata
        if type(job.outputdata) == type(LHCbDataset()):
            self.extra.outputdata += [f.name for f in job.outputdata.files]
        
        self.extra.outputdata
        
        # Get the site and the access protocol from config
        import Ganga.Utility.Config
        config=Ganga.Utility.Config.getConfig('LHCb')
        self.extra._LocalSite = config['LocalSite']
        self.extra._SEProtocol = config['SEProtocol']
#        [opt['Value'] for opt in gaudiopts if opt['Type']=='option' and opt['Name']=='OutputFile']
        
        if inputs: 
            return (inputs, self.extra)
        else:
            return (None, self.extra)


    def configure(self,master_appconfig):
        return (None,self.extra)


    def _get_requirements(self):
        list=self._parseMasterPackage()
        return

#    def _dataset2optionsstring(self,ds):
#        '''This creates a python options file for the input data.
#           Cannot use this at the moment due to genCatalog.'''
#        s  = 'from Configurables import EventSelector\n'
#        s += 'sel = EventSelector()\n'
#        s += 'sel.Input = ['
#        if type(ds) == type([1,2,3]):
#            for f in ds:
#                s += ''' "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'",''' % f
#            if s.endswith(','):
#                logger.debug('_dataset2optsstring: removing trailing comma')
#                s=s[:-1]
#            s += ']'
#        else:
#            for f in ds.files:
#                s += ''' "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'",''' % f.name
#            if s.endswith(','): 
#                logger.debug('_dataset2optsstring: removing trailing comma')
#                s=s[:-1]
#            s += ']'
#        return s

    def _dataset2optionsstring(self,ds):
        s=''
        if type(ds) == type([1,2,3]):
            s='EventSelector.Input   = {'
            for k in ds:
                s+='\n'
                s+=""" "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'",""" %k

            #Delete the last , to be compatible with the new optiosn parser
            if s.endswith(","):
                logger.debug("_dataset2optsstring: removing trailing comma")
                s=s[:-1]

            s+="""\n};"""
        else:
            s='EventSelector.Input   = {'
            for k in ds.files:
                s+='\n'
                s+=""" "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'",""" %k.name
            #Delete the last , to be compatible with the new optiosn parser
            if s.endswith(","):
                logger.debug("_dataset2optsstring: removing trailing comma")
                s=s[:-1]
            s+="""\n};"""
        return s

    #######################################################################
    # available_versions                                                  #
    #                    given a package name, try to evaluate the        #
    #                    available versions of the application            #
    #                                                                     #
    #                    This only works for a standard LHCb installation #
    #                    e.g. on afs                                      #
    #######################################################################
    def available_versions(self,appname, release_area, dev = None):
      """Try to extract the list of installed versions for a given application"""
      if appname not in _available_apps:
        raise ValueError, "Application " + appname + " not known"
      app_upper = appname.upper()
      app_upper_ = app_upper+"_"

      if not release_area:
        raise ValueError, """
        The LHCb envrinment variables are not set (LHCBRELEASES, LHCBDEV)
        """

      dirlist = os.listdir(release_area+os.sep+app_upper)
      versions = []
      for i in dirlist:
        if i.startswith(app_upper_):
          versions.append(i.replace(app_upper_, ""))
      versions.sort(self.versions_compare)
      return versions

       
    #######################################################################
    #  guess_version                                                      #
    #                Given an appname, return the last entry in the list  #
    #                of available applications                            #
    #######################################################################
    def guess_version(self,appname, release_area, dev = None):
      """Try to guess the correct version for a given application
         If the the environment variable consisting of the upper
         case name of the application + ENVROOT exists. The version
         will be taken from this variable"""
      if appname not in _available_apps:
        raise ValueError, "Application " + appname + " not known"
      # check if the user has used ProjectEnv to select a certain version
      # if so choose that version, if not choose the latest
      if os.getenv(appname.upper()+"ENVROOT"):
        return os.path.basename(os.getenv(appname.upper()+"ENVROOT"))
      else: 
        versions = self.available_versions(appname, release_area, dev)
        return versions[-1]


    #######################################################################
    # versions_compare                                                    #
    #                 Compare version strings for LHCb apps (needed to    #
    #                 sort the available versions. Taken from Ganga 3     #
    #######################################################################
    # GC: do we really need this versions compare function?
    # GC: os command for listing the dir should return the dirs in correct order.
    def versions_compare(self,x, y):
      """A simple function to compare versions of the format vXrY, with 
      X and Y being the major and minor version, respectively"""
      nbs = []
      for i in (x, y):
        rev = i.find('r')
        patch = i.find('p')
        try:
          if patch == -1:
            nb = map(int, (i[1:rev], i[rev+1:], '0'))
          else:
            nb = map(int, (i[1:rev], i[rev+1:patch], i[patch+1:]))
        except:
          nb = (0, 0, 0)
        nbs.append(nb)
      return cmp(nbs[0], nbs[1])

    
    #######################################################################
    # guess_package                                                       #
    #               given an application name guess the corresponding     # 
    #               LHCb package name                                     #
    #######################################################################
    def guess_package( self,appname):
      """Guess the package corresponding to a given application"""

      if appname in known_packs.keys():
          return known_packs[appname]
      else:
          return "UNKNOWN"
    
   
    def _setUpEnvironment( self): 
        self.shell=env._setenv( self)


    def _parseMasterPackage( self):
        # first check if we have slashes
        if self.masterpackage.find('/')>=0:
            try:
                list=self.masterpackage.split('/')
                if len(list)==3:
                    return list
                elif len(list)==2:
                    list.insert(0,'')
                    return list
                else:
                    raise ValueError,"wrongly formatted masterpackage"
            except:
                pass
        elif self.masterpackage.find(' ')>=0:
            try:
                list=self.masterpackage.split()
                if len(list)==3:
                    list = (list[2],list[0],list[1])
                    return list
                elif len(list)==2:
                    list=('',list[0],list[1])
                    return list
                else:
                    raise ValueError,"wrongly formatted masterpackage"
            except:
                pass
        else:
            raise ValueError,"wrongly formatted masterpackage"
            
            
    def _checkInputs( self):
        # Internal method to check the inputs
        # Go through the schema one by one and check if
        # we can guess the value
        # also normalise and expand filenames
        for fileitem in self.optsfile:
            fileitem.name = os.path.expanduser(fileitem.name)
            fileitem.name = os.path.normpath(fileitem.name)

        result = []
        ##############################
        #         appname            #
        ##############################
        if self.appname is None:
          #cannot guess issue error
            logger.error("The appname is not set. Cannot configure")
            raise ApplicationConfigurationError(None, "The appname is not set. Cannot configure")
        if self.appname not in _available_apps:
            logger.error("Unknown applications "+self.appname+". Cannot configure")
            raise ApplicationConfigurationError(None, "Unknown applications "+self.appname+". Cannot configure")
        ##############################
        #       version              #
        ##############################
        if self.version is None:
            self.version = self.guess_version(self.appname)
            logger.warning("The 'version' is not set. Setting it to "+self.version+".")
            logger.warning("I hope this is OK.")
            result.append("version")
        ##############################
        #     optsfile               #
        ##############################
        if len(self.optsfile)==0:
            # cannot set file
            logger.warning("The 'optsfile' is not set")
            logger.warning("I hope this is OK.")
            opts = os.path.expandvars(self.lhcb_release_area + os.sep + self.appname.upper() + os.sep + \
                                      self.appname.upper() + '_' + self.version + os.sep + \
                                      self.package + os.sep + self.appname + os.sep + self.version + \
                                      os.sep + 'options' + os.sep + self.appname + '.py')
            if opts:
                self.optsfile.append(opts)
            else:
                logger.error('Cannot find the default opts file for ' + self.appname + os.sep + self.version)
            result.append('optsfile')
#            raise ApplicationConfigurationError(None, "The 'optsfile' is not set")
        ##############################
        #     platform               #
        ##############################
        if self.platform is None:
            #plattform is set up in _setEnvironment
            pass
          
        ##############################
        #     package                #
        ##############################
        if self.package is None:
            self.package = self.guess_package(self.appname)
            logger.warning("Set package to " + self.package + ".")
            logger.warning("I hope this is OK.")
            result.append("package")
        return result
  

    def _get_user_release_area(self, env=os.environ):
        """Get the User release area for the job.        
        For the moment only rely on environment. Should be updated to take into account the 
        properties in the Gaudi job
        """
        if env.has_key('User_release_area'):
            releaseArea=env['User_release_area']
        else:
            logger.info('"User_release_area" is not set. Expect problems with configuring your job')
            releaseArea=''

        if self.user_release_area and releaseArea != self.user_release_area:
            if env.has_key('CMTPROJECTPATH'):
                cmtpp=env['CMTPROJECTPATH'].split(':')
                if cmtpp[0]!=self.user_release_area:
                    cmtpp[0]=self.user_release_area
                    env['CMTPROJECTPATH']=':'.join(cmtpp)
                    
            releaseArea = self.user_release_area.split(':')[0]

        return releaseArea


    def _get_user_platform(self,env=os.environ):
        if env.has_key('CMTCONFIG'):
            platform=env['CMTCONFIG']
        else:
            logger.info('"CMTCONFIG" not set. Cannot determin the platform you want to use')
            platform=''
        
        # GC: This allows a user to define a platform different from the default one.
        # I don't like this function since this step isn't actually getting the platform
        # but is rather setting it.
        if platform != self.platform and self.platform != None:
            if env.has_key('CMTCONFIG'):
                platform=self.platform
                env['CMTCONFIG']=platform
        return platform        

    def _get_user_dlls(self):
        from Ganga.Utility.files import fullpath
        import pprint
        libs=[]
        merged_confDBs = []
        subdir_confDBs = {}

        user_ra = self._get_user_release_area()
        full_user_ra = fullpath( user_ra) # expand any symbolic links
        platform = self._get_user_platform()
        
        # Initially I thought I should use this path...
        user_cmt_path = os.path.join( user_ra, self.appname + '_' + self.version,
                                      self.package, self.appname, self.version, 'cmt')
        # ...but now I think this is correct.
        user_cmt_path = os.path.join( user_ra, self.appname + '_' + self.version, 
                                      'cmt')
        lhcb_cmt_path = os.path.join( self.lhcb_release_area, self.appname.upper(),
                                      self.appname.upper() + '_' + self.version,
                                      'cmt')
        dir = ''
        if self.masterpackage:
            package, name, version = self._parseMasterPackage()
            dir = os.path.join( user_ra, name + '_' + version, package, 'cmt')
        elif os.path.exists( user_cmt_path):
            dir = user_cmt_path
        elif os.path.exists( lhcb_cmt_path):
            dir = lhcb_cmt_path
        else:
            logger.error( 'Cannot find the application CMT directory')
        logger.debug( 'Trying to use this directory ', dir)
        # 'cd ' +dir + ';
        rc, showProj, m = self.shell.cmd1( 'cd ' + dir +';cmt show projects', 
                                           capture_stderr=True)
        
        logger.debug( showProj)
 
        project_areas = []
        py_project_areas = []
        for line in showProj.split('\n'):
            for entry in line.split():
                if entry.startswith( user_ra) or entry.startswith( full_user_ra):
                    libpath = fullpath( os.path.join(entry.rstrip('\)'), 'InstallArea',platform,'lib'))
                    logger.debug( libpath)
                    project_areas.append( libpath)
                    pypath  = fullpath( os.path.join(entry.rstrip('\)'), 'InstallArea','python'))
                    logger.debug( pypath)
                    py_project_areas.append( pypath)
        
        for libpath in project_areas:
            if os.path.exists( libpath):
                for f in os.listdir( libpath):
                    fpath = os.path.join( libpath,f)
                    if os.path.exists( fpath):
                        libs.append( fpath)
                    else:
                        logger.warning( "File %s in %s does not exist. Skipping...",str(f),str(libpath))
                           
        for pypath in py_project_areas:
            if os.path.exists( pypath):
                for f in os.listdir( pypath):
                    confDB_path = os.path.join( pypath, f)
                    if confDB_path.endswith( '_merged_confDb.py'):
                        if os.path.exists( confDB_path):
                            merged_confDBs.append( confDB_path)
                        else:
                            logger.warning( "File %s in %s does not exist. Skipping...",str( f), str( confDB_path))
                    elif os.path.isdir( confDB_path):
                        pyfiles = []
                        for g in os.listdir( confDB_path):
                            file_path = os.path.join( confDB_path, g)
                            if (file_path.endswith( '_confDb.py') or file_path.endswith( 'Conf.py') or file_path.endswith( '__init__.py')):
                                if os.path.exists( file_path):
                                    pyfiles.append( file_path)
                                else:
                                    logger.warning( "File %s in %s does not exist. Skipping...",str( g), str( f))                                
                        subdir_confDBs[ f] = pyfiles
                    
        logger.debug("%s",pprint.pformat( libs))
        logger.debug("%s",pprint.pformat( merged_confDBs))
        logger.debug("%s",pprint.pformat( subdir_confDBs))

        return libs, merged_confDBs, subdir_confDBs     


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
                    logger.error("Can not create cmt user directory: %s", cmtpath)
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


class GaudiExtras:
    '''The GaudiExtras class. This allows us to add more to the application
    object than is defined in the schema.'''

    opts_pkl = ''
    dataopts = ''
    _SEProtocol = ''
    _LocalSite = ''
    _userdlls = []
    _merged_confDBs = []
    _subdir_confDBs = []
    inputdata = []
    _outputfiles = []
    outputdata = []
    _name = "GaudiExtras"
    _category = "extras"

################################################################################
# Individual Gaudi applications. These are thin wrappers around the Gaudi base # 
# class. The appname property is read protected and it tries to guess all the  #
# properties except the optsfile.                                              #
################################################################################

# Some generic stuff common to all classes

myschema = Gaudi._schema.inherit_copy()
myschema['appname']._meta['protected'] = 1
###############################################################################
#                             Gauss                                           #
###############################################################################
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

    
    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)


###############################################################################
#                             Boole                                           #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             Brunel                                          #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             DaVinci                                         #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             Euler                                           #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             Moore                                           #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             Vetra                                           #
###############################################################################

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

    # Copy documentation from Gaudi class
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)

###############################################################################
#                             Panoptes                                        #
###############################################################################

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
    for methodname in Gaudi._exportmethods:
        baseMethod = getattr( Gaudi, methodname )
        setattr( eval(methodname), "__doc__", baseMethod.__doc__)


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

for app in _available_apps+["Gaudi"]:
    allHandlers.add(app, 'LSF', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Interactive', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'PBS', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'SGE', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Local', GaudiLSFRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRunTimeHandler)
    allHandlers.add(app, 'Condor', GaudiLSFRunTimeHandler)

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_optsfile_shortcut(v,item):
    print 'in here'
    if type(v) is type(''):
        return [ v ]
    return None

allComponentFilters['applications']=string_optsfile_shortcut

#
#
# $Log: not supported by cvs2svn $
# Revision 1.2.2.1  2008/08/05 09:59:08  gcowan
# Extended _get_user_dlls to pick up all relevant files under InstallArea/python in the users private project areas. This directory structure is now replicated in the job inputsandbox. PYTHONPATH in the job wrappers is modified to prepend it with `pwd`/python so that the python configurables in the input sandbox are picked up by the job.
#
# Revision 1.2  2008/08/01 15:52:11  uegede
# Merged the new Gaudi application handler from branch
#
# Revision 1.1.2.1  2008/07/28 10:53:06  gcowan
# New Gaudi application handler to deal with python options. LSF and Dirac runtime handlers also updated. Old code removed.
#
# Revision 1.87.6.11.2.7  2008/07/15 17:53:51  gcowan
# Modified PythonOptionsParser to pickle the options file. This is converted to a string and added to the Gaudi.extras. The string can then be converted back to an options.pkl file within the runtime handlers and added to the job sandboxes. This replaces the need to use flat_opts. There is no need to have the format() method in PythonOptionsParser.
#
# Revision 1.87.6.11.2.6  2008/07/14 19:08:38  gcowan
# Major update to PythonOptionsParser which now uses gaudirun.py to perform the complete options file flattening. Output flat_opts.opts file is made available and placed in input sandbox of jobs. LSF and Dirac handlers updated to cope with this new design. extraopts need to be in python. User can specify input .opts file and these will be converted to python in the flattening process.
#
# Revision 1.87.6.11.2.5  2008/07/10 17:36:10  gcowan
# Modified GaudiDirac RT handler to support python options files. Small bug fixed in PythonOptions where a string was returned rather than a list.
#
# Revision 1.87.6.11.2.4  2008/07/09 00:11:49  gcowan
# Modified Gaudi._get_user_dlls() to address Savannah bug #31165. This should allow Ganga to pick up user DLLs from multiple user project areas.
#
# Modified GaudiLSFRunTimeHandler to look for gaudirun.py in the correct location.
#
# Revision 1.87.6.11.2.3  2008/07/03 12:52:07  gcowan
# Can now successfully submit and run Gaudi jobs using python job options to Local() and Condor() backends. Changes in Gaudi.py, GaudiLSFRunTimeHandler.py, PythonOptionsParser.py, Splitters.py and GaudiDiracRunTimeHandler.py. More substantial testing using alternative (and more complex) use cases required.
#
# Revision 1.87.6.11.2.2  2008/06/22 18:09:47  gcowan
# Removed the Shell object from PythonOptionsParser, now using the environment that is already setup in the Gaudi object.
#
# Modified Gaudi.py to use PythonOptionsParser to extract out Input and Output data files. Added comments to the code as I have been reading through it to suggest changes and unneccessary code.
#
# Revision 1.1.2.1  2008/06/19 16:43:28  gcowan
# Initial import of new code. Modifed Gaudi.py to use new PythonOptionsParser. This uses gaudirun.py to pickle the python options file and can easily extract the user input and output data files.
#
# Revision 1.87.6.11  2008/06/13 08:50:29  uegede
# Updated Gaudi handler
# - To allow platform to be modified
# - To work with python style options
#
# Revision 1.87.6.10  2008/06/11 21:17:16  uegede
# Allow the use of python options for Gaudi jobs
#
# Revision 1.87.6.9  2008/05/22 20:50:14  uegede
# Updates to Gaudi.py and GaudiLSFRunTimeHandler to deal better with the
# ability to give a list of options files.
# Test cases updated to new exceptions in Ganga-5.
#
# Revision 1.87.6.8  2008/05/07 15:34:00  uegede
# Updated 5.0 branch from trunk.
#
# Revision 1.87.6.7  2008/05/07 14:31:27  uegede
# Merged from trunk
#
# Revision 1.87.6.6  2008/04/25 14:26:21  wreece
# calls ApplicationConfigurationError with the right arguments
#
# Revision 1.100  2008/04/25 16:07:24  wreece
# changes to gaudi and a few backports from 5.0
#
# Revision 1.99  2008/04/25 01:15:07  uegede
# Work around for identified bug in os.path.realpath in Python 2.3.4.
# Revision 1.87.6.5  2008/04/04 15:11:38  andrew
# Schema changes:
#   * make optsfile a list
#   * rename cmt_user_path to user_release_area
#   * rename cmt_release_area to lhcb_release_area
#
# Add type info to Gaudi schema
#
# Adapt code for schema changes
#
# Revision 1.87.6.4  2008/04/04 10:01:01  andrew
# Merge from head
#
# Revision 1.87.6.3  2008/03/17 11:08:27  andrew
# Merge from head
#
# Revision 1.98  2008/03/07 15:13:40  andrew
#
# Fixes for:
#
# - [-] Bug fixes
#     - [+] bug #28955: cmt.showuses() broken
#     - [+] bug #33367: Option file format changed for specifying XML
#           slice
#     - [+] bug #29368: Dataoutput variable wrongly flagged as undefined
#     - [+] bug #33720: Multiple inclusion of options file
#
#
# Removes CERN centricity of the Gaudi wrapper script for batch and interactive
#
# Revision 1.97  2008/03/07 14:51:20  uegede
# Lib/Gaudi/Gaudi.py : Fixed bug 33800
# test/__init__.py   : Updated version of tutorial package used
# Added tests.
#
# Revision 1.96  2008/02/29 15:44:42  andrew
# Fix for bug #28955
#
# Revision 1.95  2008/02/24 22:19:47  andrew
# fix for bug 33799 (Stupid mixup of the usage of join)
#
# Revision 1.94  2008/02/18 12:36:26  andrew
# Fix missing s in FileCataogs.Catalog(s) in _determine_catalog_type()
#
# use _determine_catalog_type in GaudiLSFHandler
#
# Revision 1.93  2008/02/15 13:24:16  uegede
# - Updated Dirac backend to work with new XML file catalog specification.
# - Fixed bug in DLL identification.
#
# Revision 1.92  2008/02/14 16:07:17  andrew
# Added _determine_catalog_type function in Gaudi to determine which type of
# catalog option is needed (FileCatalogs.Catalog or PoolDbCacheSvc.Catalog
#
# Revision 1.91  2008/01/23 23:27:56  uegede
# - Leftover import of GaudiLCG removed.
#
# Revision 1.90  2008/01/23 23:15:42  uegede
# - Changed default DIRAC version to v2r18
# - Changed magic line in python script for DIRAC to have
#   "/bin/env python". this ensures that python version which is in PATH
#   is started.
# - Removed Panoramix application type as it never worked
# - Removed GaudiLCG runtime handler as it is not functional.
# Revision 1.87.6.2  2007/12/12 19:53:59  wreece
# Merge in changes from HEAD.
#
# Revision 1.89  2007/10/15 15:30:22  uegede
# Merge from Ganga_4-4-0-dev-branch-ulrik-dirac with new Dirac backend
#
# Revision 1.88  2007/10/09 14:35:17  andrew
# Fixes from Karl
#
# Revision 1.87.2.3  2007/10/08 11:36:01  uegede
# - Added test cases
# - Debugged retrieval of outputdata in Dirac backend. New exported function
#   getOutputData
# - Detects oversized  sandboxes gone to SE and download them
# - Made cmt_release_areanon-hidden.
#
# Revision 1.87.2.2  2007/09/07 15:08:38  uegede
# Dirac backend and runtime handlers updated to be controlled by a Python script.
# Gaudi jobs work with this as well now.
# Some problems with the use of absolute path in the DIRAC API are still unsolved.
# See workaround implemented in Dirac.py
#
# Revision 1.87.2.1  2007/08/15 15:50:14  uegede
# Develop a new dirac backend handler. Work in progress. The Executable and Root appliaction
# should work but Gaudi doesn't.
#
# Revision 1.87  2007/07/30 14:55:20  andrew
# Fix for rootmap files (from Ulrik)
#
# Revision 1.86  2007/07/26 14:36:17  andrew
# Fix for the getting the current version of the testapp
#
# Fixes for:
#
# - [-] Bug fixes
#     - [+] bug #28955: cmt.showuses() broken
#     - [+] bug #33367: Option file format changed for specifying XML
#           slice
#     - [+] bug #29368: Dataoutput variable wrongly flagged as undefined
#     - [+] bug #33720: Multiple inclusion of options file
#
#
# Removes CERN centricity of the Gaudi wrapper script for batch and interactive
#
