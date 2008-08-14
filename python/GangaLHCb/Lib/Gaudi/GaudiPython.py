#!/usr/bin/env python

'''
Application handler for GaudiPython applications in LHCb.
'''

__author__ = 'Ulrik Egede'
__date__ = 'August 2008'
__revision__ = 0.1

## Import the GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
from GangaLHCb.Lib.LHCbDataset import LHCbDataset

import os, re
import sys

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import env # enables setting of the environment

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

class GaudiPython(IApplication):
    """The Gaudi Application handler
    
    The GaudiPython application handler is for running LHCb GaudiPython
    jobs. This means running scripts where you are in control of the events
    loop etc. If you are usually running jobs using the gaudirun script
    this is *not* the application handler you should use. Instead use the
    DaVinci, Gauss, ... handlers.

    For its configuration it needs to know what application and version to
    use for setting up the environment. More detailed configuration options
    are described in the schema below.
    
    An example of submitting a GaudiPython job to Dirac could be:
    
    app = GaudiPython(project='DaVinci', version='v19r14')

    # Give absolute path to the python file to be executed. 
    # If several files are given the subsequent ones will go into the
    # sandbox but it is the users responsibility to include them
    app.script = ['/afs/...../myscript.py']

    # Define dataset
    ds = LHCbDataset(['LFN:foo','LFN:bar'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac())
    j.submit()

"""
    _name = 'GaudiPython'

# Set up the schema for this application
    _schema = Schema(Version(1, 0), {
            'script': FileItem(sequence=1,strict_sequence=0,defvalue=[],doc='''The name of the script to execute. A copy will be made at submission time'''),
            
            'version': SimpleItem(defvalue=None,typelist=['str'],doc='''The version of the 
            project (like "v19r2")'''),
            
            'platform': SimpleItem(defvalue = None, typelist=['str'],
                                   doc='''The platform the application is configured for (e.g. "slc4_ia32_gcc34")'''),
            
            'project': SimpleItem(defvalue = None, typelist=['str'],
                                  doc='''The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'''),
            
            'lhcb_release_area': SimpleItem(defvalue = None, typelist=['str'],
                                            doc = 'The release area'),
            
            })
    _category = 'applications'

    def _auto__init__(self):
        if not self.lhcb_release_area:
            self.lhcb_release_area = os.path.expandvars("$LHCBRELEASES")
        else:
            self.lhcb_release_area = os.path.expandvars(self.lhcb_release_area)
        logger.debug( "self.lhcb_release_area: %s", str(self.lhcb_release_area))
        if (not self.project):
            self.project = 'DaVinci'
            
        if (not self.version) and self.lhcb_release_area:  
            self.version = self.guess_version(self.project, self.lhcb_release_area)
        if (not self.platform):
            self.platform = self._get_user_platform()

        

    def master_configure(self):
        '''Configures the application'''

        self._checkInputs()
        self.appname = self.project
        self._setUpEnvironment()  

        job=self.getJobObject()
        from GangaLHCb.Lib.Gaudi import GaudiExtras
        self.extra = GaudiExtras()
        if job.inputdata:
            self.extra.inputdata = [x.name for x in job.inputdata.files]
        self.package = _available_packs[self.project]

        return (None,None)


    def configure(self,master_appconfig):
        job=self.getJobObject()
        self.dataopts = self._dataset2optionsstring(job.inputdata)
        return (None,None)


            
    def _checkInputs( self):
        # Go through the schema one by one and check if
        # we can guess the value
        # also normalise and expand filenames
        for fileitem in self.script:
            fileitem.name = os.path.expanduser(fileitem.name)
            fileitem.name = os.path.normpath(fileitem.name)

        if self.project is None:
            logger.error("The project is not set. Cannot configure")
            raise ApplicationConfigurationError(
                None, "The project is not set. Cannot configure")

        if self.project not in _available_apps:
            logger.error("Unknown application "+self.appname+
                         ". Cannot configure")
            raise ApplicationConfigurationError(
                None, "Unknown application "+self.appname+". Cannot configure")

        if self.version is None:
            self.version = self.guess_version(self.appname)
            logger.warning("The 'version' is not set. Setting it to "+
                           self.version+".")
            logger.warning("I hope this is OK.")

        if len(self.script)==0:
            logger.warning("No script defined. Will use a default script which is probably not what you want.")

            from Ganga.GPIDev.Lib.File import  File
            import inspect
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'GaudiPythonExample.py'))]

    
    def _dataset2optionsstring(self,ds):
        s=''
        if ds!=None:
            s='EventSelector.Input   = {'
            for k in ds.files:
                s+='\n'
                s+=""" "DATAFILE='%s' %s",""" % (k.name, ds.datatype_string)
            #Delete the last , to be compatible with the new optiosn parser
            if s.endswith(","):
                s=s[:-1]
        s+="""\n};"""
        return s



    def available_versions(self,appname, release_area, dev = None):
      """Try to extract the list of installed versions for a given application"""
      if appname not in _available_apps:
        raise ValueError, "Application " + appname + " not known"
      app_upper = appname.upper()
      app_upper_ = app_upper+"_"

      dirlist = os.listdir(release_area+os.sep+app_upper)
      versions = []
      for i in dirlist:
        if i.startswith(app_upper_):
          versions.append(i.replace(app_upper_, ""))
      versions.sort(self.versions_compare)
      return versions

       
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

    def _setUpEnvironment( self): 
        self.shell=env._setenv( self)

    def _get_user_platform(self,env=os.environ):
        if env.has_key('CMTCONFIG'):
            return env['CMTCONFIG']
        else:
            logger.info('"CMTCONFIG" not set. Cannot determine the platform you want to use')
            return ''

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaLHCb.Lib.Gaudi.GaudiPythonLSFRunTimeHandler import GaudiPythonLSFRunTimeHandler
from GangaLHCb.Lib.Dirac.GaudiPythonDiracRunTimeHandler import GaudiPythonDiracRunTimeHandler

allHandlers.add('GaudiPython', 'LSF', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Interactive', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'PBS', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'SGE', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Local', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', GaudiPythonDiracRunTimeHandler)
allHandlers.add('GaudiPython', 'Condor', GaudiPythonLSFRunTimeHandler)


