# Note that the special string AppName will be replaced upon initialisation
# in all cases with the relavent app name (DaVinci, Gauss etc...)
import os
import tempfile
import pprint
import sys
from GangaGaudi.Lib.Applications.Gaudi import Gaudi
from GangaGaudi.Lib.Applications.GaudiUtils import fillPackedSandbox, gzipFile
from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps, guess_version, available_packs
from GangaLHCb.Lib.Applications.AppsBaseUtils import backend_handlers, activeSummaryItems
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Schema import SimpleItem
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from Ganga.Utility.Shell import Shell
from GangaLHCb.Lib.Applications.PythonOptionsParser import PythonOptionsParser
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from Ganga.Utility.execute import execute
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.Core.exceptions import ApplicationConfigurationError
import Ganga.Utility.logging
import subprocess
import pickle
logger = Ganga.Utility.logging.getLogger()


class AppName(Gaudi):

    """The AppName Application handler

    The AppName application handler is for running LHCb GAUDI framework
    jobs. For its configuration it needs to know the version of the application
    and what options file to use. More detailed configuration options are
    described in the schema below.

    An example of submitting a AppName job to Dirac could be:

    app = AppName(version='v99r0')

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
    _name = 'AppName'
    _category = 'applications'
    _schema = Gaudi._schema.inherit_copy()
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    _schema.datadict['package'] = SimpleItem(defvalue=None,
                                             typelist=['str', 'type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read '  \
             'from. Can be written either as a path '  \
             '\"Tutorial/Analysis/v6r0\" or in a CMT style notation '  \
             '\"Analysis v6r0 Tutorial\"'
    _schema.datadict['masterpackage'] = SimpleItem(defvalue=None,
                                                   typelist=[
                                                       'str', 'type(None)'],
                                                   doc=docstr)
    docstr = 'Extra options to be passed onto the SetupProject command '\
             'used for configuring the environment. As an example '\
             'setting it to \'--dev\' will give access to the DEV area. '\
             'For full documentation of the available options see '\
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    _schema.datadict['setupProjectOptions'] = SimpleItem(defvalue='',
                                                         typelist=[
                                                             'str', 'type(None)'],
                                                         doc=docstr)

    _schema.version.major += 2
    _schema.version.minor += 0

    _exportmethods = Gaudi._exportmethods[:]
    _exportmethods += ['readInputData']

    def _get_default_version(self, gaudi_app):
        return guess_version(self, gaudi_app)

    def _auto__init__(self):
        self.appname = 'AppName'
        super(AppName, self)._auto__init__()

        if (not self.package):
            self.package = available_packs(self.appname)
        if self.appname is 'Vetra':
            self.lhcb_release_area = os.path.expandvars("$Vetra_release_area")

    def postprocess(self):
        from GangaLHCb.Lib.Applications import XMLPostProcessor
        XMLPostProcessor.postprocess(self, logger)

    def readInputData(self, optsfiles, extraopts=False):
        '''Returns a LHCbDataSet object from a list of options files. The
        optional argument extraopts will decide if the extraopts string inside
        the application is considered or not. 

        Usage examples:
        # Create an LHCbDataset object with the data found in the optionsfile
        l=DaVinci(version='v22r0p2').readInputData([\"~/cmtuser/\" \
        \"DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"]) 
        # Get the data from an options file and assign it to the jobs inputdata
        field
        j.inputdata = j.application.readInputData([\"~/cmtuser/\" \
        \"DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"])

        # Assuming you have data in your extraopts, you can use the extraopts.
        # In this case your extraopts need to be fully parseable by gaudirun.py
        # So you must make sure that you have the proper import statements.
        # e.g.
        from Gaudi.Configuration import * 
        # If you mix optionsfiles and extraopts, as usual extraopts may
        # overwright your options
        # 
        # Use this to create a new job with data from extraopts of an old job
        j=Job(inputdata=jobs[-1].application.readInputData([],True))
        '''
        def dummyfile():
            temp_fd, temp_filename = tempfile.mkstemp(text=True, suffix='.py')
            os.write(temp_fd, "Dummy file to keep the Optionsparser happy")
            os.close(temp_fd)
            return temp_filename

        if type(optsfiles) != type([]):
            optsfiles = [optsfiles]

        # use a dummy file to keep the parser happy
        if len(optsfiles) == 0:
            optsfiles.append(dummyfile())

        if extraopts:
            extraopts = self.extraopts
        else:
            extraopts = ""

        # parser = check_inputs(optsfiles, extraopts, self.env)
        try:
            parser = PythonOptionsParser(
                optsfiles, extraopts, self.getenv(False))
        except Exception as err:
            msg = 'Unable to parse the job options. Please check options ' \
                  'files and extraopts.'
            logger.error("PythonOptionsParserError:\n%s" % str(err))
            raise ApplicationConfigurationError(None, msg)

        return GPIProxyObjectFactory(parser.get_input_data())

    def getpack(self, options=''):
        """Performs a getpack on the package given within the environment
           of the application. The unix exit code is returned
        """
        import GangaLHCb.Lib.Applications.FileFunctions
        return FileFunctions.getpack(self, options)

    def make(self, argument=None):
        """Performs a make on the application. The unix exit code is 
           returned. Any arguments given are passed onto as in
           dv.make('clean').
        """
        import GangaLHCb.Lib.Applications.FileFunctions
        return FileFunctions.make(self, argument)

    def cmt(self, command):
        """Execute a cmt command in the cmt user area pointed to by the
        application. Will execute the command "cmt <command>" after the
        proper configuration. Do not include the word "cmt" yourself. The 
        unix exit code is returned."""
        if self.newStyleApp is True:
            logger.error("Cannot use this with cmake enabled!")
            return -1
        command = '###CMT### ' + command
        from GangaLHCb.Lib.Applications.CMTscript import CMTscript
        return CMTscript(self, command)

    def _getshell(self):
        import GangaLHCb.Lib.Applications.EnvironFunctions
        env = EnvironFunctions._getshell(self)
        return env

    def _get_parser(self):
        optsfiles = []
        for fileitem in self.optsfile:
            if type(fileitem) is str:
                optsfiles.append(fileitem)
            else:
                optsfiles.append(fileitem.name)
        #optsfiles = [fileitem.name for fileitem in self.optsfile]
        # add on XML summary

        extraopts = ''
        if self.extraopts:
            extraopts += self.extraopts
            if self.extraopts.find('LHCbApp().XMLSummary') is -1:
                extraopts += "\nfrom Gaudi.Configuration import *"
                extraopts += "\nfrom Configurables import LHCbApp"
                extraopts += "\nLHCbApp().XMLSummary='summary.xml'"
        else:
            extraopts += "\nfrom Gaudi.Configuration import *"
            extraopts += "\nfrom Configurables import LHCbApp"
            extraopts += "\nLHCbApp().XMLSummary='summary.xml'"

        try:
            parser = PythonOptionsParser(optsfiles, extraopts, self.getenv(False))
        except ApplicationConfigurationError as err:
            logger.error("PythonOptionsParserError:\n%s" % str(err))
            # fix this when preparing not attached to job

            msg2 = ''
            try:
                debug_dir = self.getJobObject().getDebugWorkspace().getPath()
                msg2 += 'You can also view this from within ganga '\
                    'by doing job.peek(\'../debug/gaudirun.<whatever>\').'
            except Exception, err:
                logger.debug("path Error:\n%s" % str(err))
                debug_dir = tempfile.mkdtemp()

            messages = err.message.split('###SPLIT###')
            if len(messages) is 2:
                stdout = open(debug_dir + '/gaudirun.stdout', 'w')
                stderr = open(debug_dir + '/gaudirun.stderr', 'w')
                stdout.write(messages[0])
                stderr.write(messages[1])
                stdout.close()
                stderr.close()
                msg = 'Unable to parse job options! Please check options ' \
                      'files and extraopts. The output and error streams from gaudirun.py can be ' \
                      'found in %s and %s respectively . ' % (stdout.name, stderr.name)
            else:
                f = open(debug_dir + '/gaudirun.out', 'w')
                f.write(err.message)
                f.close()
                msg = 'Unable to parse job options! Please check options ' \
                      'files and extraopts. The output from gaudirun.py can be ' \
                      'found in %s . ' % f.name
            msg += msg2
            logger.debug(msg)
            raise ApplicationConfigurationError(None, msg)
        return parser

    def _parse_options(self):
        try:
            parser = self._get_parser()
        except ApplicationConfigurationError as err:
            logger.debug("_get_parser Error:\n%s" % str(err))
            raise err

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                 'shared',
                                 getConfig('Configuration')['user'],
                                 self.is_prepared.name)
        # Need to remember to create the buffer as the perpare methods returns
        # are merely copied to the inputsandbox so must alread exist.
        #   share_path = os.path.join(share_dir,'inputsandbox')
        #   if not os.path.isdir(share_path): os.makedirs(share_path)

        fillPackedSandbox([FileBuffer('options.pkl', parser.opts_pkl_str)],
                          os.path.join(share_dir,
                                       'inputsandbox',
                                       '_input_sandbox_%s.tar' % self.is_prepared.name))
        # FileBuffer(os.path.join(share_path,'options.pkl'),
        # parser.opts_pkl_str).create()
        # self.prep_inputbox.append(File(os.path.join(share_dir,'options.pkl')))

        # Check in any input datasets defined in optsfiles and allow them to be
        # read into the
        inputdata = parser.get_input_data()
        if len(inputdata.files) > 0:
            logger.warning('Found inputdataset defined in optsfile, '
                           'this will get pickled up and stored in the '
                           'prepared state. Any change to the options/data will '
                           'therefore require an unprepare first.')
            logger.warning('NOTE: the prefered way of working '
                           'is to define inputdata in the job.inputdata field. ')
            logger.warning(
                'Data defined in job.inputdata will superseed optsfile data!')
            logger.warning('Inputdata can be transfered from optsfiles to the job.inputdata field '
                           'using job.inputdata = job.application.readInputData(optsfiles)')
            share_path = os.path.join(share_dir, 'inputdata')
            if not os.path.isdir(share_path):
                os.makedirs(share_path)
            f = open(os.path.join(share_path, 'options_data.pkl'), 'w+b')
            pickle.dump(inputdata, f)
            f.close()

        # store the outputsandbox/outputdata defined in the options file
        # Can remove this when no-longer need to define outputdata in optsfiles
        # Can remove the if job: when look into how to do prepare for standalone app
        # move into RuntimeHandler move whole parsing into options maybe?

        # try and get the job object
        # not present if preparing standalone app

        # must change this as prepare should be seperate from the jpb.inputdata


        share_path = os.path.join(share_dir, 'output')
        if not os.path.isdir(share_path):
            os.makedirs(share_path)
        f = open(os.path.join(share_path, 'options_parser.pkl'), 'w+b')
        pickle.dump(parser, f)
        f.close()

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
for (backend, handler) in backend_handlers().iteritems():
    allHandlers.add('AppName', backend, handler)

