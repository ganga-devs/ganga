#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Gaudi applications in LHCb.'''
import os
import tempfile
import gzip
import pickle
from Ganga.GPIDev.Schema import SimpleItem, FileItem
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.logging
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Lib.File import ShareDir
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.Utility.util import unique
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory, isType
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from GaudiBase import GaudiBase
from Ganga.Utility.files import fullpath
from GangaGaudi.Lib.Applications.GaudiUtils import gzipFile
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def GaudiDocString(appname):
    "Provide the documentation string for each of the Gaudi based applications"

    doc = """The Gaudi Application handler

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
    return doc.replace("Gaudi", appname)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Gaudi(GaudiBase):

    _name = 'Gaudi'
    __doc__ = GaudiDocString(_name)
    _category = 'applications'
    _exportmethods = GaudiBase._exportmethods[:]
    _exportmethods += ['prepare', 'unprepare']
    _hidden = 1
    _schema = GaudiBase._schema.inherit_copy()

    docstr = 'The gaudirun.py cli args that will be passed at run-time'
    _schema.datadict['args'] = SimpleItem(defvalue=['-T'], sequence=1, strict_sequence=0,
                                          typelist=['str', 'type(None)'], doc=docstr)
    docstr = 'The name of the optionsfile. Import statements in the file ' \
             'will be expanded at submission time and a full copy made'
    _schema.datadict['optsfile'] = FileItem(preparable=1, sequence=1, strict_sequence=0, defvalue=[],
                                            doc=docstr)
    docstr = 'A python configurable string that will be appended to the '  \
             'end of the options file. Can be multiline by using a '  \
             'notation like \nHistogramPersistencySvc().OutputFile = '  \
             '\"myPlots.root"\\nEventSelector().PrintFreq = 100\n or by '  \
             'using triple quotes around a multiline string.'
    _schema.datadict['extraopts'] = SimpleItem(preparable=1, defvalue=None,
                                               typelist=['str', 'type(None)'], doc=docstr)

    _schema.version.major += 0
    _schema.version.minor += 0

    def _auto__init__(self):
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        self._init()

    def _parse_options(self):
        raise NotImplementedError

    def prepare(self, force=False):

        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        from Ganga.GPIDev.Lib.File.File import File
        if isType(self.optsfile, (list, tuple, GangaList)):
            for this_file in self.optsfile:
                if type(this_file) is str:
                    this_file = File(this_file)
                else:
                    continue

        elif type(self.optsfile) is str:
            self.optsfile = [File(self.optsfile)]

        try:
            super(Gaudi, self).prepare(force)
        except Exception, err:
            logger.debug("Super Prepare Error:\n%s" % str(err))
            raise err

        logger.debug("Prepare")

        _is_prepared = self.is_prepared

        #logger.info("_is_prepared: %s" % _is_prepared)

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                 'shared',
                                 getConfig('Configuration')['user'],
                                 _is_prepared.name)

        # We will return a list of files 'send_to_share' which will be copied into the jobs
        # inputsandbox when prepare called from job object. NOTE that these files will not go
        # into an inputsandbox when prepare called on standalone app.
        # Things in the inputsandbox end up in the working dir at runtime.

        # Exception is just re-thrown here after setting is_prepared to None
        # could have done the setting in the actual functions but didnt want
        # prepared state altered from the readInputData pseudo-static member
        try:
            self._check_inputs()
        except Exception, err:
            logger.debug("_check_inputs Error:\n%s" % str(err))
            self.unprepare()
            raise err

        # write env into input dir and share dir
        share_path = os.path.join(share_dir, 'debug')
        if not os.path.isdir(share_path):
            os.makedirs(share_path)
        this_file = gzip.GzipFile(os.path.join(share_path, 'gaudi-env.py.gz'), 'wb')
        this_file.write('gaudi_env = %s' % str(self.getenv(True)))
        this_file.close()

        self._parse_options()
        #try:
        #    self._parse_options()
        #except Exception, err:
        #    logger.debug("_parse_options Error:\n%s" % str(err))
        #    self.unprepare()
        #    raise err

        gzipFile(os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tar' % self.is_prepared.name),
                 os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tgz' % self.is_prepared.name),
                 True)
        # add the newly created shared directory into the metadata system if
        # the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()  # create hash

    def _check_inputs(self):
        """Checks the validity of some of user's entries for Gaudi schema"""

        # Warn user that no optsfiles given
        if len(self.optsfile) == 0:
            logger.warning("The 'optsfile' is not set. I hope this is OK!")
        else:
            # Check for non-exting optsfiles defined
            nonexistentOptFiles = []
            for f in self.optsfile:
                from Ganga.GPIDev.Lib.File.File import File
                if type(f) is str:
                    myFile = File(f)
                else:
                    myFile = f
                myFile.name = fullpath(myFile.name)
                if not os.path.isfile(myFile.name):
                    nonexistentOptFiles.append(myFile)

            if len(nonexistentOptFiles):
                tmpmsg = "The 'optsfile' attribute contains non-existent file/s: ["
                for _f in nonexistentOptFiles:
                    tmpmsg += "'%s', " % _f.name
                msg = tmpmsg[:-2] + ']'
                raise ApplicationConfigurationError(None, msg)

    def master_configure(self):
        '''Handles all common master_configure actions.'''
        return (None, StandardJobConfig())

    def configure(self, appmasterconfig):
        return (None, StandardJobConfig())


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
