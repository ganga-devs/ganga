#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Gaudi applications in LHCb.'''
import os
import tempfile
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.logging
from GaudiUtils import *
from GaudiRunTimeHandler import * 
from PythonOptionsParser import PythonOptionsParser
from Francesc import *
from Ganga.Utility.util import unique

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

class Gaudi(Francesc):
    
    _name = 'Gaudi'
    __doc__ = GaudiDocString(_name)
    _category = 'applications'
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'readInputData']

    schema = get_common_gaudi_schema()
    docstr = 'The name of the optionsfile. Import statements in the file ' \
             'will be expanded at submission time and a full copy made'
    schema['optsfile'] =  FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                   doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['appname'] = SimpleItem(defvalue=None,typelist=['str','type(None)'],
                                   hidden=1,doc=docstr)
    schema['configured'] = SimpleItem(defvalue=None,hidden=0,copyable=0,
                                      typelist=['str','type(None)']) 
    docstr = 'A python configurable string that will be appended to the '  \
             'end of the options file. Can be multiline by using a '  \
             'notation like \nHistogramPersistencySvc().OutputFile = '  \
             '\"myPlots.root"\\nEventSelector().PrintFreq = 100\n or by '  \
             'using triple quotes around a multiline string.'
    schema['extraopts'] = SimpleItem(defvalue=None,
                                     typelist=['str','type(None)'],doc=docstr) 
    _schema = Schema(Version(2, 1), schema)

    def _auto__init__(self):
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        if not self.appname: return 
        self._init(self.appname,True)
            
    def master_configure(self):

        job = self.getJobObject()
        self._master_configure()
        inputs = self._check_inputs()         
        optsfiles = [fileitem.name for fileitem in self.optsfile]
        try:
            parser = PythonOptionsParser(optsfiles,self.extraopts,self.shell)
        except ApplicationConfigurationError, e:
            debug_dir = job.getDebugWorkspace().getPath()
            f = open(debug_dir + '/gaudirun.stdout','w')
            f.write(e.message)
            f.close()
            msg = 'Unable to parse job options! Please check options ' \
                  'files and extraopts. The output from gaudyrun.py can be ' \
                  'found in %s. You can also view this from within ganga '\
                  'by doing job.peek(\'../debug/gaudirun.stdout\').' % f.name
            #logger.error(msg)
            raise ApplicationConfigurationError(None,msg)

        self.extra.master_input_buffers['options.pkl'] = parser.opts_pkl_str
        inputdata = parser.get_input_data()
  
        # If user specified a dataset, ignore optsfile data but warn the user.
        if len(inputdata.files) > 0:
            if job.inputdata:
                msg = 'A dataset was specified for this job but one was ' \
                      'also defined in the options file. Data in the options '\
                      'file will be ignored...hopefully this is OK.' 
                logger.warning(msg)            
            else:
                logger.info('Using the inputdata defined in the options file.')
                self.extra.inputdata = inputdata
        
        self.extra.outputsandbox,outputdata = parser.get_output(job)
        outputdata = self.extra.outputdata + outputdata
        self.extra.outputdata = unique(outputdata)
        
        return (inputs, self.extra) # return (changed, extra)

    def configure(self,master_appconfig):
        self._configure()
        return (None,self.extra)
            
    def _check_inputs(self):
        """Checks the validity of some of user's entries for Gaudi schema"""

        self._check_gaudi_inputs(self.optsfile,self.appname)        
        if self.package is None:
            msg = "The 'package' attribute must be set for application. "
            raise ApplicationConfigurationError(None,msg)

        inputs = None
        if len(self.optsfile)==0:
            logger.warning("The 'optsfile' is not set. I hope this is OK!")
            packagedir = self.shell.env[self.appname.upper()+'ROOT']
            opts = os.path.expandvars(os.path.join(packagedir,'options',
                                                   self.appname + '.py'))
            if opts: self.optsfile.append(opts)
            else:
                logger.error('Cannot find the default opts file for ' % \
                             self.appname + os.sep + self.version)
            inputs = ['optsfile']
            
        return inputs

    def readInputData(self,optsfiles,extraopts=False):
        """Returns a LHCbDataSet object from a list of options files. The
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
        """
        
        def dummyfile():
            temp_fd,temp_filename=tempfile.mkstemp(text=True,suffix='.py')
            os.write(temp_fd,"#Dummy file to keep the Optionsparser happy")
            os.close(temp_fd)
            return temp_filename

        if type(optsfiles)!=type([]): optsfiles=[optsfiles]

        # use a dummy file to keep the parser happy
        if len(optsfiles)==0: optsfiles.append(dummyfile())

        self._getshell()
        inputs = self._check_inputs() 
        if extraopts: extraopts=self.extraopts
        else: extraopts=""
            
        try:
            parser = PythonOptionsParser(optsfiles,extraopts,self.shell)
        except Exception, e:
            msg = 'Unable to parse the job options. Please check options ' \
                  'files and extraopts.'
            raise ApplicationConfigurationError(None,msg)

        return parser.get_input_data()
   
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Individual Gaudi applications. These are thin wrappers around the Gaudi base 
# class. The appname property is read protected and it tries to guess all the
# properties except the optsfile.

myschema = Gaudi._schema.inherit_copy()
myschema['appname']._meta['protected'] = 1

# getpack,... methods added b/c of bug in exportmethods dealing w/ grandchild
class_str = """
class ###CLASS###(Gaudi):
    _name = '###CLASS###'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'readInputData']

    def __init__(self):
        super(###CLASS###, self).__init__()
        self.appname = '###CLASS###'
        ###SETLHCBRA###

    def getenv(self,options=''):
        return super(###CLASS###,self).getenv()
        
    def getpack(self,options=''):
        return super(###CLASS###,self).getpack(options)

    def make(self,argument=''):
        return super(###CLASS###,self).make(argument)

    def cmt(self,command):
        return super(###CLASS###,self).cmt(command)

    for method in ['getenv','getpack','make','cmt']:
        setattr(eval(method), \"__doc__\", getattr(Gaudi, method).__doc__)

"""

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for app in available_apps():
    exec_str = class_str.replace('###CLASS###', app)
    if app is 'Vetra':
        lhcbra = os.path.expandvars("$Vetra_release_area")
        exec_str = exec_str.replace('###SETLHCBRA###',
                                    'self.lhcb_release_area = "%s"' % lhcbra)
    else:
        exec_str = exec_str.replace('###SETLHCBRA###', '')
    if app is not 'Gaudi':
        exec(exec_str)

    for backend in ['LSF','Interactive','PBS','SGE','Local','Condor']:
        allHandlers.add(app, backend, GaudiRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

