#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Gaudi applications in LHCb.'''
import os
import tempfile
import gzip
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.logging
from GaudiUtils import *
from GaudiRunTimeHandler import * 
from PythonOptionsParser import PythonOptionsParser
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Lib.File import ShareDir
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Francesc import *
from Ganga.Utility.util import unique
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

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
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'readInputData','prepare']

    schema = get_common_gaudi_schema()
    docstr = 'The gaudirun.py cli args that will be passed at run-time'
    schema['args'] =  SimpleItem(sequence=1,strict_sequence=0,defvalue=[],
                                 typelist=['str','type(None)'],doc=docstr)
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
    docstr = 'Location of shared resources. Presence of this attribute implies'\
          'the application has been prepared.'
    schema['is_prepared'] = SimpleItem(defvalue=None,
                                       strict_sequence=0,
                                       visitable=1,
                                       copyable=1,
                                       typelist=['type(None)','str'],
                                       protected=1,
                                       doc=docstr)
    _schema = Schema(Version(2, 1), schema)

    def _auto__init__(self):
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        if not self.appname: return 
        self._init(self.appname,True)

##     def _get_parser(self):
##         import pickle

##         if self.is_prepared is None:
##             msg = "Application should be prepared but isn\'t"
##             raise ApplicationConfigurationError(None,msg)   

##         parser_file = open(os.path.join(self.is_prepared.name,'parser.pkl'),'rb')

##         if not os.path.isfile(parser_file.name):
##             msg = 'Unable to find pickled options parser.'
##             raise ApplicationConfigurationError(None,msg)
##         parser = pickle.load(parser_file)

##         return parser


    def _get_parser(self):
        optsfiles = [fileitem.name for fileitem in self.optsfile]
        try:
            parser = PythonOptionsParser(optsfiles,self.extraopts,self.shell)
        except ApplicationConfigurationError, e:
            debug_dir = self.getJobObject().getDebugWorkspace().getPath()
            f = open(debug_dir + '/gaudirun.stdout','w')
            f.write(e.message)
            f.close()
            msg = 'Unable to parse job options! Please check options ' \
                  'files and extraopts. The output from gaudyrun.py can be ' \
                  'found in %s. You can also view this from within ganga '\
                  'by doing job.peek(\'../debug/gaudirun.stdout\').' % f.name
            #logger.error(msg)
            raise ApplicationConfigurationError(None,msg)
        return parser

    def unprepare(self,force=False):
        """
        Revert an Executable() application back to it's unprepared state.
        """
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None

    def prepare(self,force=False):
        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        #shared_dirname = self.is_prepared.name
        self.incrementShareCounter(self.is_prepared.name)

        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        
        send_to_share = self._prepare()
        job = self.getJobObject()
        parser = self._check_inputs()
        ## Next line would have stored them in the jobs inputdir but want them
        ## in sharedDir now we have prepared state.
        ##input_dir = job.getInputWorkspace().getPath()
        input_dir = self.is_prepared.name#shared_dirname


        ## Need to remember to create the buffer as the perpare methods returns
        ## are merely copied to the inputsandbox so must alread exist.
        send_to_share.append(FileBuffer(os.path.join(input_dir,'options.pkl'),
                                        parser.opts_pkl_str).create())

        ## write env into input dir and share dir
        file = gzip.GzipFile(os.path.join(input_dir,'gaudi-env.py.gz'),'wb')
        file.write('gaudi_env = %s' % str(self.shell.env))
        file.close()
        send_to_share.append(File(os.path.join(input_dir,'gaudi-env.py.gz')))

        ## store the outputsandbox/outputdata defined in the options file
        ## Can remove this when no-longer need to define outputdata in optsfiles
        outputsandbox, outputdata = parser.get_output(job)
        import pickle
        ## sandbox
        file = open(os.path.join(input_dir,'outputsandbox.pkl'),'wb')
        pickle.dump(outputsandbox,file)
        file.close()
        send_to_share.append(File(os.path.join(input_dir,'outputsandbox.pkl')))
        ## data
        file = open(os.path.join(input_dir,'outputdata.pkl'),'wb')
        pickle.dump(outputdata,file)
        file.close()
        send_to_share.append(File(os.path.join(input_dir,'outputdata.pkl')))

        #add the newly created shared directory into the metadata system if the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        return [fb.name for fb in send_to_share]
    
    def master_configure(self):
        #self._master_configure()
        #appmasterconfig = GaudiAppConfig()

#        parser = self._get_parser()

  ##       job = self.getJobObject()
##         if job.inputdata: self.appconfig.inputdata = job.inputdata
##         if job.outputdata: self.appconfig.outputdata = job.outputdata

 #       self.appconfig.outputsandbox,outputdata = parser.get_output(job)
##         self.appconfig.outputdata.files += outputdata
##         self.appconfig.outputdata.files = unique(self.appconfig.outputdata.files)
        #self.appconfig = appmasterconfig

        
        return (None, StandardJobConfig()) # return (changed, extra)

    def configure(self, appmasterconfig):
        #self._configure()
        #appsubconfig = GaudiAppConfig()

        #data_str = self.appconfig.inputdata.optionsString()



##         # pick up the data.py contents from the splitter
##         name_list = [fb.name for fb in self.appconfig.inputsandbox]
##         data_count = name_list.count('data.py')         
##         if data_count == 1:
##             item = name_list.index('data.py')
##             full_data = self.appconfig.inputsandbox[item].getContents()
##             full_data += data_str
##             appsubconfig.inputsandbox.append(FileBuffer('data.py',full_data).create())
##         elif not data_count:
##             appsubconfig.inputsandbox.append(FileBuffer('data.py',data_str).create())
##         else:
##             msg = 'ERROR: more than one data.py file defined.'
##             logger.error('You should not be seeing this, if you are then it is a bug!')
##             raise ApplicationConfigurationError(None,msg)


        return (None, StandardJobConfig())

    def _check_inputs(self):
        """Checks the validity of some of user's entries for Gaudi schema"""
        self._check_gaudi_inputs(self.optsfile,self.appname)        
        if self.package is None:
            msg = "The 'package' attribute must be set for application. "
            raise ApplicationConfigurationError(None,msg)

        inputs = None

        ## Warn user that no optsfiles given
        if len(self.optsfile)==0:
            logger.warning("The 'optsfile' is not set. I hope this is OK!")

        ## Check for non-exting optsfiles defined
        nonexistentOptFiles = []
        for f in self.optsfile:
            if not os.path.isfile(f.name):
                nonexistentOptFiles.append(f)
        
        if len(nonexistentOptFiles):
            tmpmsg = "The 'optsfile' attribute contains non-existent file/s: ["
            for f in nonexistentOptFiles:
                tmpmsg+="'%s', " % f.name
            msg=tmpmsg[:-2]+']'
            raise ApplicationConfigurationError(None,msg)


        ## Check in any datasets defined in optsfiles
        parser = self._get_parser()
        inputdata = parser.get_input_data()
        if len(inputdata.files) > 0:
            msg='Found inputdataset defined in optsfiles,'\
                ' This is no longer allowed, please use the'\
                'inputdata attribute of job.inputdata'
            raise ApplicationConfigurationError(None,msg)

        return parser

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

        return GPIProxyObjectFactory(parser.get_input_data())
   
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

    for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
        allHandlers.add(app, backend, GaudiRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

