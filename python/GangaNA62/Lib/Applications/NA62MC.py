from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import *
#from Ganga.GPIDev.Lib.File import File
#from Ganga.GPIDev.Lib.File import SharedDir
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core import ApplicationConfigurationError

from Ganga.GPIDev.Lib.File.SandboxFile import SandboxFile
from commands import getstatusoutput

import os, shutil
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class NA62MC(IPrepareApp):
    """
    NA62MC application -- running arbitrary programs.
    """
    _schema = Schema(Version(2,0), {
        'run_number' : SimpleItem(defvalue=-1,typelist=['int'],doc="Run Number to pass to the scripts"),
        'seed'       : SimpleItem(defvalue=-1,typelist=['int'],doc="Random seed to pass to the scripts - overwritten by run_number if not given"),
        'num_events' : SimpleItem(defvalue=-1,typelist=['int'],doc="Number of events to generate"),
        'decay_type' : SimpleItem(defvalue=-1,typelist=['int'],doc="Decay type to generate"),
        'decay_name' : SimpleItem(defvalue="",typelist=['str'],doc="String of the Decay type"),
        'mc_version' : SimpleItem(defvalue=8,typelist=['int'],doc="MC version to use"),
        'revision'   : SimpleItem(defvalue=0,typelist=['int'],doc="MC revision to use"),
        'script_name': SimpleItem(defvalue="",typelist=['str'],doc="The name of the script to download and use on submission"),
        'radcor'     : SimpleItem(defvalue=False,typelist=['bool'],doc="Use radiatve corrections or not"),
        'file_prefix': SimpleItem(defvalue='pluto',typelist=['str'],doc="Prefix for output file"),
        'job_type'   : SimpleItem(defvalue='prod',typelist=['str'],doc="Job type (e.g. prod or test)"),
        'is_prepared': SharedItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)', 'bool', 'str'],protected=0,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=1, doc='MD5 hash of the string representation of applications preparable attributes')
        } )
    _category = 'applications'
    _name = 'NA62MC'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(NA62MC,self).__init__()

    def getMACFileName(self):
        "return the mac filename"
        return "Run%d_d%d.mac" % (self.run_number, self.decay_type)

    def getDecayString(self):
        "return the decay sring from the DB"
        return self.decay_name

    def getRevision(self):
        "return the revision from the DB"
        return self.revision

    def getNextRunNumber(self):
        "return the next available run number from the DB"
        nec_file = ".gpytho"
        work_dir = "/clusterhome/home/protopop"
        nec_str = open(os.path.join( work_dir, nec_file )).read().strip().strip('#')
        mysqlc = "mysql -hhughnon.ppe.gla.ac.uk -ugridbot -p%s -s GridJobs" % nec_str
        rc, out = getstatusoutput("echo \"SELECT run FROM jobs ORDER BY run DESC LIMIT 1;\" | %s" % mysqlc)
        if rc != 0:
            logger.error(out)
        run_num = int(out) + 1
            
        return run_num
    
    def unprepare(self, force=False):
        """
        Revert the application back to it's unprepared state.
        """
        logger.debug('Running unprepare in NA62MC app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self,force=False):
        """
        A method to place the NA62MC application into a prepared state.

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

        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        # sanity check values
        if (self.run_number == -1):
            raise Exception('Run number has not been set')

        if (self.decay_type == -1):
            raise Exception('decay type has not been set')
        
        if (self.num_events < 1):
            raise Exception('no events set to generate')

        if (self.script_name == ""):
          raise Exception('please provide the script to run')

        #lets use the same criteria as the configure() method for checking file existence & sanity
        #this will bail us out of prepare if there's somthing odd with the job config - like the executable
        #file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s'%(self.is_prepared.name))

        # download and store the WN script to run
        scr_path = os.path.join(shared_path,self.is_prepared.name, self.script_name)
        os.system("wget -q http://na62.gla.ac.uk/scripts/jw1v8.sh.txt -O %s" % scr_path)
        if not os.path.exists(scr_path):
            raise Exception('Could not download worker node script')

        # download and store the input files
        inp_path = os.path.join(shared_path,self.is_prepared.name, "input_files.tgz")
        os.system("wget -q http://na62.gla.ac.uk/input/input_files.tgz -O %s" % inp_path)
        if not os.path.exists(inp_path):
            raise Exception('Could not download the input files tarball')            

        # create the mac file and add to the shared area
        mac_out = ""
        for ln in open( os.path.join( os.path.dirname(__file__), "MAC_template.txt")).readlines():
            
            if ln.find("/output/fileName") != -1:
                # replace filename            
                mac_out += ("/output/fileName %s_v%d_r%d.root\n" % (self.file_prefix, self.mc_version, self.run_number))
            elif ln.find("/decay/type") != -1:
                # replace decay type            
                mac_out += ("/decay/type %d\n" % (self.decay_type))
            elif ln.find("/decay/radcor") != -1:
                # replace rad cor
                if (self.radcor):
                    mac_out += ("/decay/radcor 1\n")
                else:
                    mac_out += ("/decay/radcor 0\n")
            elif ln.find("/random/seedDecay") != -1:
                # replace seed decay
                seed = self.seed
                if (seed == -1):
                    seed = self.run_number
                    
                mac_out += ("/random/seedDecay %d\n" % (seed))
            elif ln.find("/run/number") != -1:
                # replace run number
                mac_out += ("/run/number %d\n" % (self.run_number))
            elif ln.find("/run/beamOn") != -1:
                # replace number of events
                mac_out += ("/run/beamOn %d\n" % (self.num_events))                
            else:
                mac_out += ln

        open(os.path.join(shared_path,self.is_prepared.name, self.getMACFileName()), "w").write(mac_out)

        #copy any 'preparable' objects into the shared directory
        send_to_sharedir = self.copyPreparables()
        #add the newly created shared directory into the metadata system if the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        #return [os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir))]
        self.post_prepare()
        return 1


    def configure(self,masterappconfig):
        from Ganga.Core import ApplicationConfigurationError
        import os.path
        
        return (None,None)


class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        #prepared_exe = File(os.path.join(shared_path,self.is_prepared.name, self.script_name))
        prepared_exe = File(os.path.join( os.path.dirname(__file__), "runNA62MC.sh"))
        #if app.is_prepared is not None:
        #    logger.info("Submitting a prepared application; taking any input files from %s" %(app.is_prepared.name))
        #    prepared_exe = File(os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(prepared_exe.name)))

        inputbox = [ File(os.path.join(os.path.join(shared_path,app.is_prepared.name), app.getMACFileName())) ]
        inputbox.append( File(os.path.join(shared_path,app.is_prepared.name, app.script_name)) )
        inputbox.append( File(os.path.join(shared_path,app.is_prepared.name, "input_files.tgz")) )
        inputbox += app._getParent().inputsandbox

        #outputbox = [ 'na62run%d.stderr.err' % app.run_number, 'na62run%d.stdout.out' % app.run_number ]
        outputbox = []
        outputbox += app._getParent().outputsandbox

        env = {'NA62SCRIPT':app.script_name,
               'NA62STDOUT':'na62run%d.out' % app.run_number,
               'NA62STDERR':'na62run%d.err' % app.run_number}
        
        args = [app.getMACFileName(), "%s/r%d/%s" % (app.job_type, app.getRevision(), app.getDecayString())]
        c = StandardJobConfig(prepared_exe,inputbox,args,outputbox,env)
        return c
        

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        prepared_exe = File(os.path.join( os.path.dirname(__file__), "runNA62MC.sh"))
        
        #if app.is_prepared is not None:
        #    logger.info("Submitting a prepared application; taking any input files from %s" %(app.is_prepared.name))
        #    prepared_exe = File(os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(prepared_exe.name)))

        #outputbox = [ 'na62run%d.stderr.err' % app.run_number, 'na62run%d.stdout.out' % app.run_number ]
        outputbox = []
        outputbox += app._getParent().outputsandbox
        
        inputbox = [ File(os.path.join(os.path.join(shared_path,app.is_prepared.name), app.getMACFileName())) ]
        inputbox.append( File(os.path.join(shared_path,app.is_prepared.name, app.script_name)) )
        inputbox.append( File(os.path.join(shared_path,app.is_prepared.name, "input_files.tgz")) )
        inputbox += app._getParent().inputsandbox
        
        env = {'NA62SCRIPT':app.script_name,
               'NA62STDOUT':'na62run%d.out' % app.run_number,
               'NA62STDERR':'na62run%d.err' % app.run_number}
        args = [app.getMACFileName(), "%s/r%d/%s" % (app.job_type, app.getRevision(), app.getDecayString())]

        # add the output files
        app._getParent().outputfiles = [SandboxFile('na62run%d.out' % app.run_number), SandboxFile('na62run%d.err' % app.run_number),
                                        SandboxFile('__jdlfile__'), SandboxFile(app.getMACFileName()) ]
        
        return LCGJobConfig(prepared_exe,inputbox,args,outputbox,env)

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('NA62MC','LSF', RTHandler)
allHandlers.add('NA62MC','Local', RTHandler)
allHandlers.add('NA62MC','PBS', RTHandler)
allHandlers.add('NA62MC','SGE', RTHandler)
allHandlers.add('NA62MC','Condor', RTHandler)
allHandlers.add('NA62MC','LCG', LCGRTHandler)
allHandlers.add('NA62MC','TestSubmitter', RTHandler)
allHandlers.add('NA62MC','Interactive', RTHandler)
allHandlers.add('NA62MC','Batch', RTHandler)
allHandlers.add('NA62MC','Cronus', RTHandler)
allHandlers.add('NA62MC','Remote', LCGRTHandler)
allHandlers.add('NA62MC','CREAM', LCGRTHandler)

