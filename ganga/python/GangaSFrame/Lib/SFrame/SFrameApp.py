################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameApp.py,v 1.3 2009-01-19 10:10:18 mbarison Exp $
################################################################################
import os, socket, pwd, commands, re, string
from xml.dom.minidom import Node

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import *
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IMerger import IMerger
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *


from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE
from GangaAtlas.Lib.ATLASDataset import ATLASCastorDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaAtlas.Lib.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset import filecheck

from Ganga.Lib.LCG import LCGRequirements, LCGJobConfig

from Ganga.Utility.Config import makeConfig, getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Credentials import GridProxy

from commands import getstatusoutput    
import threading

# def my_subjobstatus(j):
#     for i in j.subjobs:
#         print "%s:\t%s" % (`i`, i.status)

#     return

### sets only appear in Python 2.4+
if not 'set' in dir(__builtins__):
    import sets
    set = sets.Set    

def mktemp(extension,name,path):
    """Create a unique file"""
         
    pattern=os.path.join(path,name+"-%05d"+extension)
    i=1
    while i<99999:
        filename=pattern % i
        if not os.path.exists(filename):
            return filename
        i+=1
    return None


def sframe_find( arg, dirname, fnames ):
    ''' Function for finding the main SFrame package. '''

    logger.debug("dirname: %s fnames: %s" % (dirname, fnames))

    # Remove CVS and svn directories from the search path:
    for i in range( fnames.count( "CVS" ) ):
        fnames.remove( "CVS" )

    for i in range( fnames.count( ".svn" ) ):
        fnames.remove( ".svn" )   


    logger.debug("Basename: %s Found: %s" % (os.path.basename( dirname ), "SFrame" in os.path.basename( dirname )))

    if "SFrame" in os.path.basename( dirname ):
        for fname in fnames:
            if fname == "Makefile":
                fl = os.path.join(dirname, fname)
                if not os.path.islink(fl):
                    sframe_dir.append( dirname )


def package_find( arg, dirname, fnames ):
    ''' Function for finding additional SFrame packages. '''

    logger.debug("dirname: %s fnames: %s" % (dirname, fnames))

    # Remove CVS and svn directories from the search path:
    for i in range( fnames.count( "CVS" ) ):
        fnames.remove( "CVS" )

    for i in range( fnames.count( ".svn" ) ):
        fnames.remove( ".svn" )

    # Remove the main SFrame source from the search path:
    for i in range( fnames.count( "SFrame" ) ):
        fnames.remove( "SFrame" )

    # If it has a Makefile in it, it has to be a "package":
    for fname in fnames:
        if fname == "Makefile" and dirname not in sframe_dir:
            fl = os.path.join(dirname, fname)
            if not os.path.islink(fl):
                package_list.append( dirname )
                logger.debug("Added %s to package list" % dirname)
 

class SFrameApp(IApplication):
    """
    SFrameApp application -- running SFrame analyses on GRID.
    
    When you want to run on a worker node you should specify
    the directory where the SFrame binaries are located.
    Ganga will create a tarball and ship it in the sandbox:
       app.sframe_dir = File('/path/to/sframe/binaries')

    You can also instruct ganga to use a ready-made archive
    with the SFrame binaries:
       app.sframe_archive = File('/path/to/sframe/tarball')

    It's highly recommended that you compile and link your SFrame binaries
    against one of the official ROOT versions contained in the ATLAS releases.
    You must instruct ganga to use the same release by selecting it:
       app.atlas_release = '13.0.10'

    SFrame needs an XML options file to run, select it here:
       app.xml_options = File('/path/to/options/file')   
    """

    _schema = Schema(Version(2,0), {
        'xml_options' : FileItem(doc='A XML File specifying the job options.'), 
        'sframe_dir'  : FileItem(defvalue=File(os.environ['HOME']+'/SFrame'),doc="The directory containing the SFrame lib/ and bin/ directories"),
        'sframe_archive' : FileItem(doc='A tar file of the SFrame libraries'),
        'atlas_release' : SimpleItem(defvalue='14.2.20',doc='The ATLAS sw release version defines which ROOT version is used'),
        'env' : SimpleItem(defvalue={},doc='Environment'),
        'exclude_list' : SimpleItem(defvalue=[ "CVS", "obj", "*~", "*.root",
                             "*.ps", "*.so", "*.d",
                             "*.rootmap", "*.pyc",
                             "*._Dict.*", "*.o", "python" ], doc='Patterns to be excluded from the compiled archive'),
        'user_email' : SimpleItem(defvalue = '', doc='email for job status notifications'),
        } )
    _category = 'applications'
    _name = 'SFrameApp'
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'xml_options', 'widget' : 'File' },
                  { 'attribute' : 'sframe_dir', 'widget' : 'FileOrString' },
                  { 'attribute' : 'sframe_archive', 'widget' : 'FileOrString' },
                  { 'attribute' : 'atlas_release', 'widget' : 'DictOfString' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' },
                  { 'attribute' : 'user_email', 'widget' : 'String' },
                  ]
    

    _GUIAdvancedPrefs = [ { 'attribute' : 'xml_options', 'widget' : 'File' },
                          { 'attribute' : 'sframe_dir', 'widget' : 'FileOrString' },
                          { 'attribute' : 'sframe_archive', 'widget' : 'FileOrString' },
                          { 'attribute' : 'atlas_release', 'widget' : 'DictOfString' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' },]

    def __init__(self):
        super(SFrameApp,self).__init__()
        
    def configure(self,masterappconfig):
        from Ganga.Core import ApplicationConfigurationError
        import os.path
        
        # do the validation of input attributes, with additional checks for exe property
        from Ganga.Lib.LCG import LCG
        
#        if type(self._getParent().backend) != type(LCG()):
#            raise ApplicationConfigurationError(None,'SFrameApp supports only the LCG backend')
        

        def validate_argument(x,tp=None):
            if type(x) is type(''):
                if tp == 'EXE':
                    if not x:
                        raise ApplicationConfigurationError(None,'exe not specified')
                        
                    if len(x.split())>1:
                        raise ApplicationConfigurationError(None,'exe "%s" contains white spaces'%x)

                    dirn,filen = os.path.split(x)
                    if not filen:
                        raise ApplicationConfigurationError(None,'exe "%s" is a directory'%x)
                    if dirn and not os.path.isabs(dirn):
                        raise ApplicationConfigurationError(None,'exe "%s" is a relative path'%x)

                elif tp == "REL":
                    if not x:
                        raise ApplicationConfigurationError(None,'ATLAS sw release not specified')

            else:
              try:
                  if tp == 'DIR':
                      if not os.path.isdir(x.name):
                          raise ApplicationConfigurationError(None,'%s: directory not found'%x.name)
                  else:
                      if not x.exists():
                          raise ApplicationConfigurationError(None,'%s: file not found'%x.name)
              except AttributeError:
                  raise ApplicationConfigurationError(None,'%s (%s): unsupported type, must be a string or File'%(str(x),str(type(x))))

        validate_argument(self.sframe_dir,'DIR')       
        validate_argument(self.sframe_archive)
        validate_argument(self.xml_options)
        validate_argument(self.atlas_release, 'REL')

        #for a in self.args:
        #    validate_argument(a)
        
        return (None,None)


    def prepare(self, remote_compile = True):
        from Ganga.Core import ApplicationConfigurationError
        
        if remote_compile:
            global package_list, sframe_dir

            package_list = []
            sframe_dir = []

            self.env['SFRAME_COMPILE'] = '1'
            
            from Ganga.Core import FileWorkspace
            ws=FileWorkspace.FileWorkspace(FileWorkspace.gettop(),subpath='file')
            ws.create(None)
            self.sframe_archive.name=mktemp('.tar.gz',"sframe",ws.getPath())

            #
            # Find the SFrame sources:
            #
            logger.info("Searching for the SFrame sources:")
            self.sframe_dir.name = self.sframe_dir.name.rstrip("/")
            os.path.walk( self.sframe_dir.name, sframe_find, None )
            if len( sframe_dir ) < 1:
                logger.error("No SFrame sources found!")
                raise ApplicationConfigurationError(None,"No valid SFrame src directory given!")
                
               
            elif len( sframe_dir ) > 1:
                logger.error("Multiple SFrame sources!")
                for i in sframe_dir:
                    logger.error("   dir: %s" % i)
                raise ApplicationConfigurationError(None,"No valid SFrame directory given!")


            logger.info("SFrame source found under: %s" % sframe_dir[ 0 ])

            #
            # Find any additional packages:
            #
            #logger.info("Searching for additional packages:")
            #os.path.walk( self.sframe_dir.name, package_find, None )
            #if not len( package_list ):
            #    logger.info("No additional packages found")
            #else:
            #    logger.info("Additional packages found under: %s" % string.join( package_list, ', ' ))

            #
            # We don't want absolute filenames, stripping topdir
            #

            logger.debug("%s %s" % (sframe_dir[0], self.sframe_dir.name))

            #sframe_dir =[sframe_dir[0].lstrip(self.sframe_dir.name).lstrip("/")]
            if sframe_dir[0] == "":
                sframe_dir = ["./"]
                
            tmp_list = []

            for i in package_list:
                if "dev" not in i:
                    tmp_list.append(i.lstrip(self.sframe_dir.name).lstrip("/"))

            package_list = tmp_list

            #
            # Set up the list of things to exclude from the archive:
            #
            exclude_string = ""

            for i in self.exclude_list:
                exclude_string += " --exclude=\"%s\"" % i

            #
            # Create the archive with all the sources:
            #
            logger.info("Creating archive:")
            
            savedir=os.getcwd()
            os.chdir(self.sframe_dir.name)
            archive_command = "tar -czf " + self.sframe_archive.name + " " + sframe_dir[ 0 ] + " " + string.join( package_list, ' ' ) + exclude_string
            logger.info("   %s" % archive_command)
            os.system( archive_command )
            
            os.chdir(savedir)
            
        else:
            if self.env.has_key('SFRAME_COMPILE'):   
                self.env.pop('SFRAME_COMPILE')
            
            if self.sframe_dir.name:       

                from Ganga.Core import FileWorkspace
                ws=FileWorkspace.FileWorkspace(FileWorkspace.gettop(),subpath='file')
                ws.create(None)
                self.sframe_archive.name=mktemp('.tar.gz',"sframe",ws.getPath())
                logger.info('Creating SFrame archive:\n %s ...',self.sframe_archive.name)
                logger.info('From %s', self.sframe_dir.name)

                savedir=os.getcwd()

                os.chdir(self.sframe_dir.name)

                os.system("tar -czhf %s bin lib user/config/JobConfig.dtd --exclude CVS 2>/dev/null" % self.sframe_archive.name)

                os.chdir(savedir)

            else:
                logger.error("No valid SFrame directory given!")
                raise ApplicationConfigurationError(None,"No valid SFrame directory given!")
                

class SFrameAppSplitterJob(ISplitter):
    """SFrameApp handler for job splitting"""
    
    _name = "SFrameAppSplitterJob"
    _schema = Schema(Version(1,0), {
        'numsubjobs': SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs"),
        'match_subjobs_files': SimpleItem(defvalue=False,sequence=0, doc="Match the number of subjobs to the number of inputfiles")
        } )

    _GUIPrefs = [ { 'attribute' : 'numsubjobs',  'widget' : 'Int' },
                  { 'attribute' : 'match_subjobs_files',  'widget' : 'Bool' }
                  ]

    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []
        logger.debug("SFrameAppSplitterJob split called")
        
        # Preparation
        inputnames=[]
        inputguids=[]
        if job.inputdata:

            if (job.inputdata._name == 'ATLASCastorDataset') or \
                   (job.inputdata._name == 'ATLASLocalDataset'):
                inputnames=[]
                for i in xrange(self.numsubjobs):    
                    inputnames.append([])
                for j in xrange(len(job.inputdata.get_dataset_filenames())):
                    inputnames[j % self.numsubjobs].append(job.inputdata.get_dataset_filenames()[j])

            if job.inputdata._name == 'ATLASDataset':
                for i in xrange(self.numsubjobs):    
                    inputnames.append([])
                for j in xrange(len(job.inputdata.get_dataset())):
                    inputnames[j % self.numsubjobs].append(job.inputdata.get_dataset()[j])

            if job.inputdata._name == 'DQ2Dataset':
                content = []
                input_files = []
                input_guids = []
                names = None
                # Get list of filenames and guids
                contents = [(guid, lfn) for guid, lfn in \
                            job.inputdata.get_contents() \
                            if '.root' in lfn]
                if self.match_subjobs_files:
                    self.numsubjobs = len(contents)
                # Fill dummy values
                for i in xrange(self.numsubjobs):    
                    inputnames.append([])
                    inputguids.append([])
                input_files = [ lfn  for guid, lfn in contents]
                input_guids = [ guid for guid, lfn in contents]

                logger.debug("inputfiles " + `input_files`)

                # Splitting
                for j in xrange(len(input_files)):
                    inputnames[j % self.numsubjobs].append(input_files[j])
                    inputguids[j % self.numsubjobs].append(input_guids[j])

        logger.debug(`inputnames`)

        # Do the splitting
        for i in range(self.numsubjobs):
            j = Job()
            j.inputdata=job.inputdata
            if job.inputdata:
                if job.inputdata._name == 'ATLASDataset':
                    j.inputdata.lfn=inputnames[i]
                else:
                    j.inputdata.names=inputnames[i]
                    if job.inputdata._name == 'DQ2Dataset':
                        j.inputdata.guids=inputguids[i]
                        j.inputdata.number_of_files = len(inputguids[i])
            j.outputdata=job.outputdata
            j.application = job.application
            j.backend=job.backend
            j.inputsandbox=job.inputsandbox
            j.outputsandbox=job.outputsandbox

            subjobs.append(j)
        return subjobs

class SFrameAppOutputMerger(IMerger):
    """SFrameApp handler for output merging"""
   
    _name = "SFrameAppOutputMerger"
    _schema = Schema(Version(1,0), {
        'sum_outputdir': SimpleItem(defvalue='',sequence=0, doc="Output directory of merged files"),
        'subjobs' : SimpleItem(defvalue=[],sequence=1, doc="Subjob numbers to be merged" ),
        'ignorefailed' : SimpleItem(defvalue = False, doc='Jobs that are in the failed or killed states will be excluded from the merge when this flag is set to True.')

        } )

    _GUIPrefs = [ { 'attribute' : 'sum_outputdir',  'widget' : 'String' },
                  { 'attribute' : 'subjobs',        'widget' : 'Int_List' },
                  { 'attribute' : 'ignorefailed',   'widget' : 'Bool'}]

    sum_outputlocation = ''

    class merger_root(threading.Thread):
        def __init__(self, cmd):
            self.cmd = cmd
            threading.Thread.__init__(self)
        def run(self):
            rc, out = getstatusoutput(self.cmd)
            logger.debug("Merging command: %s", self.cmd)
            logger.debug("Merging output: %s", out)
            if (rc==0):
                logger.info("Merging successful, output in: %s", SFrameAppOutputMerger.sum_outputlocation)
            else:
                logger.error("Error occured during merging: %s", out)

    def merge(self, subjobs = None, sum_outputdir = None, **options ):
        '''Merge local root tuples of subjobs output'''

        import os
        job = self._getRoot()

        if job.status is not "completed" and \
               job.status is not "failed":
            logger.error("Job status is: \"%s\". Wait until the job is completed (or failed)." % job.status)
            return 1
        
        try:
            logger.info("job output merging routine called.")
            logger.debug('job.outputdir: %s',job.outputdir)
            logger.debug('job.merger.sum_outputdir: %s',self.sum_outputdir)
            logger.debug('sum_outputdir: %s', sum_outputdir)
            logger.debug('self.subjobs: %s',self.subjobs)
            logger.debug('subjobs: %s',subjobs)
            
        except AttributeError:
            logger.error("job.outputdata errors")
            return 1

        # Output files
        outputfiles = [i for i in job.outputsandbox if '.root' in i]

        # Remove duplicates
        outputfiles = list(set(outputfiles))
        
        logger.debug('outputfiles : %s', outputfiles)
          
        # Check input parameters
        # Merging result output directory
        if sum_outputdir:
            tpath = expandfilename(sum_outputdir)
        elif self.sum_outputdir:
            tpath = expandfilename(self.sum_outputdir)

        if tpath[-1:]!="/":
            tpath = tpath + "/"
    
        if os.access(tpath,os.F_OK) and os.access(tpath,os.W_OK):
            SFrameAppOutputMerger.sum_outputlocation = tpath
            logger.debug("sum_outputlocation: %s ", SFrameAppOutputMerger.sum_outputlocation)
        else:
            logger.error("Outputdir %s not found", tpath)
            return 1

        # Merge only subset ?
        mergesubjob = []
        tsubjobs = None
        if subjobs:
            from Ganga.GPIDev.Lib.Job import Job
            tsubjobs = []
            for subjob in subjobs:
                if (subjob.__class__.__name__ == 'Job'):
                    id = "%d.%d" % (subjob.master.id, subjob.id)
                    tsubjobs.append(id)
                else:
                    tsubjobs.append(subjob)
        elif self.subjobs:
            tsubjobs = self.subjobs

        if tsubjobs:  # Subset of subjobs e.g. ['135.13500001', '134.134000002']
            for tsubjob in tsubjobs:
                mergesubjob.append(tsubjob)
        else: # All subjobs of a single jobs
            for subjob in job.subjobs:
                id = "%d.%d" % (job.id, subjob.id)
                mergesubjob.append(id)
            
        logger.debug('mergesubjob: %s',mergesubjob)

        # Find local output files
        rootfile = []
        for ifile in xrange(len(outputfiles)):
            isubjob = 0
            for subjob in mergesubjob:
                if isubjob==0:
                    rootfile.append([])

                pfn = "%s/%s" %(job.subjobs[int(subjob.split('.')[1])].outputdir, outputfiles[ifile])
                    
                fsize = filecheck(pfn)
                if (fsize>0):
                    rootfile[ifile].append(pfn)
                

                isubjob = isubjob + 1 


        # Check if ROOT is properly setup
        from Ganga.Utility.root import getrootprefix
        rc, rootprefix = getrootprefix()
        if (rc!=0):
            logger.error("No merging")
            return 1

        # put together ntuple merging command
        logger.debug("rootfile: %s", rootfile)
        logger.debug("subjobs: %d, outputfiles: %d, rootfiles: %d", len(job.subjobs), len(outputfiles), len(rootfile))

        # Determine merge command
        # use addAANT if available
        mergecmd = rootprefix + "hadd -f "
        rc,out = commands.getstatusoutput('which addAANT')
        if rc:
           mergecmd = "hadd -f " 
        else:
            mergecmd = out + " "
        logger.warning("Merge with: %s", mergecmd)
        
        for ifile in xrange(len(rootfile)):
            logger.debug(ifile)
            cmd = ""
            for isubjobs in xrange(len(rootfile[ifile])):
                cmd = cmd + rootfile[ifile][isubjobs] + " "
            if cmd:
                # FIXME: merging of TTrees (without option -T) seems to work only with ROOT v5                
                cmd = mergecmd + SFrameAppOutputMerger.sum_outputlocation + "/SUM%d_"%job.id+outputfiles[ifile] + " " + cmd
                # Do the merging
                thread = self.merger_root(cmd)
                thread.setDaemon(True)
                thread.start()
                
            else:
                logger.error("Nothing there to be merged.")

        # Subjob logfile merging

        ilogfile = options.get('logfile')

        if ilogfile == 1:
            cmd = ""
            for subjob in job.subjobs:
                try:
                    if mergesubjob.index(subjob.id)>=0:
                        cmd = cmd + subjob.outputdir + "stdout "
                except ValueError:
                    pass

            cmd = "cat " + cmd + " > " + SFrameAppOutputMerger.sum_outputlocation + "SUMstdout" 
            # Do the merging
            thread = self.merger_root(cmd)
            thread.setDaemon(True)
            thread.start()


config = makeConfig('SFrameApp','SFrame configuration parameters')
logger = getLogger('SFrameApp')


# disable type checking for 'exe' property (a workaround to assign File() objects)
# FIXME: a cleaner solution, which is integrated with type information in schemas should be used automatically
#config = getConfig('SFrameApp')
#config.setDefaultOption('xml_options',SFrameApp._schema.getItem('xml_options')['defvalue'], type(None),override=True)
#logger = getLogger('SFrameApp')

config.addOption('LCGOutputLocation', 'srm://srm-atlas.cern.ch/castor/cern.ch/grid/atlas/scratch/%s/ganga' % os.environ['USER'], 'FIXME')
config.addOption('LocalOutputLocation', '/castor/cern.ch/atlas/scratch/%s/ganga' % os.environ['USER'], 'FIXME')
config.addOption('ATLAS_SOFTWARE', '/afs/cern.ch/project/gd/apps/atlas/slc3/software', 'FIXME')
config.addOption('PRODUCTION_ARCHIVE_BASEURL', 'http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Production/kits/', 'FIXME')
config.addOption('ExcludedSites', '' , 'FIXME')
config.addOption('ATLASOutputDatasetLFC', 'prod-lfc-atlas-local.cern.ch', 'FIXME')


mc = getConfig('MonitoringServices')
mc.addOption('SFrameApp', None, 'FIXME')

# $Log: not supported by cvs2svn $
# Revision 1.2  2008/11/24 16:12:49  mbarison
# *** empty log message ***
#
# Revision 1.1  2008/11/19 15:42:58  mbarison
# first version
#
# Revision 1.4  2008/04/16 15:35:59  mbarison
# adding CVS log
#
