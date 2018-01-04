##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ATLASDataset.py,v 1.8 2009-04-24 08:54:23 dvanders Exp $
###############################################################################
# A simple ATLAS dataset
#
# ATLAS/ARDA

import os, re, fnmatch
import commands

from GangaCore.GPIDev.Lib.Dataset import Dataset
from GangaCore.GPIDev.Schema import *

from GangaCore.Utility.Config  import getConfig, makeConfig, ConfigError 
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Shell import Shell
from GangaCore.Utility.files import expandfilename
from GangaCore.Utility.GridShell import getShell

shell = Shell()
logger = getLogger()
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')

def filecheck(filename):
    """Check if filename exists and return filesize"""

    if filename.find("root://") != -1:
        # special check for EOS files
        cmd =  "%s %s// file info %s" % (config['PathToEOSBinary'], "/".join(filename.split("/")[:3]), '/'.join(filename.split("/")[3:]))
        rc, out = commands.getstatusoutput(cmd)

        if rc != 0:
            logger.debug("Problem checking EOS file '%s'. Ouptut: %s" % (filename, out))
            return -1

        # Assume first line is:
        # File: '/eos/atlas/user/m/mslater/tests/1.0/AnalysisSkeleton.aan.root'  Size: 1461388
        toks = out.split("\n")[0].split(":")
        if len(toks) != 3:
            logger.warning("Unexpected EOS output '%s'" % (out.split("\n")[0]))
            return 1

        fsize = int(toks[2])
        return fsize
    else:
        try:
            open(filename)
            fsize = os.stat(filename).st_size
        except IOError:
            logger.debug("File %s not found", filename)
            return -1
        
        if (fsize>0):
            return fsize


from commands import getstatusoutput    
import threading
from GangaCore.Core import GangaThread

class Download:
    """Helper class for background download of files stored on remote SEs"""
    def __init__(self):
        super(Download, self).__init__()

    lfns = []
    #rootfile = []
    lock = threading.RLock()

    class download_lcglr(GangaThread.GangaThread):
        def __init__(self, cmd):
            self.cmd = cmd
            GangaThread.GangaThread.__init__(self,'download_lcglr')

        def run(self):
            gridshell = getShell()

            gridshell.env['LFC_HOST'] = config['ATLASOutputDatasetLFC']
            gridshell.env['LCG_CATALOG_TYPE'] = 'lfc'
                         
            rc, out, m = gridshell.cmd1(self.cmd,allowed_exit=[0,255])
            #rc, out = getstatusoutput(self.cmd)
            if (rc==0):
                logger.debug("lcglr: %s", self.cmd)
                Download.lock.acquire()
                Download.lfns.append(out.strip())
                Download.lock.release()
            else:
                logger.error("Error occured during %s %s", self.cmd, out)

    class download_lcgcp(GangaThread.GangaThread):
        def __init__(self, cmd, ifile, pfn):
            self.cmd = cmd
            self.ifile = ifile
            self.pfn = pfn
            GangaThread.GangaThread.__init__(self,'download_lcgcp')
            
        def run(self):
            gridshell = getShell()
            gridshell.env['LFC_HOST'] = config['ATLASOutputDatasetLFC']
            gridshell.env['LCG_CATALOG_TYPE'] = 'lfc'
            rc, out, m = gridshell.cmd1(self.cmd,allowed_exit=[0,255])
            #rc, out = getstatusoutput(self.cmd)
            if (rc==0):
                logger.debug("lcg-cp finished: %s", self.cmd)
                logger.info("lcg-cp of %s finished", self.pfn)
                #Download.lock.acquire()
                #Download.rootfile[self.ifile].append(self.pfn)
                #Download.lock.release()
            else:
                logger.error("Error occured during %s %s", self.cmd, out)
    
    class download_dq2(GangaThread.GangaThread):
        def __init__(self, cmd):
            self.cmd = cmd
            GangaThread.GangaThread.__init__(self,'download_dq2')
            
        def run(self):
            gridshell = getShell()
            gridshell.env['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
            gridshell.env['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
            gridshell.env['DQ2_LOCAL_ID']=''

            ## Don't set up from the included DQ2 package as this will fail, either because
            # of python version (2.5+ required) or LFC python bindings missing
            
            #import GangaAtlas.PACKAGE
            #try:
            #    pythonpath=GangaAtlas.PACKAGE.setup.getPackagePath2('DQ2Clients','PYTHONPATH',force=False)
            #except:
            #    pythonpath = ''
            #gridshell.env['PYTHONPATH'] = gridshell.env['PYTHONPATH']+':'+pythonpath

            ## exclude the Ganga-owned external package for LFC python binding
            pythonpaths = []
            for path in gridshell.env['PYTHONPATH'].split(':'):
                if not re.match('.*\/external\/lfc\/.*', path):
                    pythonpaths.append(path)
            gridshell.env['PYTHONPATH'] = ':'.join(pythonpaths)
            
            ## exclude any rubbish from Athena
            ld_lib_paths = []
            for path in gridshell.env['LD_LIBRARY_PATH'].split(':'):
                if not re.match('.*\/external\/lfc\/.*', path) and not re.match('.*\/sw\/lcg\/external\/.*', path):
                    ld_lib_paths.append(path)
            gridshell.env['LD_LIBRARY_PATH'] = ':'.join(ld_lib_paths)
            
            paths = []
            for path in gridshell.env['PATH'].split(':'):
                if not re.match('.*\/external\/lfc\/.*', path) and not re.match('.*\/sw\/lcg\/external\/.*', path):
                    paths.append(path)
            gridshell.env['PATH'] = ':'.join(paths)
            
            rc, out, m = gridshell.cmd1("source " + configDQ2['setupScript'] + " && " + self.cmd,allowed_exit=[0,255])

            if (rc==0):
                logger.debug("dq2-get finished: %s", self.cmd)
                logger.debug("dq2-get output: %s %s %s"%(rc,out,m) )
                logger.warning("dq2-get finished")
            else:
                logger.error("Error occured during %s %s", self.cmd, out)


class ATLASTier3Dataset(Dataset):
    """ATLASTier3Dataset is a list of PFNs"""
    
    _schema = Schema(Version(1,0), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc='List of input file Physical File Names'),
        'pfnListFile': FileItem(doc='A text file containing a newline-separated list of Physical File Names'),
        })
    
    _category = 'datasets'
    _name = 'ATLASTier3Dataset'

    _exportmethods = []

    _GUIPrefs = [ { 'attribute' : 'names',  'widget' : 'String_List' },
                  { 'attribute' : 'pfnListFile',  'widget' : 'File' }
                 ]

    def __init__(self):
        super(ATLASTier3Dataset, self).__init__()

class ATLASLocalDataset(Dataset):
    """ATLAS Datasets is a list of local files"""
    
    _schema = Schema(Version(1,0), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='List of input files with full path'),
        'use_poolfilecatalog_failover' : SimpleItem(defvalue = False, doc = 'Use pool_insertFileToCatalog per single file if bulk insert fails'),
        'create_poolfilecatalog' : SimpleItem(defvalue = False, doc = 'Try to add these files to the PoolFileCatalog'),
    })
    
    _category = 'datasets'
    _name = 'ATLASLocalDataset'

    _exportmethods = ['get_dataset', 'get_dataset_filenames', 'get_dataset_from_list' ]

    _GUIPrefs = [ { 'attribute' : 'names',  'widget' : 'String_List' } ]
   
    def __init__(self):
        super(ATLASLocalDataset, self).__init__()
        
    def get_dataset_from_list(self,list_file,no_dir_check = False):
        """Get the dataset files as listed in a text file"""

        logger.info('Reading list file %s ...', list_file)

        if not os.path.exists(list_file):
            logger.error('File %s does not exist', list_file)
            return

        f = open( list_file )
        for ln in f.readlines():

            # ignore comments and blank lines
            if not ln.strip() or ln.strip()[0] == '#':
                continue

            # if no_dir_check then just copy the list of files
            if no_dir_check:
                self.names.append(ln.strip())
                continue

            # split the directory from the file and call get_dataset
            if os.path.isdir(ln.strip()):
                self.get_dataset( ln.strip() )
            else:
                self.get_dataset( os.path.dirname(ln.strip()), os.path.basename( ln.strip() ) )
           
    def get_dataset(self,directory,filter=None):
       """Get the actual files of a dataset"""
      
       logger.info('Reading %s ...',directory)


       if not os.path.isdir(directory):
           logger.error('Path %s is no directory',directory)
           return

       directory = os.path.abspath(directory)
       if filter:
           new_names = [ os.path.join(directory,name) for name in fnmatch.filter(os.listdir(directory),filter) ]
       else:
           new_names = [ os.path.join(directory,name) for name in os.listdir(directory) ]

       self.names.extend( new_names )

       self._setDirty()

    def get_dataset_filenames(self):
        """Get filenames"""
        return self.names

    @staticmethod
    def get_filenames(app):
        """Retrieve the file names starting from an application object"""
      
        job=app._getRoot()
        if not job:
            logger.warning('Application object is not associated to a job.')
            return []
         
#       jobs without inputdata are allowed
         
        if not job.inputdata: return []
      
        if not job.inputdata._name == 'ATLASLocalDataset':
            logger.warning('Dataset is not of type ATLASLocalDataset.')
            return []

        return job.inputdata.names


class ATLASOutputDataset(Dataset):
    """Generic ATLAS Dataset for a list of different outputfiles"""
    
    _schema = Schema(Version(1,0), {
        'outputdata'     : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc='Output files to be returned via SE'), 
        'output'         : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, protected=1, doc = 'Output information automatically filled by the job'),
        'location'       : SimpleItem(defvalue='',doc='SE output path location'),
        'local_location' : SimpleItem(defvalue='',doc='Local output path location')
        })
    
    _category = 'datasets'
    _name = 'ATLASOutputDataset'

    _exportmethods = [ 'retrieve', 'fill' ]

    _GUIPrefs = [ { 'attribute' : 'outputdata',     'widget' : 'String_List' },
                  { 'attribute' : 'output',         'widget' : 'String_List' },
                  { 'attribute' : 'location',       'widget' : 'String' },
                  { 'attribute' : 'local_location', 'widget' : 'File' } ]

    def __init__(self):
        super(ATLASOutputDataset, self).__init__()

    def fill(self, type=None, name=None, **options ):
        """Determine outputdata and outputsandbox locations of finished jobs
        and fill output variable"""
        from GangaCore.GPIDev.Lib.Job import Job
        job = self._getParent()

#       Determine local output path to store files
        if job.outputdata.location and ((job.backend._name == 'Local') or (job.backend._name == 'LSF') or (job.backend._name == 'PBS') or (job.backend._name == 'SGE') or (job.backend._name == 'Condor')):
            outputlocation =  expandfilename(job.outputdata.location)
            # update the local_location variable to point to the location
            job.outputdata.local_location = outputlocation
        elif job.outputdata.local_location:
            outputlocation = expandfilename(job.outputdata.local_location)
        else:
            try:
                tmpdir = os.environ['TMPDIR']
            except:
                tmpdir = '/tmp/'
            outputlocation = tmpdir

#       Output files on SE
        outputfiles = job.outputdata.outputdata
        
#       Search output_guid files from LCG jobs in outputsandbox
        jobguids = []

        if (job.backend._name == 'LCG' ):
            pfn = job.outputdir + "output_guids"
            fsize = filecheck(pfn)
            if (fsize>0):
                jobguids.append(pfn)
                logger.debug('jobsguids: %s', jobguids)
            
#       Get guids from output_guid files
            for ijobguids in jobguids: 
                f = open(ijobguids)
                tempguids =  [ line.strip() for line in f ]
                if not self.output:
                    self.output = self.output + tempguids

#       Local host execution
        if (job.backend._name == 'Local' or \
            job.backend._name == 'LSF' or \
            job.backend._name == 'PBS' or \
            job.backend._name == 'SGE' or \
            job.backend._name == 'Condor'):
            for file in outputfiles:

                pfn = outputlocation+"/"+file
                fsize = filecheck(pfn)
                if (fsize>0):
                    self.output.append(pfn)

#       Output files in the sandbox 
        outputsandboxfiles = job.outputsandbox
        for file in outputsandboxfiles:
            pfn = job.outputdir+"/"+file
            fsize = filecheck(pfn)
            if (fsize>0):
                self.output.append(pfn)

#       Master job finish
        if not job.master and job.subjobs:
            for subjob in job.subjobs:
                self.output.append(subjob.outputdata.output)


    def retrieve(self, type=None, name=None, **options ):
        """Retieve files listed in outputdata and registered in output from
        remote SE to local filesystem in background thread"""
        from GangaCore.GPIDev.Lib.Job import Job
        import os
        
        job = self._getParent()

        try:
            logger.info("ATLASOutputDataset.retrieve() called.")
            logger.debug('job.id: %d, Job.subjobs: %d',job.id,len(job.subjobs))
            logger.debug('job.outputdir: %s',job.outputdir)
            logger.debug('job.outputsandbox: %s',job.outputsandbox)
            logger.debug('job.outputdata.outputsandbox: %s',job.outputdata)
            logger.debug('job.outputdata.outputdata: %s',job.outputdata.outputdata)
            logger.debug('job.outputdata.output: %s',job.outputdata.output)
            logger.debug('job.outputdata.location: %s',job.outputdata.location)
            logger.debug('job.outputdata.local_location: %s',job.outputdata.local_location)
            
        except AttributeError:
            logger.error('job.outputdata error')
            return 1

        local_location = options.get('local_location')

#       Determine local output path to store files
        outputlocation = ''
        if job.outputdata.location and (job.backend._name == 'Local'):
            outputlocation = expandfilename(job.outputdata.location)
        elif local_location:
            outputlocation = expandfilename(local_location) 
        elif job.outputdata.local_location:
            outputlocation = expandfilename(job.outputdata.local_location)
        else:
            # User job repository location
            #outputlocation = job.outputdir+'/../'            
            pass
            
#       Output files 
        outputfiles = job.outputdata.outputdata
        

#$Log: not supported by cvs2svn $
#Revision 1.7  2009/04/24 08:36:21  dvanders
##49529: provide name for GangaThread
#
#Revision 1.6  2009/03/19 15:46:17  dvanders
#Thread->GangaThread
#
#Revision 1.5  2009/01/29 15:46:50  mslater
#Removed unnecessary print statement in Download class
#
#Revision 1.4  2008/08/19 13:15:32  elmsheus
#Fix bug #39010, Add ATLASLocalDataset support for list of datasets
#
#Revision 1.3  2008/07/28 15:02:30  elmsheus
#Fix for bug #35256
#
#Revision 1.2  2008/07/28 14:27:34  elmsheus
#* Upgrade to DQ2Clients 0.1.17 and DQ2 API
#* Add full support for DQ2 container datasets in DQ2Dataset
#* Change in DQ2OutputDataset.retrieve(): use dq2-get
#* Fix bug #39286: Athena().atlas_environment omits type_list
#
#Revision 1.1  2008/07/17 16:41:18  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.30.2.5  2008/05/12 15:55:38  elmsheus
#Fix small typo
#
#Revision 1.30.2.4  2008/05/12 09:07:31  elmsheus
#Add SGE output support
#
#Revision 1.30.2.3  2008/03/07 20:26:22  elmsheus
#* Apply Ganga-5-0-restructure-config-branch patch
#* Move GangaAtlas-4-15 tag to GangaAtlas-5-0-branch
#
#Revision 1.30.2.2  2008/02/18 11:03:22  elmsheus
#Copy GangaAtlas-4-13 to GangaAtlas-5-0-branch and config updates
#
#Revision 1.31  2008/03/03 14:11:40  elmsheus
#Athena:
#* Fix problem in requirements file creation - directories on lowest
#  level are now included by cmt
#* Fix DQ2JobSplitter problem with unused datasets - add exception
#* Fix problem in athena-local.sh - add ServiceMgr correctly
#* Add missing environment variables in DQ2OutputDataset.retrieve()
#* Add python32 detection in ganga-stage-in-out-dq2.py if dq2_get is
#  used
#
#Revision 1.30  2007/09/28 12:31:35  elmsheus
#Add local_location option to retrieve method
#
#Revision 1.29  2007/09/24 15:13:47  elmsheus
#* Change output path in ATLASOutputDataset.retrieve() methode from
#  jobid.subjobid to jobid/subjobid
#* Fix ATLASOutputDataset merging to actually find previously downloaded
#  files
#
#Revision 1.28  2007/09/24 08:42:10  elmsheus
#Apply patches to migrate to GangaCore.Utility.GridShell
#
#Revision 1.27  2007/05/23 13:28:31  elmsheus
#Add ATLASOutputDatasetLFC config variable to set LFC host for ATLASOutputDataset
#
#Revision 1.26  2007/04/02 08:07:25  elmsheus
#* Fix directory scanning procedure in Athena.prepare()
#* Fix GUIPrefs problems
#
#Revision 1.25  2007/03/24 15:36:26  liko
#Fix get_dataset in ATLASLocalDataset
#
#Revision 1.24  2007/03/21 15:11:29  elmsheus
#Add GUIPrefs
#
#Revision 1.23  2007/02/22 12:55:41  elmsheus
#Fix output path and use gridShell
#
#Revision 1.22  2007/02/21 08:09:04  elmsheus
#Small fixes
#
#Revision 1.21  2007/02/20 17:39:40  elmsheus
#Introduce new functionality in ATLASDataset: ganga-stagein-lfc.py
#specify list of lfns and lfc host
#direct access to inputdata files on worker node
#
#Revision 1.20  2007/02/12 15:31:42  elmsheus
#Port 4.2.8 changes to head
#Fix job.splitter in Athena*RTHandler
#
#Revision 1.19  2006/11/27 12:18:02  elmsheus
#Fix CVS merging errors
#
#Revision 1.18  2006/11/24 15:39:13  elmsheus
#Small fixes
#
#Revision 1.17  2006/11/24 13:32:37  elmsheus
#Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
#Add Frederics changes and improvement for AthenaMC
#
#Revision 1.16.2.4  2006/11/24 15:15:52  elmsheus
#Fix prefix_hack
#
#Revision 1.16.2.3  2006/11/22 14:20:52  elmsheus
#* introduce prefix_hack to lcg-cp/lr calls in
#  ATLASOutputDataset.retrieve()
#* fixed double downloading feature in
#  ATLASOutputDataset.retrieve()
#* move download location for ATLASOutputDataset.retrieve()
#  to job.outputdir from temp directory if local_location is not given
#* Print out clear error message if cmt parsing fails in Athena.py
#* Migrate to GridProxy library in Athena*RTHandler.py
#* Changes in output renaming schema for DQ2OutputDataset files
#
#* Fix proxy name bug in AthenaMCLCGRTHandler.py
#* Fix path problem in wrapper.sh
#
#Revision 1.16.2.2  2006/11/07 09:41:09  elmsheus
#Enable outputdata.retrieve() also for master job
#Add 'addAANT' root tuple merging
#
#Revision 1.16.2.1  2006/10/27 15:31:57  elmsheus
#Fix output location issue
#
#Revision 1.16  2006/10/23 07:21:27  elmsheus
#Fix AthenaLocaDataset bug
#
#Revision 1.15  2006/10/16 11:59:25  elmsheus
#Change ATLASLocalDataset get_dataset
#
#Revision 1.14  2006/09/08 16:11:44  elmsheus
#Expand SimpleItem directory variables with expandfilenames
#
#Revision 1.13  2006/09/08 14:40:46  elmsheus
#Fix ouput bug for LSF, PBS jobs
#
#Revision 1.12  2006/08/09 16:47:12  elmsheus
#Introduction of DQ2OutputDataset, fix minor bugs
#
#Revision 1.11  2006/07/09 08:41:01  elmsheus
#ATLASOutputDataset introduction, DQ2 updates, Splitter and Merger code clean-up, and more
#
#Revision 1.10  2006/05/27 10:18:01  elmsheus
#Fix ATLASLocalDataset processing problem
#
#Revision 1.9  2006/05/09 13:45:29  elmsheus
#Introduction of
# Athena job splitting based on number of subjobs
# DQ2Dataset and DQ2 file download
# AthenaLocalDataset
#
#Revision 1.8  2006/03/19 01:35:28  liko
#repair datasets
#
#Revision 1.7  2006/03/17 00:55:53  liko
#Fixes in ATLASDataset
#
#Revision 1.6  2006/03/15 16:30:07  liko
#Remove DIAL & ADA
#
#Revision 1.5  2006/03/15 15:49:14  liko
#Small fixes in ATLASCastorDataset
#
#Revision 1.4  2005/10/20 09:47:25  karl
#KH: minor correction
#
#Revision 1.3  2005/09/21 23:37:29  liko
#Bugfixes and support for Simulation
#
#Revision 1.2  2005/09/06 11:37:14  liko
#Mainly the Athena handler
#
