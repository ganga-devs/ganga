###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Athena.py,v 1.64 2009-07-09 19:04:03 elmsheus Exp $
###############################################################################
# Athena Job Handler
#
# ARDA/ATLAS
# 

import os, re, commands, string, sys, shutil
import math

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import PostprocessStatusUpdate
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaAtlas.Lib.ATLASDataset import filecheck, ATLASOutputDataset

from Ganga.Lib.Mergers.Merger import *
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Lib.File import ShareDir, File
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory, GPIProxyObject

from pandatools import AthenaUtils
from Ganga.Utility.Plugin import allPlugins

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

def file_install(athena_compile_flag='True'):

    return """# genereated by GANGA4
# Install a user area
#
# ATLAS/ARDA

ATHENA_MAJOR_RELEASE=`echo $ATLAS_RELEASE | cut -d '.' -f 1`

if [ n$SITEROOT != n'/afs/cern.ch' ] && [ n$CMTSITE != n'CERN' ]
then
  if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
  then
      source $SITEROOT/dist/$ATLAS_RELEASE/AtlasRelease/*/cmt/setup.sh
  elif ( [ $ATHENA_MAJOR_RELEASE -gt 11 ] ) && ( [ -z $ATLAS_PRODUCTION_ARCHIVE ] )
  then
      if [ -z $ATLAS_PROJECT ]
      then
          source $SITEROOT/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
      elif [ ! -z $ATLAS_PROJECT ]
      then
          source $SITEROOT/${ATLAS_PROJECT}/$ATLAS_PRODUCTION/${ATLAS_PROJECT}RunTime/cmt/setup.sh
      fi
  fi
else
  if [ ! -z `echo $ATLAS_RELEASE | grep 11.` ]
  then
      source $SITEROOT/software/dist/$ATLAS_RELEASE/Control/AthenaRunTime/*/cmt/setup.sh
  elif ( [ ! -z `echo $ATLAS_RELEASE | grep 12.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 13.` ] ) && [ -z $ATLAS_PRODUCTION_ARCHIVE ]
  then
      source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh 
      export ATLASOFFLINE=`cmt show path | grep AtlasOffline | sed -e ""s:.*path\ ::"" | sed -e ""s:\ from.*::""`
      source $ATLASOFFLINE/AtlasOfflineRunTime/cmt/setup.sh
      RHREL=`cat /etc/redhat-release`
      SC4=`echo $RHREL | grep -c 'Scientific Linux CERN SLC release 4'`
      if [ $SC4 -gt 0 ]; then
        export PATH=/afs/cern.ch/atlas/offline/external/slc3compat/1.0.0/bin/i686-slc3-gcc323/:$PATH
        export LD_LIBRARY_PATH=/afs/cern.ch/atlas/offline/external/slc3compat/1.0.0/bin/i686-slc3-gcc323/:$LD_LIBRARY_PATH
      fi
  elif ( [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] || [ ! -z `echo $ATLAS_RELEASE | grep 14.` ] ) && [ -z $ATLAS_PRODUCTION_ARCHIVE ]
  then
      source $ATLAS_SOFTWARE/$ATLAS_RELEASE/cmtsite/setup.sh -tag=$ATLAS_RELEASE,32
      source $ATLAS_SOFTWARE/$ATLAS_RELEASE/AtlasOffline/$ATLAS_RELEASE/AtlasOfflineRunTime/cmt/setup.sh
  fi
fi

export CMTPATH=$PWD:$CMTPATH
dum=`echo $LD_LIBRARY_PATH | tr ':' '\n' | egrep -v '^/lib' | egrep -v '^/usr/lib' | tr '\n' ':' `
export LD_LIBRARY_PATH=$dum

echo '**********************************************************'
echo 'CMTPATH = ' $CMTPATH
echo 'PYTHONPATH = '$PYTHONPATH
echo '**********************************************************'

cmt config
#cmt broadcast source setup.sh
#cmt broadcast cmt config
source setup.sh

echo '**********************************************************'
echo 'CMTPATH = ' $CMTPATH
echo 'PYTHONPATH = '$PYTHONPATH
echo '**********************************************************'

if [ '%(athena_compile_flag)s' = 'True' ]
then
    echo '==========================='
    echo 'GCC =' `which gcc`
    echo `gcc --version`
    echo 'PATH =' $PATH
    echo 'LD_LIBRARY_PATH =' $LD_LIBRARY_PATH
    echo '==========================='
    cmt broadcast gmake -s

    cmt config
    #cmt broadcast source setup.sh
    #cmt broadcast cmt config
    source setup.sh

fi
echo '**********************************************************'
echo 'CMTPATH = ' $CMTPATH
echo 'PYTHONPATH = '$PYTHONPATH
echo '**********************************************************'

""" % { 'athena_compile_flag' : athena_compile_flag } 

def create_tarball( userarea, runDir, currentDir, archiveDir, extFile, excludeFile, maxFileSize, useAthenaPackages, verbose, compile_flag ):
    """Helper function to create EXE tarball"""
    # gather normal files

    # go to workdir
    os.chdir(userarea)

    # gather files under work dir
    logger.info("gathering files under %s", userarea)

    # change from glob to re
    excludeFile2 = []
    for tmpItem in excludeFile:
        # change . to \. for regexp
        tmpItem = tmpItem.replace('.','\.')        
        # change * to .* for regexp
        tmpItem = tmpItem.replace('*','.*')
        # append
        excludeFile2.append(tmpItem)

    extFile2 = []
    for tmpItem in extFile:
        # change . to \. for regexp
        ###tmpItem = tmpItem.replace('.','\.')
        # change * to .* for regexp
        ####tmpItem = tmpItem.replace('*','.*')
        # append
        extFile2.append(tmpItem)

    # get files in the working dir
    skippedExt   = ['.o','.a','.so']
    skippedFlag  = False
    workDirFiles = []
    for tmpRoot,tmpDirs,tmpFiles in os.walk('.'):
        emptyFlag    = True
        for tmpFile in tmpFiles:
            tmpPath = '%s/%s' % (tmpRoot,tmpFile)
            # get size
            try:
                size = os.path.getsize(tmpPath)
            except:
                # skip dead symlink
                if verbose:
                    type,value,traceBack = sys.exc_info()
                    logger.info("  Ignore : %s:%s" % (type,value))
                continue
            # check exclude files
            excludeFileFlag = False
            for tmpPatt in excludeFile2:
                if re.search(tmpPatt,tmpPath) != None:
                    excludeFileFlag = True
                    break
            if excludeFileFlag:
                continue
            # skipped extension
            isSkippedExt = False
            for tmpExt in skippedExt:
                if tmpPath.endswith(tmpExt):
                    isSkippedExt = True
                    break
            # check root
            isRoot = False
            if re.search('\.root(\.\d+)*$',tmpPath) != None:
                isRoot = True
            # extra files
            isExtra = False
            for tmpExt in extFile2:
                if re.search(tmpExt+'$',tmpPath) != None:
                    isExtra = True
                    break
            # regular files
            if not isExtra:
                # unset emptyFlag even if all files are skipped
                emptyFlag = False
                # skipped extensions
                if isSkippedExt:
                    logger.info( "  skip %s %s" % (str(skippedExt),tmpPath))
                    skippedFlag = True
                    continue
                # skip root
                if isRoot:
                    logger.info( "  skip root file %s" % tmpPath)
                    skippedFlag = True
                    continue
                # check size
                if size > maxFileSize:
                    logger.info( "  skip large file %s:%sB>%sB" % (tmpPath,size, maxFileSize))
                    skippedFlag = True
                    continue

            # remove ./
            tmpPath = re.sub('^\./','',tmpPath)
            # append
            workDirFiles.append(tmpPath)
            emptyFlag = False
        # add empty directory
        if emptyFlag:
            # check for .* subdirs that will be excluded later
            skip = False
            for tmpdir in tmpDirs:
                if tmpdir[0] != '.':
                    skip = True
            for tmpfile in tmpFiles:
                if tmpdir[0] != '.':
                    skip = True
            if skip:
                continue
            
            tmpPath = re.sub('^\./','',tmpRoot)
            # skip tmpDir
            if tmpPath.split('/')[-1] == archiveDir.split('/')[-1]:
                continue
            # append
            workDirFiles.append(tmpPath)
    if skippedFlag:
        logger.info("please use --inputsandbox or j.inputdata.inputsandbox or change config.Athena.EXE_MAXFILESIZE if you need to send the skipped files to WNs")

    # create archive
    # use 'jobO' for libDS/noBuild
    # and 'sources' for build job
    if not compile_flag:
        archiveName     = 'jobO.%s.tar' % commands.getoutput('uuidgen 2> /dev/null')
        archiveFullName = "%s/%s" % (archiveDir,archiveName)
    else:
        archiveName     = 'sources.%s.tar' % commands.getoutput('uuidgen 2> /dev/null')
        archiveFullName = "%s/%s" % (archiveDir,archiveName)

    # collect files
    chunkLength = 100
    chunks = lambda l, n: [l[x: x+n] for x in xrange(0, len(l), n)]
    chunksworkDirFiles = chunks(workDirFiles, chunkLength)
    logger.info('Adding files to source tarball %s' % archiveFullName )
    for tmpChunkFiles in chunksworkDirFiles:
        #if os.path.islink(tmpChunkFiles):
        #    status,out = commands.getstatusoutput("tar --exclude '.[a-zA-Z]*' -rh '%s' -f '%s'" % (tmpFile,archiveFullName))
        #else:
        cmd = "tar --exclude '.[a-zA-Z]*' -rhf '%s' " % archiveFullName
        for tmpFile in tmpChunkFiles:
            cmd = cmd + " '%s' " % tmpFile
        status,out = commands.getstatusoutput(cmd)
        if verbose:
            logger.info(cmd)
        if status != 0 or out != '':
            logger.info(out)

    # go to tmpdir
    os.chdir(archiveDir)

    # compress
    #status,out = commands.getstatusoutput('gzip %s' % archiveName)
    #archiveName += '.gz'
    #if status !=0 or verbose:
    #    logger.info( out)

    # check archive
    #status,out = commands.getstatusoutput('ls -l %s' % archiveName)
    #if verbose:
    #    logger.info( out)
    #if status != 0:
    #    raise ApplicationConfigurationError("Failed to archive working area.\n If you see 'Disk quota exceeded'") 
 
    # check symlinks
    if useAthenaPackages:
        logger.info("checking symbolic links")
        status,out = commands.getstatusoutput('tar tvf %s' % archiveName)
        if status != 0:
            raise ApplicationConfigurationError("Failed to expand archive")

        symlinks = []    
        for line in out.split('\n'):
            items = line.split()
            if items[0].startswith('l') and items[-1].startswith('/'):
                symlinks.append(line)
        if symlinks != []:
            tmpStr  = "Found some unresolved symlinks which may cause a problem\n"
            tmpStr += "     See, e.g., http://savannah.cern.ch/bugs/?43885\n"
            tmpStr += "   Please ignore if you believe they are harmless"
            logger.warning(tmpStr)
            for symlink in symlinks:
                logger.info("  %s" % symlink)

    return archiveName, archiveFullName

class AthenaOutputDataset(GangaObject):
    """Specify the output datasets"""
   
    _schema = Schema(Version(1,0), {
        'location' : SimpleItem(defvalue='',doc='Output location'),
        'files'    : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Files to be returned') 
    })
   
    _category = 'athena_output_dataset'
    _name     = 'AthenaOutputDataset'
   
    def __init__(self):
        super(AthenaOutputDataset,self).__init__()
      
class Athena(IPrepareApp):
    """The main Athena Job Handler"""

    _schema = Schema(Version(2,3), {
                 'atlas_release'          : SimpleItem(defvalue='',doc='ATLAS Software Release'),
                 'atlas_production'       : SimpleItem(defvalue='',doc='ATLAS Production Software Release'),
                 'atlas_project'          : SimpleItem(defvalue='',doc='ATLAS Project Name'),
                 'atlas_cmtconfig'        : SimpleItem(defvalue='',doc='ATLAS CMTCONFIG environment variable'),
                 'atlas_exetype'          : SimpleItem(defvalue='ATHENA',doc='Athena Executable type, e.g. ATHENA, PYARA, ROOT, TRF, EXE '),
                 'atlas_environment'      : SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='Extra environment variable to be set'),
                 'atlas_dbrelease'        : SimpleItem(defvalue='LATEST',doc='ATLAS DBRelease DQ2 dataset and DQ2Release tar file. Use LATEST for most recent.'),
                 'atlas_run_dir'          : SimpleItem(defvalue='./', doc='ATLAS run directory'),
                 'atlas_run_config'       : SimpleItem(defvalue={'input': {'noInput': True}, 'other': {}, 'output': {'alloutputs': []}}, doc='ATLAS run configuration'),
                 'atlas_supp_stream'      : SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='suppress some output streams. e.g., [\'ESD\',\'TAG\']'),
                 'atlas_use_AIDA'         : SimpleItem(defvalue=False, doc='use AIDA'),
                 'trf_parameter'          : SimpleItem(defvalue={},typelist=["dict","str"], doc='Parameters for transformations'),
                 'user_area'              : FileItem(preparable=1, doc='A tar file of the user area'),
                 'user_area_path'         : SimpleItem(defvalue='', doc='Path where user_area tarfile is created'),
                 'athena_compile'         : SimpleItem(defvalue=False, doc='Switch if user code should be compiled remotely'),
                 'useAthenaPackages'      : SimpleItem(defvalue=False, doc='Switch to add AthenaPackages to tarball if Athena.exetype=EXE is used. Also used to enable cmt setup of athena packages for Panda when exetype is PYARA.'),
                 'group_area'             : FileItem(preparable=1, doc='A tar file of the group area'),
                 'max_events'             : SimpleItem(defvalue=-999, typelist=['int'], doc='Maximum number of events'),
                 'skip_events'            : SimpleItem(defvalue=0, typelist=['int'], doc='Number of events to skip'),
                 'run_event'              : SimpleItem(defvalue=[], typelist=['list'], doc='Run event list'),
                 'run_event_file'         : SimpleItem(defvalue='', doc='Name of the file containing run/event list for Panda backend'),
                 'option_file'            : FileItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc="list of job options files" ),
                 'options'                : SimpleItem(defvalue='',doc='Additional Athena options'),
                 'user_setupfile'         : FileItem(preparable=1, doc='User setup script for special setup'),
                 'exclude_from_user_area' : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Pattern of files to exclude from user area'),
                 'append_to_user_area'    : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Extra files to include in the user area'),
                 'exclude_package'        : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Packages to exclude from user area requirements file'),
                 'stats'                  : SimpleItem(defvalue = {}, doc='Dictionary of stats info'),
                 'collect_stats'          : SimpleItem(defvalue = False, doc='Switch to collect statistics info and store in stats field'),
                 'recex_type'             : SimpleItem(defvalue = '',doc='Set to RDO, ESD or AOD to enable RecExCommon type jobs of appropriate type'),
                 'glue_packages'          : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='list of glue packages which cannot be found due to empty i686-slc4-gcc34-opt. e.g., [\'External/AtlasHepMC\',\'External/Lhapdf\']'),
                 'is_prepared'            : SharedItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)', 'bool', 'str'],protected=0,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=1, doc='MD5 hash of the string representation of applications preparable attributes'),
                 'useRootCore'            : SimpleItem(defvalue = False, doc='Use RootCore'),
                 'useRootCoreNoBuild'     : SimpleItem(defvalue = False, doc='Use RootCore with NoBuild'),
                 'useMana'            : SimpleItem(defvalue = False, doc='Use Mana'),
                 'useNoDebugLogs'         : SimpleItem(defvalue = False, doc='Use debug print-out in logfiles of Local/Batch/CREAM/LCG backend'),
                 'useNewTRF'              : SimpleItem(defvalue = True, doc='Use the original filename with the attempt number for input in --trf when there is only one input, which follows the globbing scheme of new transformation framework'),
                 'useNoAthenaSetup'       : SimpleItem(defvalue = False, doc='Use No Athena setup to allow e.g. free ROOT setup'),
                 })
                     
    _category = 'applications'
    _name = 'Athena'
    _exportmethods = ['prepare_old', 'setup', 'postprocess', 'prepare', 'unprepare']
    
    _GUIPrefs = [ { 'attribute' : 'atlas_release',     'widget' : 'String' },
                  { 'attribute' : 'atlas_production',  'widget' : 'String' },
                  { 'attribute' : 'atlas_project',     'widget' : 'String' },
                  { 'attribute' : 'atlas_cmtconfig',   'widget' : 'String' },
                  { 'attribute' : 'atlas_exetype',    'widget' : 'String_Choice', 'choices':['ATHENA', 'PYARA', 'ROOT', 'TRF' ]},
                  { 'attribute' : 'atlas_environment', 'widget' : 'String_List' },
                  { 'attribute' : 'user_area',         'widget' : 'FileOrString' },
                  { 'attribute' : 'user_area_path',    'widget' : 'String' },
                  { 'attribute' : 'athena_run_dir',    'widget' : 'String' },
                  { 'attribute' : 'athena_compile',    'widget' : 'Bool' },
                  { 'attribute' : 'group_area',        'widget' : 'FileOrString' },
                  { 'attribute' : 'max_events',        'widget' : 'Int' },
                  { 'attribute' : 'skip_events',        'widget' : 'Int' },
                  { 'attribute' : 'run_event',        'widget' : 'List' },
                  { 'attribute' : 'option_file',       'widget' : 'FileOrString_List' },
                  { 'attribute' : 'options',           'widget' : 'String_List' },
                  { 'attribute' : 'user_setupfile',    'widget' : 'FileOrString' },
                  { 'attribute' : 'exclude_from_user_area', 'widget' : 'FileOrString_List' },
                  { 'attribute' : 'exclude_package',   'widget' : 'String_List' },
                  { 'attribute' : 'collect_stats',     'widget' : 'Bool' },
                  { 'attribute' : 'recex_type',     'widget' : 'String' },  
                  { 'attribute' : 'glue_packages',   'widget' : 'String_List' }
                  ]



    def read_cmt(self):
        """Get some relevant CMT settings"""
 
        rc, output = commands.getstatusoutput('cmt -quiet show macros')
        if rc: logger.warning('Return code %d from cmt command.',rc) 

        cmt = dict(re.findall('(.*)=\'(.*)\'\n',output))

        try:
            self.package       = cmt['package']
            self.userarea      = os.path.realpath(cmt['%s_cmtpath' % self.package])
        except KeyError:
            raise ApplicationConfigurationError(None, 'CMT could not parse correct environment ! \n Did you start/setup ganga in the run/ or cmt/ subdirectory of your athena analysis package ?')

        # Determine ATLAS Release Version
        rc, output = commands.getstatusoutput('cmt -quiet show projects')
        if rc: logger.warning('Return code %d from cmt command.',rc) 

        cmt = dict(re.findall('([\w]+) ([\w]+\.[\w]+\.[\w]+)',output))

        path = dict(re.findall('([\w]+) [\w]+\.[\w]+\.[\w]+\.[\w]+ \(in ([\S]+)\)',output))

        # Version 11.0.x
        try:
            self.atlas_release = cmt['AtlasRelease']
        except:
            pass
        # Version 12.0.x
        try:
            self.atlas_release = cmt['AtlasOffline']
        except:
            try:
                self.atlas_release = os.environ['AtlasVersion']
            except:
                pass

        try:
            self.atlas_production = os.path.basename(path['AtlasProduction'])
        except:
            self.atlas_production = ''

        # GroupArea
        lines = output.split('\n')
        tupLines = tuple(lines)
        lines = []
        for line in tupLines:
            if not line.startswith('#'):
                lines.append(line)
        lines = output.split('\n')
        athenaVer = ''
        productionVer = ''
        projectName = ''
        self.grouparea = ''
        allitems = []
        for line in lines:
            res = re.search('\(in ([^\)]+)\)',line)
            if res != None:
                items = line.split()
                allitems.append(items[0])
                if items[0] in ('dist', 'AtlasRelease', 'AtlasOffline'):
                    # Atlas release
                    athenaVer = os.path.basename(res.group(1))
                    break
                elif items[0] in [ 'AtlasProduction', 'AtlasPoint1', 'AtlasTier0'  ]:
                    # production cache
                    productionVer = '%s' % os.path.basename(res.group(1))
                    projectName = '%s' %items[0]
                else:
                    # group area
                    self.grouparea = os.path.realpath(res.group(1))
                    if (self.grouparea == self.userarea):
                        self.grouparea = ''

        if ('dist' not in allitems) and ('AtlasRelease' not in allitems) and ('AtlasOffline' not in allitems):
            self.grouparea = ''

        if self.atlas_release =='':
            self.atlas_release = athenaVer
        if productionVer != '':
            self.atlas_production = productionVer
        if projectName != '':
            self.atlas_project = projectName
        if 'CMTCONFIG' in os.environ:
            self.atlas_cmtconfig = os.environ['CMTCONFIG']
            if self.atlas_cmtconfig.startswith('x86_64'):
                raise ApplicationConfigurationError(None, 'CMTCONFIG = %s, Your CMT setup is using 64 bit - please change to 32 bit !'% self.atlas_cmtconfig )
        return

    def setup(self):
        """Run CMT setup script"""

        rc, output = commands.getstatusoutput('source setup.sh; printenv')
        if rc: logger.warning('Unexpected return code %d from setup command',rc)

        for key, val in re.findall('(\S+)=(\S+)\n',output):
            if key not in ['_','PWD','SHLVL']:
                os.environ[key] = val

    def collectStats(self):
        """Collect job statistics from different log files and fill dict
        Athena.stats"""
        import gzip, time, fileinput
        from Ganga.GPIDev.Lib.Job import Job
        job = self.getJobObject()

        if job.backend._name in [ 'Panda' ]:
            self.stats = job.backend.get_stats()
            return

        if job.backend._name in [ 'Jedi' ]:
            if job.backend.pandajobs:
                self.stats = [ pj.get_stats() for pj in job.backend.pandajobs ]
            return

        # Collect stats from LCG backend stats.pickle file
        if job.backend._name in [ 'LCG', 'CREAM', 'Local', 'SGE', 'LSF', 'PBS' ]:
            import pickle
            fileName =  os.path.join(job.outputdir + "stats.pickle")
            if "stats.pickle" in os.listdir(job.outputdir):  
                f = open(fileName,"r")
                self.stats = pickle.load(f)
                f.close()
                
        # collect stats from __jobscript__.log
        if '__jobscript__.log' in os.listdir(job.outputdir):
            fileName = os.path.join(job.outputdir,'__jobscript__.log' )
            try:
                for line in fileinput.input([fileName]):
                    if line.find('[Info] Job Wrapper start.')>-1:
                        starttime = re.match('(.*)  .*Info.* Job Wrapper start.',line).group(1)
                        self.stats['starttime'] = time.mktime(time.strptime(starttime))-time.timezone
                    if line.find('[Info] Job Wrapper stop.')>-1:
                        stoptime = re.match('(.*)  .*Info.* Job Wrapper stop.',line).group(1)
                        self.stats['stoptime'] = time.mktime(time.strptime(stoptime))-time.timezone
            except:
                pass

        # Collect stats from __jdlfile__ (LCG)
        if '__jdlfile__' in os.listdir(job.inputdir):
            self.stats['jdltime']  = int(os.stat(os.path.join(job.inputdir,'__jdlfile__'))[9])

        try:        
            if 'starttime' not in self.stats and 'gangatime1' in self.stats:
                self.stats['starttime'] = self.stats['gangatime1']
            if 'stoptime' not in self.stats and 'gangatime5' in self.stats:
                self.stats['stoptime'] = self.stats['gangatime5'] 
        except:
            pass

        # Return for LCG backend since stats.pickle is used
        if job.backend._name in [ 'LCG', 'CREAM', 'Local', 'SGE', 'LSF', 'PBS' ]:
            return

        # Compress NG stdout.txt
        if 'stdout.txt' in os.listdir(job.outputdir):
            fileNameIn = os.path.join(job.outputdir,'stdout.txt')
            fileNameOut = os.path.join(job.outputdir,'stdout.txt.gz')
            f_in = open(fileNameIn, 'rb')
            f_out = gzip.open(fileNameOut, 'wb')
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()

        # Compress NG stdout (fix for problem of gzip broken on some NG sites)
        if 'stdout' in os.listdir(job.outputdir):
            fileNameIn = os.path.join(job.outputdir,'stdout')
            fileNameOut = os.path.join(job.outputdir,'stdout.gz')
            f_in = open(fileNameIn, 'rb')
            f_out = gzip.open(fileNameOut, 'wb')
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()

        # collect stats from stderr
        try:
            if 'stderr.gz' in os.listdir(job.outputdir) or 'stdout.txt.gz' in os.listdir(job.outputdir) or 'stdout.gz' in os.listdir(job.outputdir):
                percentcpu = 0
                ipercentcpu = 0
                wallclock = 0
                usertime = 0
                systemtime = 0
                # LCG backend
                if 'stderr.gz' in os.listdir(job.outputdir):
                    zfile = os.popen('zcat '+os.path.join(job.outputdir,'stderr.gz' ))
                # NG has stdout.txt as output
                elif 'stdout.txt.gz' in os.listdir(job.outputdir):
                    zfile = os.popen('zcat '+os.path.join(job.outputdir,'stdout.txt.gz' ))
                elif 'stdout.gz' in os.listdir(job.outputdir):  
                    zfile = os.popen('zcat '+os.path.join(job.outputdir,'stdout.gz' ))   
                for line in zfile:
                    if line.find('Percent of CPU this job got')>-1:
                        try:
                            percentcpu = percentcpu + int(re.match('.*got: (.*).',line).group(1))
                        except ValueError:
                            percentcpu = 0  
                        ipercentcpu = ipercentcpu + 1
                    if line.find('Elapsed (wall clock) time')>-1:
                        try:
                            iwallclock = re.match('.*m:ss\): (.*)\.\d\d',line).group(1).split(':')
                            wallclock = wallclock + int(iwallclock[0])*60+int(iwallclock[1])
                        except:
                            iwallclock = re.match('.*m:ss\): (.*)',line).group(1).split(':')
                            wallclock = wallclock + int(iwallclock[0])*3600+int(iwallclock[1])*60+int(iwallclock[2])
                    if line.find('User time (seconds)')>-1:
                        iusertime = float(re.match('.*User time \(seconds\): (.*)',line).group(1))
                        usertime = usertime + iusertime
                    if line.find('System time (seconds)')>-1:
                        isystemtime = float(re.match('.*System time \(seconds\): (.*)',line).group(1))
                        systemtime = systemtime + isystemtime
                    if line.find('Exit status')>-1:
                        self.stats['exitstatus'] = re.match('.*status: (.*)',line).group(1)
                    if line.find('can not be opened for reading (Timed out)')>-1:
                        self.stats['filetimedout'] = True

                if ipercentcpu > 0:            
                    self.stats['percentcpu'] = percentcpu / ipercentcpu
                    self.stats['usertime'] = usertime
                    self.stats['systemtime'] = systemtime
                    self.stats['wallclock'] = wallclock
                else:
                    self.stats['percentcpu'] = 0
                    self.stats['wallclock'] = 0
                    self.stats['usertime'] = 0
                    self.stats['systemtime'] = 0
                if zfile:        
                    zfile.close()

        except MemoryError:
            logger.warning('ERROR in Athena.collectStats - logfiles too large to be unpacked.')
            pass

        # collect stats from stdout
        try:
            if 'stdout.gz' in os.listdir(job.outputdir) or 'stdout.txt.gz' in os.listdir(job.outputdir):
                totalevents = 0
                itotalevents = 0
                jtotalevents = 0
                numfiles = 0
                numfiles2 = 0
                numfiles3 = 0
                if 'stdout.gz' in os.listdir(job.outputdir):
                    zfile = os.popen('zcat '+os.path.join(job.outputdir,'stdout.gz' ))
                # NG has stdout.txt as output
                if 'stdout.txt.gz' in os.listdir(job.outputdir):
                    zfile = os.popen('zcat '+os.path.join(job.outputdir,'stdout.txt.gz' ))
                for line in zfile:
                    if line.find('Storing file at:')>-1:
                        self.stats['outse'] = re.match('.*at: (.*)',line).group(1)
                    if line.find('SITE_NAME=')>-1:
                        self.stats['site'] = re.match('SITE_NAME=(.*)',line).group(1)
                    #if line.find('Database being retired...')>-1:
                    #    self.stats['dbretired'] = True
                    if line.find('Core dump from CoreDumpSvc')>-1:
                        self.stats['coredump'] = True
                    if line.find('Cannot load entry')>-1:
                        self.stats['cannotloadentry'] = True
                    if line.find('cannot open a ROOT file in mode READ if it does not exists')>-1:
                        self.stats['filenotexist'] = True
                    if line.find('FATAL finalize: Invalid state "Configured"')>-1:
                        self.stats['invalidstateconfig'] = True
                    if line.find('failure in an algorithm execute')>-1:
                        self.stats['failalg'] = True
                    if line.find('events processed so far')>-1:
                        try:
                            itotalevents = int(re.match('.* run #\d+ (\d+) events processed so far.*',line).group(1))
                            jtotalevents = int(itotalevents)
                        except:
                            pass
                    if line.find('cObj_DataHeader...')>-1:
                        numfiles2 = numfiles2 + int(re.match('.* #=(.*)',line).group(1))
                    if line.find('"PFN:')>-1:
                        numfiles3 = numfiles3 + 1
                    if line.find('rfio://')>-1 and line.find('Always Root file version')>-1:
                        try:
                            self.stats['server'] = re.match('(.+://.+)//.*',line).group(1)
                        except:
                            self.stats['server'] = 'unknown'

                    if line.find('Info Database being retired...')>-1:
                        numfiles = numfiles + 1
                        totalevents = totalevents + itotalevents
                        itotalevents = 0
                    if line.find('GANGATIME1')==0:
                        self.stats['gangatime1'] = int(re.match('GANGATIME1=(.*)',line).group(1))
                    if line.find('GANGATIME2')==0:
                        self.stats['gangatime2'] = int(re.match('GANGATIME2=(.*)',line).group(1))
                    if line.find('GANGATIME3')==0:
                        self.stats['gangatime3'] = int(re.match('GANGATIME3=(.*)',line).group(1))
                    if line.find('GANGATIME4')==0:
                        self.stats['gangatime4'] = int(re.match('GANGATIME4=(.*)',line).group(1))
                    if line.find('GANGATIME5')==0:
                        self.stats['gangatime5'] = int(re.match('GANGATIME5=(.*)',line).group(1))

                    try:
                        if line.find('NET_ETH_RX_PREATHENA')==0:
                            self.stats['NET_ETH_RX_PREATHENA'] = int(re.match('NET_ETH_RX_PREATHENA=(.*)',line).group(1))
                        if line.find('NET_ETH_RX_AFTERATHENA')==0:
                            self.stats['NET_ETH_RX_AFTERATHENA'] = int(re.match('NET_ETH_RX_AFTERATHENA=(.*)',line).group(1))
                    except:
                        self.stats['NET_ETH_RX_PREATHENA'] = 0
                        self.stats['NET_ETH_RX_AFTERATHENA'] = 0

                    try:
                        if line.find('### node info:')==0:
                            self.stats['arch'] = re.match('### node info: .*,(.*, .*),.*,.*,.*,.*', line).group(1).strip()
                    except:
                        self.stats['arch'] = ''

                self.stats['numfiles2'] = numfiles2

                if job.inputdata and job.inputdata._name == 'DQ2Dataset':
                    if job.inputdata.type == 'DQ2_COPY':
                        self.stats['numfiles'] = numfiles / 2
                        self.stats['totalevents'] = totalevents
                        self.stats['numfiles3'] = numfiles3 / 2
                    elif job.inputdata.type == 'FILE_STAGER':
                        self.stats['numfiles'] = (numfiles - 2)/2
                        self.stats['totalevents'] = jtotalevents
                        self.stats['numfiles3'] = numfiles3 - 1
                    else:
                        self.stats['numfiles'] = numfiles - 1
                        self.stats['totalevents'] = jtotalevents
                        self.stats['numfiles3'] = numfiles3 - 1

                if zfile:        
                    zfile.close()

        except MemoryError:
            logger.warning('ERROR in Athena.collectStats - logfiles too large to be unpacked.')
            pass

        if job.backend._name in [ 'NG' ]:
            if 'gangatime1' in self.stats:
                self.stats['starttime'] = self.stats['gangatime1'] 
            if 'gangatime5' in self.stats:
                self.stats['stoptime'] = self.stats['gangatime5'] 

        try:        
            if 'starttime' not in self.stats and 'gangatime1' in self.stats:
                self.stats['starttime'] = self.stats['gangatime1']
            if 'stoptime' not in self.stats and 'gangatime5' in self.stats:
                self.stats['stoptime'] = self.stats['gangatime5'] 
        except:
            pass

    def postprocess_failed(self):
        """Check consistency of output dataset"""
        from Ganga.GPIDev.Lib.Job import Job
        job = self.getJobObject()
        if job.backend._name in [ 'LCG', 'CREAM' ]:
            # it's the master job 
            if not job.master and job.subjobs:
                numsubjobs = len(job.subjobs)
            else:
                numsubjobs = 0

            if job.outputdata:
                try:
                    job.outputdata.check_content_consistency(numsubjobs)
                except Exception as Value:
                    logger.warning('An ERROR occured during job.outputdata.check_consistency() call: %s, %s', Exception, Value)
                    pass

        return

    def postprocess(self):
        """Determine outputdata and outputsandbox locations of finished jobs
        and fill output variable"""
        from Ganga.GPIDev.Lib.Job import Job
        job = self.getJobObject()
        
        if not job.backend._name in [ 'NG', 'Panda', 'Jedi' ]:
            if job.outputdata:
                try:
                    job.outputdata.fill()
                except Exception as Value:
                    logger.warning('An ERROR occured during job.outputdata.fill() call: %s, %s', Exception, Value)
                    pass                                   

                if not job.outputdata.output:
                    logger.error('Could not stat output data. Marking job as failed.')
                    raise PostprocessStatusUpdate('failed')
                
        # collect athena job statistics
        if self.collect_stats and job.backend._name in [ 'LCG', 'CREAM', 'NG', 'Panda', 'Jedi', 'Local', 'SGE', 'LSF', 'PBS' ]:
            self.collectStats()
        # collect statistics for master job   
        if not job.master and job.subjobs and not job.backend._name in [ 'Jedi' ]:
            numfiles = 0
            numfiles2 = 0
            numfiles3 = 0
            totalevents = 0
            for subjob in job.subjobs:
                if 'numfiles' in subjob.application.stats:
                    if subjob.application.stats['numfiles']:
                        try:
                            numfiles = numfiles + int(subjob.application.stats['numfiles'])
                        except:
                            pass
                if 'numfiles2' in subjob.application.stats:
                    if subjob.application.stats['numfiles2']:
                        try:
                            numfiles2 = numfiles2 + int(subjob.application.stats['numfiles2'])
                        except:
                            pass
                if 'numfiles3' in subjob.application.stats:
                    if subjob.application.stats['numfiles3']:
                        try:
                            numfiles3 = numfiles3 + int(subjob.application.stats['numfiles3'])
                        except:
                            pass
                if 'totalevents' in subjob.application.stats:
                    if subjob.application.stats['totalevents']:
                        try:
                            totalevents = int(totalevents) + int(subjob.application.stats['totalevents'])
                        except:
                            pass
            self.stats['numfiles']=numfiles
            self.stats['numfiles2']=numfiles2
            self.stats['numfiles3']=numfiles3
            self.stats['totalevents']=totalevents        

    def files_lcg_ng(self):
        """Add install.sh and requirements to inputsandbox needed on LGC/NG backend"""

        # tmpDir
        if 'TMPDIR' in os.environ:
            tmpDir = os.environ['TMPDIR']
        else:
            cn = os.path.basename( os.path.expanduser( "~" ) )
            tmpDir = os.path.realpath('/tmp/' + cn )

        if not os.access(tmpDir,os.W_OK):    
            os.makedirs(tmpDir)

        # Check if user_area_path exists
        if self.user_area_path != '':
            if not os.path.exists(self.user_area_path):
                logger.warning('user_area_path %s does not exist! Using %s instead.', self.user_area_path, tmpDir )
                self.user_area_path = tmpDir
        else:
            self.user_area_path = tmpDir

        archiveDir = self.user_area_path
        
        savedir=os.getcwd()

        if self.athena_compile==1 or self.athena_compile==True:
            athena_compile_flag='True'
        if self.athena_compile==0 or self.athena_compile==False:
            athena_compile_flag='False'

        # Create install.sh
        filename = os.path.join(archiveDir,'install.sh' )
        file(filename,'w').write( file_install( athena_compile_flag))

        # Create requirements
        filename = os.path.join(archiveDir,'requirements' )
        req = file(filename,'w')
        req.write('# generated by GANGA\nuse AtlasPolicy AtlasPolicy-*\n')

        user_excludes = ['']

        os.chdir(self.userarea)
        out = commands.getoutput('find . -name cmt' )
        os.chdir(savedir)

        re_package1 = None
        re_package2 = None
        if self.atlas_release.find('11.')>=0 or self.atlas_release.find('10.')>=0:
            re_package1 = re.compile('^\./(.+)/([^/]+)/([^/]+)/cmt$')
        else:
            re_package1 = re.compile('^\./(.+)/([^/]+)/cmt$')
            re_package2 = re.compile('^\./(.+)/cmt$')

        for line in out.split():
            match1=re_package1.match(line)
            if match1 and not match1.group(2) in self.exclude_package:
                if self.atlas_release.find('11.')>=0 or self.atlas_release.find('10.')>=0:
                    req.write('use %s %s %s\n' % (match1.group(2),match1.group(3),match1.group(1)))
                else:
                    req.write('use %s %s-* %s\n' %  (match1.group(2), match1.group(2), match1.group(1)))

                user_excludes += ["%s/%s" % (match1.group(1),match1.group(2))]
                user_excludes += ["InstallArea/*/%s" % match1.group(2)]

            if re_package2:
                match2=re_package2.match(line)
                if match2 and not match1 and not match2.group(1) in self.exclude_package:
                    #req.write('use %s %s-* %s\n' %  (match2.group(1), match2.group(1), match2.group(1)))
                    req.write('use %s %s-*\n' %  (match2.group(1), match2.group(1) ))
                    user_excludes += ["%s" % match2.group(1)]
                    user_excludes += ["InstallArea/*/%s" % match2.group(1)]

        req.close()

        os.chdir(savedir)       
        # Read in extraFiles
        extraFiles = [ 'requirements', 'install.sh' ]
        extraFilesPath = []
        for ifile in extraFiles:
            extraFilesPath.append(os.path.join(archiveDir, ifile))

        os.chdir(savedir)
        return extraFilesPath

    def prepare(self, force=False, **options):
        """Extract Athena job configuration and prepare job for submission"""

        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        logger.info('Preparing %s application.'%(self._name))
        logger.debug('New prepare() method has been called. The old prepare method is called now prepare_old()')
        opt_athena_compile = options.get('athena_compile')
        if opt_athena_compile:
            self.athena_compile = opt_athena_compile  
            logger.warning('prepare(athena_compile=True/False) has been used - please change to the new option Athena.athena_compile=True/False.')

        # make sure the exetype is upper case
        self.atlas_exetype = self.atlas_exetype.upper()

        # Set CMTCONFIG
        if self.atlas_cmtconfig == "":
            if 'CMTCONFIG' in os.environ:
                self.atlas_cmtconfig = os.environ['CMTCONFIG']
                if self.atlas_cmtconfig.startswith('x86_64'):
                    #raise ApplicationConfigurationError(None, 'CMTCONFIG = %s, Your CMT setup is using 64 bit - please change to 32 bit !'% self.atlas_cmtconfig )
                    logger.warning('CMTCONFIG = %s, Your CMT setup is using 64 bit - are you sure you want to use 64 bit ?'% self.atlas_cmtconfig)
            else:
                self.atlas_cmtconfig = config['CMTCONFIG']
                os.environ['CMTCONFIG'] = self.atlas_cmtconfig 


        # check for conflicts
        if self.useMana and self.useRootCore:
            raise ApplicationConfigurationError(None,'Cannot specify RootCore and Mana. One or the other please!')
        
        # ensure mana jobs are EXE
        if self.useMana:
            if not self.atlas_exetype in ['EXE']:
                logger.info("Setting atlas_exetype to EXE for Mana running...")
                self.atlas_exetype = 'EXE'

            # setup mana
            from pandatools import MiscUtils

            # find the workarea
            tmpSt,tmpOut = MiscUtils.getManaSetupParam('workarea')
            if not tmpSt:
                raise ApplicationConfigurationError(None,'Problem getting workarea from Mana setup: "%s"' % tmpOut)

            logger.info("Setting the Mana work area to '%s'"% tmpOut.strip())

            # extract mana version number
            if self.atlas_release == '':
                tmpSt,tmpOut = MiscUtils.getManaVer()
                if not tmpSt:
                    raise ApplicationConfigurationError(None,'Problem getting Mana version from Mana setup: "%s"' % tmpOut.strip())

                logger.info("Setting the Mana version to '%s'"% tmpOut.strip())
                self.atlas_release = tmpOut.strip()
                
            # check mana version
            logger.info("Checking Mana version '%s'" % self.atlas_release)
            sMana, oMana, self.atlas_release, self.atlas_cmtconfig = MiscUtils.checkManaVersion(self.atlas_release, self.atlas_cmtconfig)

            if not sMana:
                raise ApplicationConfigurationError(None,'Error checking mana version: "%s"' % oMana)

            logger.info("Final Mana version '%s', cmt config '%s'" % (self.atlas_release, self.atlas_cmtconfig))

        # get info from CMT
        if not self.atlas_exetype in ['EXE'] or (self.atlas_release=='' and self.useNoAthenaSetup==False): 
            # get Athena versions
            rc, out = AthenaUtils.getAthenaVer()
            # failed
            if not rc:
                raise ApplicationConfigurationError(None, 'CMT could not parse correct environment ! \n Did you start/setup ganga in the run/ or cmt/ subdirectory of your athena analysis package ?')
            self.userarea = out['workArea'] 
            self.atlas_release = out['athenaVer'] 
            self.grouparea = out['groupArea'] 
            if out['cacheVer']:
                pat = re.compile('-(.*)_(.*)')
                match_pat = pat.match(out['cacheVer'])
                if match_pat:
                    self.atlas_project = match_pat.group(1)
                    self.atlas_production = match_pat.group(2)
            else:
                self.atlas_production = ''

        else: 
            self.userarea = os.path.realpath(os.getcwd())
            self.grouparea = ''
            if self.atlas_production and not self.atlas_project:
                self.atlas_project = 'AtlasProduction'

        logger.info('Found Working Directory %s',self.userarea)
        logger.info('Found ATLAS Release %s',self.atlas_release)
        if self.atlas_production:
            logger.info('Found ATLAS Production Release %s',self.atlas_production)
        if self.atlas_project:
            logger.info('Found ATLAS Project %s',self.atlas_project)
        logger.info('Found ATLAS CMTCONFIG %s',self.atlas_cmtconfig)
        if self.grouparea:
            logger.info('Found GroupArea at %s',self.grouparea)

        # save current dir
        currentDir = os.path.realpath(os.getcwd())
        # get run directory
        # remove special characters                    
        sString=re.sub('[\+]','.', self.userarea)
        runDir = re.sub('^%s' % sString, '', currentDir)
        if runDir == currentDir:
            raise ApplicationConfigurationError(None, 'You need to run prepare() in a directory under %s' % self.userarea)
        elif runDir == '':
            runDir = '.'
        elif runDir.startswith('/'):
            runDir = runDir[1:]
        runDir = runDir+'/'
        self.atlas_run_dir = runDir
        if not self.atlas_run_dir and self.atlas_exetype in ['EXE']: 
            self.atlas_run_dir = './'
        logger.info('Using run directory: %s',self.atlas_run_dir)

        # extract run configuration
        # run ConfigExtractor for normal jobO
        logger.info('Extracting athena run configuration ... ')
        logger.info('This can take a while since the athena autoconfiguration is called in the background to extract the jobOption configuration ... ')
        jobO = ''

        # Add special options
        if self.options:
            if self.options.startswith('-c'):
                jobO = ' %s ' % self.options
            else:
                jobO = ' -c %s ' % self.options

        if not self.option_file and not self.atlas_exetype in ['EXE', 'TRF']:
            raise ApplicationConfigurationError(None,'Set option_file before calling prepare()')
        for opt_file in self.option_file:
            if not self.atlas_exetype in ['EXE', 'TRF']: 
                if not opt_file.exists():
                    raise ApplicationConfigurationError(None,'The job option file %s does not exist.' % opt_file.name)
                else:
                    # check for dodgy TAG things
                    if 'uncompress.py' in self.append_to_user_area and 'subcoll.tar.gz' in self.append_to_user_area:
                        for ln in open(opt_file.name).readlines():
                            bad_lines = ['from IOVDbSvc.CondDB import conddb', 'include("RecJobTransforms/UseOracle.py")']
 
                            for bl in bad_lines:
                                posA = ln.find(bl)
                                posB = ln.find("#")
 
                                if posA != -1 and (posA < posB or posB == -1):
                                    raise ApplicationConfigurationError(None,'Please remove the line "%s" from your JOs' % bl)

                    jobO = jobO + opt_file.name + " "
            else:
                pass

        supStream = [s.upper() for s in self.atlas_supp_stream]
        shipInput = False
        trf = False
        if self.atlas_exetype in ['PYARA','ROOT','TRF','ARES','EXE']:
            trf = True

        logger.debug('jobO : %s', jobO)

        if not self.atlas_exetype in ['EXE']: 
            rc, runConfig = AthenaUtils.extractRunConfig(jobO, supStream, self.atlas_use_AIDA, shipInput, trf)
            #self.atlas_run_config = runConfig
            # The above line sometimes doesn't work and sets self.atlas_run_config to False because runConfig isn't just a dictionary
            # Therefore copy it by hand
            for k in runConfig:
                self.atlas_run_config[k] = runConfig[k]
            #self.atlas_run_config = {'input': {}, 'other': {}, 'output': {'outAANT': [('AANTupleStream', 'AANT', 'AnalysisSkeleton.aan.root')], 'alloutputs': ['AnalysisSkeleton.aan.root']}}
            logger.info('Detected Athena run configuration: %s',self.atlas_run_config)
            if not rc:
                raise ApplicationConfigurationError(None, 'Error in AthenaUtils.extractRunConfig - could not extract Athena configuration!')
        else:
            self.atlas_run_config = {'input': {}, 'other': {}, 'output': {}}
            logger.info('Set Athena run configuration to: %s',self.atlas_run_config)

        # tmpDir
        if 'TMPDIR' in os.environ:
            tmpDir = os.environ['TMPDIR']
        else:
            cn = os.path.basename( os.path.expanduser( "~" ) )
            tmpDir = os.path.realpath('/tmp/' + cn )

        if not os.access(tmpDir,os.W_OK):    
            os.makedirs(tmpDir)

        # Check if user_area_path exists
        if self.user_area_path != '':
            if not os.path.exists(self.user_area_path):
                logger.warning('user_area_path %s does not exist! Using %s instead.', self.user_area_path, tmpDir )
                self.user_area_path = tmpDir
        else:
            self.user_area_path = tmpDir
        savedir=os.getcwd()

        archiveDir = self.user_area_path

        # set extFile
        AthenaUtils.extFile=[]
        AthenaUtils.setExtFile(self.append_to_user_area)
        AthenaUtils.excludeFile=[]
        AthenaUtils.setExcludeFile(','.join(self.exclude_from_user_area))


        # copy RootCore packages
        if self.useRootCore or self.useRootCoreNoBuild:
            # check $ROOTCOREDIR
            if 'ROOTCOREDIR' not in os.environ:
                raise ApplicationConfigurationError(None,'$ROOTCOREDIR is not definied in your enviornment. Please setup RootCore runtime beforehand')

            # check grid_submit.sh
            rootCoreSubmitSh  = os.environ['ROOTCOREDIR'] + '/scripts/grid_submit.sh'
            if self.useRootCoreNoBuild:
                rootCoreSubmitSh  = os.environ['ROOTCOREDIR'] + '/scripts/grid_submit_nobuild.sh'

            rootCoreCompileSh = os.environ['ROOTCOREDIR'] + '/scripts/grid_compile.sh'
            rootCoreRunSh     = os.environ['ROOTCOREDIR'] + '/scripts/grid_run.sh'
            for tmpShFile in [rootCoreSubmitSh,rootCoreCompileSh,rootCoreRunSh]:
                if not os.path.exists(tmpShFile):
                    tmpErrMsg  = "%s doesn't exist. Please use a newer version of RootCore" % tmpShFile
                    raise ApplicationConfigurationError(None,tmpErrMsg)

            logger.info("Copying RootCore packages to current dir ...")
            # destination
            pandaRootCoreWorkDirName = '__panda_rootCoreWorkDir'
            rootCoreDestWorkDir = currentDir + '/' + pandaRootCoreWorkDirName

            # If exist, then delete tmp RootCore dir
            if os.path.exists(rootCoreDestWorkDir):
                logger.warning('Removing already previously existing temporary RootCore submission directory %s ...', rootCoreDestWorkDir)
                out = commands.getoutput('rm -rf ' + rootCoreDestWorkDir)

            # add all files to extFile
            #AthenaUtils.extFile.append(pandaRootCoreWorkDirName + '/.*')
            self.append_to_user_area+=[pandaRootCoreWorkDirName + '/.*']
            # add to be deleted on exit
            ###delFilesOnExit.append(rootCoreDestWorkDir)
            
            tmpStat = os.system('%s %s' % (rootCoreSubmitSh,rootCoreDestWorkDir))
            tmpStat %= 255
            if tmpStat != 0:
                tmpErrMsg  = "%s failed with %s" % (rootCoreSubmitSh,tmpStat)
                raise ApplicationConfigurationError(None,tmpErrMsg)
            # copy build and run scripts
            shutil.copy(rootCoreRunSh,rootCoreDestWorkDir)            
            shutil.copy(rootCoreCompileSh,rootCoreDestWorkDir)
                                            

        # archive sources
        verbose = False
        if self.atlas_exetype in ['EXE']: #and not self.athena_compile:  - for EXE, compilation decides what the tarball is called
            maxFileSize = config['EXE_MAXFILESIZE']
            archiveName, archiveFullName = create_tarball(self.userarea, runDir, currentDir, archiveDir, self.append_to_user_area, self.exclude_from_user_area, maxFileSize, self.useAthenaPackages, verbose, self.athena_compile )
        else:
            archiveName, archiveFullName = AthenaUtils.archiveSourceFiles(self.userarea, runDir, currentDir, archiveDir, verbose, self.glue_packages, config['dereferenceSymLinks'])
        logger.info('Creating %s ...', archiveFullName )

        # Add InstallArea
        if not self.athena_compile: 
            if not self.atlas_exetype in ['EXE']: 
                nobuild = True
                AthenaUtils.archiveInstallArea(self.userarea, self.grouparea, archiveName, archiveFullName, archiveDir, nobuild, verbose)
                logger.info('Option athena_compile=%s. Adding InstallArea to %s ...', self.athena_compile, archiveFullName )


        # Add LCG/NG files
        extraFiles = self.files_lcg_ng()
        for ifile in extraFiles:
            filename = os.path.split(ifile)[1]
            out = commands.getoutput('pushd . && cd %s && tar -f %s -rh %s && popd' % (archiveDir, archiveFullName, filename))
            os.unlink(ifile)

        # compress
        rc, out = commands.getstatusoutput('gzip %s' % archiveFullName)
        archiveName += '.gz'
        archiveFullName += '.gz'
        if rc != 0:
            logger.error(out)

        logger.info('Compressing to %s ...', archiveFullName )

        self.user_area.name = archiveFullName
        os.chdir(savedir)

        # Remove tmp RootCore dir
        if self.useRootCore or self.useRootCoreNoBuild:
            logger.info('Removing temporary RootCore submission directory %s ...', rootCoreDestWorkDir)
            out = commands.getoutput('rm -rf ' + rootCoreDestWorkDir)

       
        self.is_prepared = ShareDir()

        send_to_sharedir = self.copyPreparables()

        #get hold of the metadata object for storing shared directory reference counts
        #we now inherit this from the IPrepareApp class
        self.checkPreparedHasParent(self)
        self.post_prepare()

        # remove a temporary user area
        if config['RemoveTempUserAreaAfterPrepare'] and self.user_area_path == tmpDir:
            os.remove( self.user_area.name )

        return 1


    def prepare_old(self, athena_compile=True, NG=False, **options):
        """Prepare the job from the user area"""

        logger.warning('prepare_old() method has been called. The new prepare method is called prepare()')

        self.read_cmt()

        user_excludes = ['']

        logger.info('Found ATLAS Release %s',self.atlas_release)
        if self.atlas_production:
            logger.info('Found ATLAS Production Release %s',self.atlas_production)
        if self.atlas_project:
            logger.info('Found ATLAS Project %s',self.atlas_project)
        logger.info('Found ATLAS CMTCONFIG %s',self.atlas_cmtconfig)
        logger.info('Found User Package %s',self.package)
        logger.debug('Excluding Package %s',self.exclude_package)
        if self.grouparea:
            logger.info('Found GroupArea at %s',self.grouparea)
        savedir=os.getcwd()
        os.chdir(self.userarea)

        pfn = os.path.join(os.getcwd(), 'requirements')
        pfnTmp = pfn+'.gangatmp'            
        fsize = filecheck(pfn)
        if (fsize>0):
            os.rename(pfn,pfnTmp)

        req = file('requirements','w')
        req.write('# generated by GANGA4\nuse AtlasPolicy AtlasPolicy-*\n')

        out = commands.getoutput('find . -name cmt')
        re_package1 = None
        re_package2 = None
        if self.atlas_release.find('11.')>=0 or self.atlas_release.find('10.')>=0:
            re_package1 = re.compile('^\./(.+)/([^/]+)/([^/]+)/cmt$')
        else:
            re_package1 = re.compile('^\./(.+)/([^/]+)/cmt$')
            re_package2 = re.compile('^\./(.+)/cmt$')

        for line in out.split():
            match1=re_package1.match(line)
            if match1 and not match1.group(2) in self.exclude_package:
                if self.atlas_release.find('11.')>=0 or self.atlas_release.find('10.')>=0:
                    req.write('use %s %s %s\n' % (match1.group(2),match1.group(3),match1.group(1)))
                else:
                    req.write('use %s %s-* %s\n' %  (match1.group(2), match1.group(2), match1.group(1)))

                user_excludes += ["%s/%s" % (match1.group(1),match1.group(2))]
                user_excludes += ["InstallArea/*/%s" % match1.group(2)]
    
            if re_package2:
                match2=re_package2.match(line)
                if match2 and not match1 and not match2.group(1) in self.exclude_package:
                    #req.write('use %s %s-* %s\n' %  (match2.group(1), match2.group(1), match2.group(1)))
                    req.write('use %s %s-*\n' %  (match2.group(1), match2.group(1) ))
                    user_excludes += ["%s" % match2.group(1)]
                    user_excludes += ["InstallArea/*/%s" % match2.group(1)]

       
        req.close()

        if (athena_compile==True) and (NG==True):
            raise ApplicationConfigurationError(None, 'athena_compile==True and NG==True ! There is no compilation possible on NorduGrid (NG) - please remove either the athena_compile or NG option as argument of the prepare() method !')

        if athena_compile==1 or athena_compile==True:
            athena_compile_flag='True'
        if athena_compile==0 or athena_compile==False:
            athena_compile_flag='False'
        if NG==1 or NG==True:
            athena_compile_flag='False'

        file('install.sh','w').write( file_install( athena_compile_flag))
        if self.user_area_path != '':
            if not os.path.exists(self.user_area_path):
                from Ganga.Core import FileWorkspace
                ws=FileWorkspace.FileWorkspace(FileWorkspace.gettop(),subpath='file')
                ws.create(None)
                self.user_area_path = ws.getPath()

            self.user_area.name=mktemp('.tar.gz',self.package, self.user_area_path)

        else:  
            if 'TMPDIR' in os.environ:
                tmpDir = os.environ['TMPDIR']
            else:
                cn = os.path.basename( os.path.expanduser( "~" ) )
                tmpDir = os.path.realpath('/tmp/' + cn )

            if not os.access(tmpDir,os.W_OK):    
                os.makedirs(tmpDir)
            
            self.user_area_path = tmpDir
            
        self.user_area.name=mktemp('.tar.gz',self.package, self.user_area_path)
        logger.info('Creating %s ...',self.user_area.name)

        # Remove InstallArea from tar file if athena_compile==True
        # And not NorduGrid
        if NG==0 or NG==False:
            if athena_compile==1 or athena_compile==True:
                if not 'InstallArea' in self.exclude_from_user_area:
                    self.exclude_from_user_area.append( 'InstallArea' )
                if not self.atlas_cmtconfig in self.exclude_from_user_area:
                    self.exclude_from_user_area.append( self.atlas_cmtconfig )                    
                    logger.debug("Removing cmt created directories %s from tarfile" %  self.atlas_cmtconfig)
        
        tarcmd = 'tar '
        if self.exclude_from_user_area:
            tarcmd+='--wildcards '
            for ex in self.exclude_from_user_area:
                tarcmd+=' --exclude %s ' % ex

        if athena_compile and not NG:
            cmd = '%(tc)s -czhf %(ua)s . 2>/dev/null' % {'tc':tarcmd,'ua':self.user_area.name}
        elif NG:
            cmd = '%(tc)s -czhf %(ua)s . 2>/dev/null' % {'tc':tarcmd,'ua':self.user_area.name}
        else:
            cmd = '%(tc)s -czhf %(ua)s . 2>/dev/null' % {'tc':tarcmd,'ua':self.user_area.name}

        os.system(cmd)
        logger.debug(cmd)

        os.unlink('requirements')
        os.unlink('install.sh')

        fsize = filecheck(pfnTmp)
        if (fsize>0):
            os.rename(pfnTmp,pfn)

        # GroupArea
        excludes = string.join(user_excludes, " --exclude=")
        group_area_remote = options.get('group_area_remote')
        if self.grouparea:
            if not group_area_remote:
                if 'TMPDIR' in os.environ:
                    tmp = os.environ['TMPDIR']
                else:
                    cn = os.path.basename( os.path.expanduser( "~" ) )
                    tmp = os.path.realpath('/tmp/' + cn )
            
                tmpDir = '%s/%s' % (tmp,commands.getoutput('uuidgen 2> /dev/null'))    
                os.makedirs(tmpDir)
                os.chdir(tmpDir)       

                self.group_area.name=mktemp('.tar.gz',os.path.basename(self.grouparea),tmpDir)
                logger.info('Creating %s ...',self.group_area.name)

                os.chdir(self.grouparea)
            
                tarcmd = 'tar '
                #os.system('%(tc)s -czf %(ua)s . 2>/dev/null' % {'tc':tarcmd,'ua':self.group_area.name})
                os.system('%(tc)s -czf %(ua)s %(ex)s . 2>/dev/null' % {'tc':tarcmd,'ua':self.group_area.name,'ex':excludes})
                logger.debug(tarcmd)
            else:
                logger.info('Using Group area from: %s ',self.group_area.name)            
                
        os.chdir(savedir)       
        self._setDirty()

    def configure(self,masterappconfig):
        logger.debug('Athena configure called')
        return (None,None)

    def getLatestDBRelease(self):
        import tempfile,time
        import cPickle as pickle
        from pandatools import Client
        from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

        TMPDIR = tempfile.gettempdir()
        nickname = getNickname(allowMissingNickname=False)
        DBRELCACHE = '%s/ganga.latestdbrel.%s'%(TMPDIR,nickname)

        try:
            fh = open(DBRELCACHE)
            dbrelCache = pickle.load(fh)
            fh.close()
            if dbrelCache['mtime'] > time.time() - 3600:
                logger.debug('Loading LATEST DBRelease from local cache')
                self.atlas_dbrelease = dbrelCache['atlas_dbrelease']
            else:
                raise Exception()
        except:
            logger.debug('Updating local LATEST DBRelease cache')
            self.atlas_dbrelease = Client.getLatestDBRelease(False)
            dbrelCache = {}
            dbrelCache['mtime'] = time.time()
            dbrelCache['atlas_dbrelease'] = self.atlas_dbrelease
            fh = open(DBRELCACHE,'w')
            pickle.dump(dbrelCache,fh)
            fh.close()

    def master_configure(self):

        logger.debug('Athena master_configure called')

        if self.user_area.name:
            tmp_user_area_name = self.user_area.name
            if self.is_prepared is not True:
                from Ganga.Utility.files import expandfilename
                shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
                tmp_user_area_name = os.path.join(os.path.join(shared_path,self.is_prepared.name),os.path.basename(self.user_area.name))
                                        
            if not os.path.exists( tmp_user_area_name ):
                raise ApplicationConfigurationError(None,'The tar file %s with the user area does not exist.' % tmp_user_area_name)

        if self.group_area.name:
            if string.find(self.group_area.name,"http")<0 and not self.group_area.exists():
                raise ApplicationConfigurationError(None,'The tar file %s with the group area does not exist.' % self.group_area.name)
       
        for opt_file in self.option_file:
            if not self.atlas_exetype in ['EXE', 'TRF'] and not opt_file.exists():
                raise ApplicationConfigurationError(None,'The job option file %s does not exist.' % opt_file.name)


        job = self.getJobObject()

        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                if job.inputdata.dataset and not job.inputdata.dataset_exists():
                    raise ApplicationConfigurationError(None,'DQ2 input dataset %s does not exist.' % job.inputdata.dataset)
                if job.inputdata.tagdataset and not job.inputdata.tagdataset_exists():
                    raise ApplicationConfigurationError(None,'DQ2 tag dataset %s does not exist.' % job.inputdata.tagdataset)

        # check grid/local class match up
        if job.backend._name in ['LCG', 'CREAM' ,'Panda', 'NG']: 
            # check splitter
            if job.splitter and not job.splitter._name in ['DQ2JobSplitter', 'AnaTaskSplitterJob', 'ATLASTier3Splitter', 'GenericSplitter']:
                raise ApplicationConfigurationError(None,"Cannot use splitter type '%s' with %s backend" % (job.splitter._name, job.backend._name) )
            
            # Check that only DQ2Datasets/AMIDatasets are used on the grid        
            #if job.inputdata and not job.inputdata._name in ['DQ2Dataset', 'AMIDataset']:
            #    raise ApplicationConfigurationError(None,"Cannot use dataset type '%s' with %s backend" % (job.inputdata._name, job.backend._name) )
 
            # Check that only DQ2OutputDatasets are used on the grid
            #if job.outputdata and not job.outputdata._name in ['DQ2OutputDataset']:
            #    raise ApplicationConfigurationError(None,"Cannot use dataset type '%s' with %s backend" % (job.outputdata._name, job.backend._name))
        elif (job.backend._name in ['SGE' ] and config['ENABLE_SGE_DQ2JOBSPLITTER']):
            if job.splitter and not job.splitter._name in ['DQ2JobSplitter', 'AthenaSplitterJob']:
                raise ApplicationConfigurationError(None,"Cannot use splitter type '%s' with %s backend" % (job.splitter._name, job.backend._name) )

        elif job.backend._name in [ 'Jedi']: 
            # check splitter
            if job.splitter and not job.splitter._name in [ 'GenericSplitter']:
                raise ApplicationConfigurationError(None,"Cannot use splitter type '%s' with %s backend" % (job.splitter._name, job.backend._name) )
 
        else:
            
            # check splitter
            if job.splitter and not job.splitter._name in ['AthenaSplitterJob', 'AnaTaskSplitterJob', 'ATLASTier3Splitter']:
                raise ApplicationConfigurationError(None,"Cannot use splitter type '%s' with %s backend" % (job.splitter._name, job.backend._name) )
             
            # Check that only ATLASLocalDataset are used locally       
            #if job.inputdata and not job.inputdata._name in ['ATLASLocalDataset']:
            #    raise ApplicationConfigurationError(None,"Cannot use dataset type '%s' with %s backend" % (job.inputdata._name, job.backend._name) )
 
            # Check that only ATLASOutputDataset are used locally
            #if job.outputdata and not job.outputdata._name in ['ATLASOutputDataset']:
            #    raise ApplicationConfigurationError(None,"Cannot use dataset type '%s' with %s backend" % (job.outputdata._name, job.backend._name))
 
        # Check if DQ2_COPY is set and disable it
        if job.backend._name in ['LCG', 'CREAM' ]:
            if job.inputdata and job.inputdata._name in [ 'DQ2Dataset' ] and job.inputdata.type == 'DQ2_COPY' and not config['ENABLE_DQ2COPY']:
                raise ApplicationConfigurationError(None,"The workflow job.inputdata.type='DQ2_COPY' is not supported anymore ! Please use a different input access mode." )

        # Check if FILE_STAGER is set 
        if job.backend._name in ['SGE' ]:
            if job.inputdata and job.inputdata._name in [ 'DQ2Dataset' ] and job.inputdata.type == 'FILE_STAGER' and not config['ENABLE_SGE_FILESTAGER']:
                raise ApplicationConfigurationError(None,"The workflow job.inputdata.type='FILE_STAGER' is not enabled for SGE backend. Switch is on with config.Athena.ENABLE_SGE_FILESTAGER=True and use it carefully." )
        elif job.backend._name in ['LSF', 'PBS', 'Local', 'Condor' ]:
            if job.inputdata and job.inputdata._name in [ 'DQ2Dataset' ] and job.inputdata.type == 'FILE_STAGER':
                raise ApplicationConfigurationError(None,"The workflow job.inputdata.type='FILE_STAGER' is not enabled for the %s backend." % job.backend._name )
                         
        # check recex options
        if not self.recex_type in ['', 'RDO', 'ESD', 'AOD']:
            raise ApplicationConfigurationError(None, 'RecEx type %s not supported. Try RDO, ESD or AOD.' % self.recex_type)

        # Switch to RUCIO - no longer need to find the LATEST DB
        #try:
        #    if self.atlas_dbrelease == 'LATEST':
        #        try:
        #            self.getLatestDBRelease()
        #        except:
        #            from pandatools import Client
        #            self.atlas_dbrelease = Client.getLatestDBRelease(False)
        #except:
        #    raise ApplicationConfigurationError(None, "Error retrieving LATEST DBRelease. Try setting application.atlas_dbrelease manually.")
            

        if self.atlas_dbrelease: 
            match = re.search('DBRelease-(.*)\.tar\.gz', self.atlas_dbrelease )
            if match:
                dbvers = match.group(1)
                self.atlas_environment+=['DBRELEASE_OVERRIDE=%s'%dbvers] 

        return (0,None)

from Ganga.GPIDev.Adapters.ISplitter import ISplitter

class AthenaSplitterJob(ISplitter):
    """Athena handler for job splitting"""
    
    _name = "AthenaSplitterJob"
    _schema = Schema(Version(1,0), {
        'numsubjobs'          : SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs"),
        'numfiles_subjob'     : SimpleItem(defvalue=0,sequence=0, doc="Number of files per subjob"),
        'match_subjobs_files' : SimpleItem(defvalue=False,sequence=0, doc="Match the number of subjobs to the number of inputfiles"),
        'split_per_dataset'   : SimpleItem(defvalue=False,sequence=0, doc="Match the number of subjobs to the number of datasets"),
        'output_loc_to_input' : SimpleItem(defvalue={}, doc='Dictionary that lists the input files that should go to a '
                                                            'particular output dir, e.g. { "/out/dir": ["/in/file1", "/in/file2"].'
                                                            'Input files must match what is given to ATLASLocalDataset }')
        } )

    _GUIPrefs = [ { 'attribute' : 'numsubjobs',           'widget' : 'Int' },
                  { 'attribute' : 'numfiles_subjob',      'widget' : 'Int' },
                  { 'attribute' : 'match_subjobs_files',  'widget' : 'Bool' },
                  { 'attribute' : 'split_per_dataset',    'widget' : 'Bool' },
                  ]

    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []
        logger.debug("AthenaSplitterJob split called")
        
        # Preparation
        inputnames=[]
        inputguids=[]
        if job.inputdata:

            if (job.inputdata._name == 'ATLASCastorDataset') or \
                   (job.inputdata._name == 'ATLASLocalDataset'):
                inputnames = []
                outputnames = []
                numfiles = len(job.inputdata.get_dataset_filenames())
                if self.numfiles_subjob > 0:
                    self.numsubjobs = int( math.ceil( numfiles / float(self.numfiles_subjob) ) )
                if self.match_subjobs_files:
                    self.numsubjobs = numfiles
                if numfiles < self.numsubjobs:
                    self.numsubjobs = numfiles
                    logger.warning('Number of requested subjobs larger than number of files found - changing j.splitter.numsubjobs = %d ' %self.numsubjobs )

                # check for output data mapping
                if self.output_loc_to_input and isinstance(job.outputdata, ATLASOutputDataset):

                    # check all input data is available in the mapping dictionary
                    all_input = []
                    for file_list in self.output_loc_to_input.values():
                        all_input += file_list

                    if frozenset(all_input) != frozenset(job.inputdata.names):
                        raise ApplicationConfigurationError(None, 'Not all inputdata specified in splitter data mapping.'
                                                                  'Please make sure all input data is mapped.')

                    # check we have enough subjobs for outputdirs
                    if self.numsubjobs < len(self.output_loc_to_input):
                        self.numsubjobs = len(self.output_loc_to_input)
                        logger.warning('Number of requested subjobs smaller than number of output dirs found - '
                                       'changing j.splitter.numsubjobs = %d ' % self.numsubjobs)

                    # find num files per subjob - take into account extra jobs from output dir boundaries
                    num_files_per_sj = int(math.ceil(numfiles/(self.numsubjobs - len(self.output_loc_to_input)+1)))

                    # loop over the map and set the input/outputnames to the subjob number appropriately
                    # we split for every output file and also when we hit the number of files per subjob
                    for outdir in self.output_loc_to_input:

                        # another subjob needed
                        inputnames.append([])
                        outputnames.append([])
                        fnum = 0

                        for infile in self.output_loc_to_input[outdir]:
                            inputnames[-1].append(infile)
                            fnum += 1
                            if fnum == num_files_per_sj and infile != self.output_loc_to_input[outdir][-1]:
                                outputnames[-1] = outdir

                                # another subjob needed
                                inputnames.append([])
                                outputnames.append([])
                                fnum = 0

                        outputnames[-1] = outdir

                    if len(inputnames) != self.numsubjobs:
                        logger.warning('Generated %d subjobs instead of the requested %d subjobs due to output '
                                       'mapping' % (len(inputnames), self.numsubjobs))
                        self.numsubjobs = len(inputnames)

                    # check all input data was mapped
                    all_input = []
                    for file_list in inputnames:
                        all_input += file_list
                    if frozenset(all_input) != frozenset(job.inputdata.names):
                        raise ApplicationConfigurationError(None, "Inputdata was incorrectly mapped to output location."
                                                                  "This shouldn't happen - please contact the devs!")

                else:
                    for i in xrange(self.numsubjobs):
                        inputnames.append([])
                        outputnames.append([])

                    for j in xrange(numfiles):
                        inputnames[j % self.numsubjobs].append(job.inputdata.get_dataset_filenames()[j])

            if job.inputdata._name == 'ATLASDataset':
                for i in xrange(self.numsubjobs):    
                    inputnames.append([])
                for j in xrange(len(job.inputdata.get_dataset())):
                    inputnames[j % self.numsubjobs].append(job.inputdata.get_dataset()[j])

            if job.inputdata._name == 'DQ2Dataset':
                # Splitting per dataset
                if self.split_per_dataset:
                    contents = job.inputdata.get_contents(overlap=False)
                    datasets = job.inputdata.dataset
                    self.numsubjobs = len(datasets)
                    for dataset in datasets:
                        content = contents[dataset]
                        content.sort(lambda x,y:cmp(x[1],y[1]))
                        inputnames.append( [ lfn for guid, lfn in content ] )
                        inputguids.append( [ guid for guid, lfn in content ] )
                else:
                    # Splitting per file
                    content = []
                    input_files = []
                    input_guids = []
                    names = None
                    # Get list of filenames and guids
                    contents = job.inputdata.get_contents()
                    if self.match_subjobs_files:
                        self.numsubjobs = len(contents)
                    elif self.numfiles_subjob>0:
                        numjobs = len(contents) / int(self.numfiles_subjob)
                        if (len(contents) % self.numfiles_subjob)>0:
                            numjobs += 1
                        self.numsubjobs = numjobs
                        logger.info('Submitting %s subjobs',numjobs)

                    # Fill dummy values
                    for i in xrange(self.numsubjobs):    
                        inputnames.append([])
                        inputguids.append([])
                    input_files = [ lfn  for guid, lfn in contents ]
                    input_guids = [ guid for guid, lfn in contents ]

                    # Splitting
                    for j in xrange(len(input_files)):
                        inputnames[j % self.numsubjobs].append(input_files[j])
                        inputguids[j % self.numsubjobs].append(input_guids[j])

        if job.backend._name == 'LCG' and job.backend.middleware=='GLITE' and self.numsubjobs>config['MaxJobsAthenaSplitterJobLCG']:
            printout = 'Job submission failed ! AthenaSplitterJob.numsubjobs>%s - glite WMS does not like bulk jobs with more than approximately 100 subjobs - use less subjobs or use job.backend.middleware=="EDG"  ' %config['MaxJobsAthenaSplitterJobLCG']
            raise ApplicationConfigurationError(None, printout)

        # Do the splitting
        for i in range(self.numsubjobs):
            j = Job()
            j.name = job.name + "_" + str(i)
            j.inputdata=job.inputdata
            if job.inputdata:
                if job.inputdata._name == 'ATLASDataset':
                    j.inputdata.lfn=inputnames[i]
                else:
                    j.inputdata.names=inputnames[i]
                    if job.inputdata._name == 'DQ2Dataset':
                        j.inputdata.guids=inputguids[i]
                        j.inputdata.number_of_files = len(inputguids[i])
                        if self.split_per_dataset:
                            j.inputdata.dataset=job.inputdata.dataset[i]
            j.outputdata=job.outputdata

            # Set the output location if we have mapping
            if self.output_loc_to_input and isinstance(job.outputdata, ATLASOutputDataset):
                j.outputdata.location = outputnames[i]

            j.application = job.application
            j.backend=job.backend
            j.inputsandbox=job.inputsandbox
            j.outputsandbox=job.outputsandbox

            subjobs.append(j)
        return subjobs

class ATLASTier3Splitter(ISplitter):
    """Splitter for ATLASTier3Dataset"""
    
    _name = "ATLASTier3Splitter"
    _schema = Schema(Version(1,0), {
        'numjobs'              : SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs"),
        'numfiles'             : SimpleItem(defvalue=0,sequence=0, doc="Number of files per subjob")
        } )

    _GUIPrefs = [ { 'attribute' : 'numjobs',           'widget' : 'Int' },
                  { 'attribute' : 'numfiles',          'widget' : 'Int' },
                  ]

    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []
        logger.debug("ATLASTier3Splitter split called")
        
        if not job.inputdata:
            raise ApplicationConfigurationError(None, "ATLASTier3Splitter requires ATLASTier3Dataset")
        if job.inputdata._name != 'ATLASTier3Dataset':
            raise ApplicationConfigurationError(None, "ATLASTier3Splitter requires ATLASTier3Dataset")
        if self.numjobs and self.numfiles:
            logger.warning('You specified numjobs and numfiles. Setting numjobs = 0 to continue.')
            self.numjobs = 0
            #raise ApplicationConfigurationError(None, "ATLASTier3Splitter: specify numjobs or numfiles, but not both.")
       
        if job.inputdata.pfnListFile.name:
            logger.info('Loading file names from %s'%job.inputdata.pfnListFile.name)
            pfnListFile = open(job.inputdata.pfnListFile.name)
            job.inputdata.names = [name.strip() for name in pfnListFile]
            pfnListFile.close()
 
        allnames = list(job.inputdata.names)

        # default behaviour is 20 subjobs
        if not self.numjobs and not self.numfiles:
            self.numjobs = min(20,len(allnames))

        # limit numfiles and numjobs
        self.numjobs = min(self.numjobs,len(allnames))
        self.numfiles = min(self.numfiles,len(allnames))

        # calculate numfiles and numjobs
        if self.numfiles:
            (self.numjobs,r) = divmod(len(allnames),self.numfiles)
            if r: self.numjobs += 1
        elif self.numjobs:
            (self.numfiles,r) = divmod(len(allnames),self.numjobs)
            if r: self.numfiles += 1

        # Do the splitting
        allnames.reverse()
        for i in range(self.numjobs):
            j = Job()
            j.inputdata=job.inputdata
            j.inputdata.names=[]
            while allnames and len(j.inputdata.names) < self.numfiles:
                j.inputdata.names.append(allnames.pop())
            j.outputdata = job.outputdata
            j.application = job.application
            j.backend = job.backend
            j.inputsandbox = job.inputsandbox
            j.outputsandbox = job.outputsandbox
            subjobs.append(j)
        
        return subjobs

from Ganga.GPIDev.Adapters.IMerger import IMerger
from commands import getstatusoutput    
import threading
from GangaAtlas.Lib.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset import filecheck

class AthenaOutputMerger(IMerger):
    """Athena handler for output merging"""
   
    _name = "AthenaOutputMerger"
    _schema = Schema(Version(1,0), {
        'sum_outputdir': SimpleItem(defvalue='',sequence=0, doc="Output directory of merged files"),
        'subjobs' : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc="Subjob numbers to be merged" ),
        'ignorefailed' : SimpleItem(defvalue = False, doc='Jobs that are in the failed or killed states will be excluded from the merge when this flag is set to True.')
        } )

    _exportmethods = ['merge']
    _GUIPrefs = [ { 'attribute' : 'sum_outputdir',  'widget' : 'String' },
                  { 'attribute' : 'subjobs',        'widget' : 'Int_List' },
                  {'attribute' : 'ignorefailed', 'widget' : 'Bool'}
                  ]

    # set the automerge to *not* use the output dir of the job
    set_outputdir_for_automerge = False

    def execute(self, job, newstatus):
        """
        Overload Execute primarily so merge isn't called with an directory specified
        """
        if (len(job.subjobs) != 0):
            try:
                return self.merge(job.subjobs)
            except PostProcessException as e:
                logger.error(str(e))
                return self.failure
        else:
            return True

    def merge(self, subjobs = None, sum_outputdir = None, **options ):
        '''Merge local root tuples of subjobs output'''
        import os
        from Ganga.GPIDev.Lib.Job import Job
        job = self._getRoot()
        if (isinstance(job,GPIProxyObject) and not isinstance(job._impl,Job)) or not isinstance(job, Job):
            job = None
            id = None
        else:    
            id = '%d' % job.id

        filelist = []
        joblist = []
        
        # Append jobs to the joblist for cross job merging
        if subjobs:
            for ijob in subjobs:
                # check for subjobs of these jobs
                if not ijob.subjobs:
                    if isinstance(ijob,GPIProxyObject) and isinstance(ijob._impl,Job):
                        joblist.append(ijob._impl)
                    else:
                        joblist.append(ijob)                        
                else:                    
                    for isubjob in ijob.subjobs:
                        if isinstance(isubjob,GPIProxyObject) and isinstance(isubjob._impl,Job):
                            joblist.append(isubjob._impl)
                        else:
                            joblist.append(isubjob)
                    
        # Determine outputlocation
        local_location = options.get('local_location')
        if local_location:
            outputlocation = expandfilename(local_location)
            if id and not outputlocation.endswith(id):
                outputlocation = os.path.join( outputlocation, id )
        elif job.outputdata.local_location:
            outputlocation = expandfilename(job.outputdata.local_location)
            if not outputlocation.endswith(id):
                outputlocation = os.path.join( outputlocation, id )
        elif job.outputdata.location and (job.backend._name == 'Local'):
            outputlocation = expandfilename(job.outputdata.location)
        else:
            if job.outputdata._name=='DQ2OutputDataset':
                outputlocation = job.outputdir
            elif job.outputdata._name=='ATLASOutputDataset':
                outputlocation = job.outputdir

        if not sum_outputdir and self.sum_outputdir:
            sum_outputdir = self.sum_outputdir

        if sum_outputdir:
            try:
                if not os.path.exists(sum_outputdir):
                    os.makedirs(sum_outputdir)
                outputlocation = sum_outputdir
                
            except OSError:
                logger.error('Merger could create sum_outputdir: %s', sum_outputdir )
                pass
 

        #if job.status == 'completed':
        logger.debug('Merger outputlocation is: %s',outputlocation)

        # Determine file names
        if not job:
            sjlist = joblist
        else:
            sjlist = job._getRoot().subjobs

        for isubjob in sjlist:
            if isubjob.outputdata.output:                
                
                iline = 0
                for line in isubjob.outputdata.output:
                    if isubjob.outputdata._name=='DQ2OutputDataset':
                        [dataset,lfn,guid,size,md5sum,siteID]=line.split(",")
                    elif isubjob.outputdata._name=='ATLASOutputDataset':
                        lfn = isubjob.outputdata.outputdata[iline]
                        id = "%d" % (isubjob.id)
                        lfn = os.path.join(id, lfn)

                    pfn = os.path.join(outputlocation,lfn)

                    if not os.path.exists(pfn):
                        if isubjob.outputdata._name=='DQ2OutputDataset':
                            if isubjob.outputdata.local_location:
                                pfn = os.path.join( isubjob.outputdata.local_location, isubjob.outputdata.datasetname, lfn )
                            else:
                                # fall back on usual place
                                pfn = os.path.join( isubjob.outputdir , lfn )
                        else:
                            pfn = isubjob.outputdata.output[iline]

                    # find the output list from outputdata or construct if required
                    output_list = isubjob.outputdata.outputdata
                    if len(output_list) == 0:
                        output_list = []
                        # check for numbered extensions
                        ext = os.path.splitext(lfn)[1]
                        if ext[1:].isdigit():
                            ext = os.path.splitext( os.path.splitext(lfn)[0] )[1]
                        if ext == '.root':
                            output_list.append(lfn.split('.')[3] + '.root')
                            
                    for name in output_list:

                        if (os.path.splitext(name)[0] in lfn and os.path.splitext(name)[1] in lfn) or name in lfn:
                            pfnlink =  os.path.join( isubjob.outputdir, name )

                            if isubjob.outputdata._name=='DQ2OutputDataset' or (isubjob.outputdata._name=='ATLASOutputDataset' and not isubjob.outputdata.local_location==''):

                                try:
                                    open(pfn)
                                    fsize = os.stat(pfn).st_size
                                except IOError:
                                    if isubjob.status == 'completed':
                                        logger.debug('%s does not exist - please use retrieve() method to download file.', pfn)
                                    continue
                                if not fsize>0:
                                    if isubjob.status == 'completed':
                                        logger.debug('Filesize of %s is 0 - please use retrieve() method to download file.', pfn)
                                    continue
                                try:
                                    open(pfnlink)
                                except IOError:
                                    os.symlink(pfn, pfnlink)

                            if not name in filelist:
                                filelist.append(name)
                    iline = iline + 1

                
                if not isubjob in joblist:
                    joblist.append(isubjob)
        
        logger.debug('Merger filelist: %s',filelist)
        logger.debug('Merger joblist: %s', [ '%d.%d'%(j.master.id,j.id) for j in joblist] )
        #logger.debug('Merger joblist: %s', joblist )

        # Start the actual merging
        type = options.get('cmd')
        if type == 'addAANT':
            rm = RootMergerAANT()
        else:
            rm = SmartMerger()

        igfailed = False
        igfailedoption = options.get('ignorefailed')
        if igfailedoption == None:
            igfailedoption = self.ignorefailed

        if igfailedoption == True:
            igfailed = True
        else:
            igfailed = False
        
        rm.files = filelist
        rc = rm.merge(joblist, outputdir=outputlocation, overwrite=True, ignorefailed=igfailed)

        return rc


class RootMergerAANT(IMerger):
    """Merger class for AANT ROOT files"""
    
    _category = 'postprocessor'
    _exportmethods = ['merge']
    _name = 'RootMergerAANT'
    _schema = IMerger._schema.inherit_copy()

    def __init__(self):
        super(RootMergerAANT,self).__init__(_RootMergeToolAANT())

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(RootMergerAANT,self).merge(jobs, outputdir, ignorefailed, overwrite)

allPlugins.add(RootMergerAANT,'mergers','RootMergerAANT')

class _RootMergeToolAANT(IMerger):
    """Wrapper around addAANT that merges root files."""

    _category = 'postprocessor'
    _hidden = 1
    _name = '_RootMergeToolAANT'
    _schema = IMerger._schema.inherit_copy()

    def mergefiles(self, file_list, output_file):

        from Ganga.Utility.root import getrootprefix, checkrootprefix
        rc, rootprefix =  getrootprefix()

        if rc != 0:
            raise MergerError('ROOT has not been properly configured. Check your .gangarc file.')

        if checkrootprefix():
            raise MergerError('Can not run ROOT correctly. Check your .gangarc file.')

        #we always force as the overwrite is handled by our parent
        rc,out = commands.getstatusoutput('which addAANT')
        if rc:
            merge_cmd = rootprefix + "hadd -f " 
        else:
            merge_cmd = out + " "

        logger.debug("Merge with: %s", merge_cmd)

        #add the list of files, output file first
        arg_list = [output_file]
        arg_list.extend(file_list)
        merge_cmd += string.join(arg_list,' ')

        logger.info(merge_cmd)
        rc, out = commands.getstatusoutput(merge_cmd)
        
        if rc:
            logger.error(out)
            raise MergerError('The ROOT merge failed to complete. The cammand used was %s.' % merge_cmd)

           

logger = getLogger()
config = getConfig('Athena')

# $Log: not supported by cvs2svn $
# Revision 1.63  2009/07/02 19:36:23  elmsheus
# Small fix
#
# Revision 1.62  2009/07/02 18:15:29  elmsheus
# Initial import
#
# Revision 1.61  2009/06/22 14:36:19  mslater
# Added better support for RecEx* analyses
#
# Revision 1.60  2009/06/09 17:29:13  elmsheus
# Fix bug #51300
#
# Revision 1.59  2009/06/09 16:13:34  elmsheus
# bug #51369: the new prepare method generates bad requirements file
#
# Revision 1.58  2009/05/31 17:36:04  elmsheus
# Protection for fileinput.input
#
# Revision 1.56  2009/05/31 17:00:09  elmsheus
# Protection for itotalevents
#
# Revision 1.55  2009/05/28 08:48:32  elmsheus
# Fix totalevents problem
#
# Revision 1.54  2009/05/19 09:11:22  elmsheus
# Add CPU speed Architecure to stats
#
# Revision 1.53  2009/05/14 16:25:28  dvanders
# added option 'append_to_user_area'
#
# Revision 1.52  2009/05/07 06:32:56  dvanders
# Fix for https://savannah.cern.ch/bugs/index.php?50086
#
# Revision 1.51  2009/04/29 12:52:07  elmsheus
# Change schema version of Athena class
#
# Revision 1.50  2009/04/28 10:21:25  elmsheus
# Fix if numfiles is empty string
#
# Revision 1.49  2009/04/27 15:02:00  elmsheus
# * Fix for bug #49337: PYTHONPATH cmt setup problem in 15.0.0
#   should be solved now.
# * New: prepare() has been rewritten
#   - The functionality should be the same
#   - New CMT detection and tarfile creation for Panda
#   - New Athena.athena_compile variable
#   - prepare(athena_compile=True/False) is obsolete
# * renamed original method prepare() to prepare_old()
#
# Revision 1.48  2009/04/22 07:51:12  dvanders
# collect stats for Panda backend
#
# Revision 1.47  2009/04/20 16:07:16  elmsheus
# Rename prepare->prepare_old and panda_prepare->prepare
#
# Revision 1.46  2009/04/07 08:35:55  elmsheus
# Introduce panda_prepare, dependent now on panda-client external
#
# Revision 1.45  2009/04/06 07:17:41  elmsheus
# Fix for #48964, athena v15 setup
#
# Revision 1.44  2009/02/22 15:32:21  elmsheus
# New numfiles3 stats number
#
# Revision 1.43  2009/02/22 14:20:12  elmsheus
# Corrections for Network stats
#
# Revision 1.42  2009/02/20 10:13:54  elmsheus
# Fix for network
#
# Revision 1.41  2009/02/20 10:07:55  elmsheus
# Fix for network
#
# Revision 1.40  2009/02/20 09:41:08  elmsheus
# Fix for network
#
# Revision 1.39  2009/02/19 09:46:53  elmsheus
# Add network traffic information to Athena.stats
#
# Revision 1.38  2009/01/29 14:59:11  elmsheus
# #4620, Add dbrelease support
#
# Revision 1.37  2009/01/29 14:33:38  mslater
# Fix to make sure the merger works on DQ2OutputDataset
#
# Revision 1.36  2009/01/29 10:50:11  elmsheus
# Support for TRFs in the Athena application
#
# Revision 1.35  2009/01/15 08:31:40  elmsheus
# Some NG statistics patches
#
# Revision 1.34  2009/01/08 08:57:50  elmsheus
# Put patch at correct place
#
# Revision 1.33  2009/01/08 08:45:10  elmsheus
# Improve athena package setup and tarball packing
#
# Revision 1.32  2008/12/12 11:15:28  elmsheus
# Small fix
#
# Revision 1.31  2008/12/11 10:54:52  elmsheus
# Add Athena.user_area_path: configure path where to put user_area
#     file which is created during call of prepare()
#     Changed default to $TMPDIR or /tmp/$USER
#     If Athena.user_area_path='workspace' old default of ganga workspace
#     is used
#
# Revision 1.30  2008/12/10 15:24:20  elmsheus
# Fix for the master job numfiles2
#
# Revision 1.29  2008/12/10 15:20:21  elmsheus
# Change in the .gz reading and fixes for numfiles for the FILE_STAGER
#
# Revision 1.28  2008/12/08 16:01:49  elmsheus
# Make logfile reading more memory friendly
#
# Revision 1.27  2008/12/07 16:21:18  elmsheus
# Small fix
#
# Revision 1.26  2008/12/07 16:19:44  elmsheus
# Add try/except protection
#
# Revision 1.25  2008/12/07 16:02:09  elmsheus
# Introduce Athena.collect_stats switch and add master job statistics collection
#
# Revision 1.24  2008/11/27 12:15:33  elmsheus
# Fix timezone
#
# Revision 1.23  2008/11/27 10:44:59  elmsheus
# Athena.stats updates for GangaNG
#
# Revision 1.22  2008/11/27 07:48:58  elmsheus
# Small fix
#
# Revision 1.21  2008/11/25 19:35:37  elmsheus
# Small fix
#
# Revision 1.20  2008/11/25 19:30:44  elmsheus
# Add numfiles2
#
# Revision 1.19  2008/11/25 19:25:31  elmsheus
# Add numfiles2
#
# Revision 1.18  2008/11/25 08:12:47  elmsheus
# Add addition time parameters for Athena.stats
#
# Revision 1.17  2008/11/24 07:43:05  elmsheus
# Small fix
#
# Revision 1.16  2008/11/23 16:57:43  elmsheus
# Factorize and extend job statistics
#
# Revision 1.15  2008/11/17 15:52:23  elmsheus
# Add MaxJobsAthenaSplitterJobLCG again
#
# Revision 1.14  2008/11/17 15:38:58  elmsheus
# Make DCACHE_RA_BUFFER configurable
#
# Revision 1.13  2008/11/17 15:01:42  elmsheus
# Add Athena.stats
#
# Revision 1.12  2008/11/12 11:28:20  mslater
# Fix for bug 42661: User packages not being recognised in Athena Root dir
#
# Revision 1.11  2008/10/25 16:09:39  elmsheus
# Introduce splitting per dataset
#
# Revision 1.10  2008/10/21 09:23:16  elmsheus
# Add AtlasTier0
#
# Revision 1.9  2008/10/20 07:47:10  elmsheus
# Fix HelloWorld job for Local/Batch backend
#
# Revision 1.8  2008/10/16 15:43:37  elmsheus
# Athena.max_events has to been an integer now (Bug #42613)
#
# Revision 1.7  2008/10/07 21:25:40  elmsheus
# Increase maximum number of subjobs
#
# Revision 1.6  2008/09/30 12:09:31  mslater
# Small bug fix for local dataset merging
#
# Revision 1.5  2008/09/02 16:06:27  elmsheus
# Athena:
# * Fix SE type detection problems for space tokens at DPM sites
# * Change default DQ2 stage-out to use _USERDISK token
# * Change default DQ2Dataset default values of
#   DQ2_BACKUP_OUTPUT_LOCATIONS
# * Disable functionality of DQ2Dataset.match_ce_all and
#   DQ2Dataset.min_num_files and print out warning.
#   These are obsolete and DQ2JobSplitter should be used instead.
# * Add option config['DQ2']['USE_STAGEOUT_SUBSCRIPTION'] to allow DQ2
#   subscription to final output SE destinations instead of "remote lcg-cr"
#   Will be enabled in future version if DQ2 site services are ready for this
# =============================================
# A few changes to enforce the computing model:
# =============================================
# * DQ2OutputDataset: j.backend.location is verified to be in the same cloud
#   as j.backend.requirements.cloud during job submission to LCG
# * Add AtlasLCGRequirements.cloud option.
#   Use: T0, IT, ES, FR, UK, DE, NL, TW, CA, (US, NG) or
#        CERN, ITALYSITES, SPAINSITES, FRANCESITES, UKSITES, FZKSITES,
#        NLSITES, TAIWANSITES, CANADASITES, (USASITES, NDGF)
#
#   *********************************************************
#   Job submission to LCG requires now one of the following options:
#   - j.backend.requirements.cloud='ID'
#   - j.backend.requirements.sites=['SITENAME']
#   ********************************************************
# * Sites specified with j.backend.requirements.sites need to be in the same
#   cloud for the LCG backend
# * Restrict the number of subjobs of AthenaSplitterJob to value of
#   config['Athena']['MaxJobsAthenaSplitterJobLCG'] (100) if glite WMS is
#   used.
#
# scripts:
# * athena: Add --cloud option
#
# Revision 1.4  2008/07/30 13:13:10  elmsheus
# Fix bug #39549: raise execption if (athena_compile==True) and (NG==True)
#
# Revision 1.3  2008/07/30 07:28:57  elmsheus
# Debug print-outs during compilation
#
# Revision 1.2  2008/07/28 14:27:34  elmsheus
# * Upgrade to DQ2Clients 0.1.17 and DQ2 API
# * Add full support for DQ2 container datasets in DQ2Dataset
# * Change in DQ2OutputDataset.retrieve(): use dq2-get
# * Fix bug #39286: Athena().atlas_environment omits type_list
#
# Revision 1.1  2008/07/17 16:41:18  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.67.2.19  2008/07/12 09:37:17  elmsheus
# Fix for bug #38795
#
# Revision 1.67.2.18  2008/07/12 08:58:12  elmsheus
# * DQ2JobSplitter.py: Add numsubjobs option - now jobs can also be
#   splitted by number of subjobs
# * Athena.py: Introduce Athena.atlas_exetype, choices: ATHENA, PYARA, ROOT
#   Execute the following executable on worker node:
#   ATHENA: athena.py jobsOptions input.py
#   PYARA: python jobOptions
#   ROOT: root -q -b jobOptions
# * ganga-stage-in-out-dq2.py: produce now in parallel to input.py also a
#   flat file input.txt containing the inputfiles list. This files can be
#   read in but PYARA or ROOT application flow
# * Change --split and --splitfiles to use DQ2JobSplitter if LCG backend is used
# * Add --athena_exe ATHENA or PYARA or ROOT (see above)
#
# Revision 1.67.2.17  2008/07/10 06:26:13  elmsheus
# * athena-lch.sh: Fix problems with some DPM sites in athena v14
# Hurng-Chuns updates:
# * BOOT.py/Athena.py: improvements of cmtconfig magic function
# * DQ2Dataset.py: Fix wrong return value of get_contents
#
# Revision 1.67.2.16  2008/06/30 08:24:30  elmsheus
# Small fix
#
# Revision 1.67.2.15  2008/06/30 08:07:31  elmsheus
# Disallow 64bit user_area code
#
# Revision 1.67.2.14  2008/05/26 19:55:28  elmsheus
# Update AtlasProduction handling, add Athena.atlas_production
#
# Revision 1.67.2.13  2008/05/02 08:26:06  elmsheus
# Change postprocess for Panda and NG
#
# Revision 1.67.2.12  2008/05/01 16:36:06  elmsheus
# Migrate GangaAtlas-4-4-12 changes
#
# Revision 1.67.2.11  2008/04/07 16:28:30  elmsheus
# ATLAS_PROJECT and install.sh fix
#
# Revision 1.67.2.10  2008/04/07 05:02:49  elmsheus
# Update Project setup
#
# Revision 1.67.2.9  2008/04/03 12:50:06  elmsheus
# Update v14 setup for CERN, remove proxy config
#
# Revision 1.67.2.8  2008/04/03 08:01:52  elmsheus
# * Add updates for Athena v14 submission
# * Fix Athena version detection
#
# Revision 1.67.2.7  2008/04/01 13:33:33  elmsheus
# * New feature: DQ2Dataset and all other routine support multiple
#   datasets
# * Update athena 14 support
# * Change from ccdcapatlas to ccdcache for LYON
# * Add addition SE for BEIJING
# * Fix AtlasPoint1 setup problem in athena-lcg.sh and athena-local.sh
#
# Revision 1.67.2.6  2008/03/27 13:53:31  elmsheus
# * Updates for DQ2_COPY
# * Updates for v13 TAGs
# * Remove setting of X509_CERT_DIR during start-up
# * Add additional exception in DQ2Dataset
# * New version of dq2_get
#
# Revision 1.67.2.5  2008/03/20 14:56:02  elmsheus
# * config fixes
# * Typo fixes
#
# Revision 1.67.2.4  2008/03/20 12:53:59  elmsheus
# * Apply GangaAtlas-4-16 update
# * New option DQ2Dataset.type='DQ2_COPY'
#   copies input file from SE to worker node instead of Posix I/O
# * Fix configuration option problems
#
# Revision 1.67.2.3  2008/03/07 20:26:22  elmsheus
# * Apply Ganga-5-0-restructure-config-branch patch
# * Move GangaAtlas-4-15 tag to GangaAtlas-5-0-branch
#
# Revision 1.67.2.2  2008/02/18 11:03:23  elmsheus
# Copy GangaAtlas-4-13 to GangaAtlas-5-0-branch and config updates
#
# Revision 1.68  2008/01/18 09:07:00  elmsheus
# * Improve detection of AtlasPoint1, AtlasProduction and GroupAreas
#
# Revision 1.69  2008/03/03 14:11:43  elmsheus
# Athena:
# * Fix problem in requirements file creation - directories on lowest
#   level are now included by cmt
# * Fix DQ2JobSplitter problem with unused datasets - add exception
# * Fix problem in athena-local.sh - add ServiceMgr correctly
# * Add missing environment variables in DQ2OutputDataset.retrieve()
# * Add python32 detection in ganga-stage-in-out-dq2.py if dq2_get is
#   used
#
# Revision 1.68  2008/01/18 09:07:00  elmsheus
# * Improve detection of AtlasPoint1, AtlasProduction and GroupAreas
#
# Revision 1.67  2007/11/16 16:22:26  elmsheus
# Remove hardcoded atlas_release in install.sh
#
# Revision 1.66  2007/11/12 12:42:08  elmsheus
# Small fix
#
# Revision 1.65  2007/11/04 15:19:35  elmsheus
# Small Fix
#
# Revision 1.64  2007/11/04 14:25:07  elmsheus
# * Improve Athena Production release detection and use Athena.athena_production
# * Improve GroupArea detection
# * Change behaviour at j.application.prepare(athena_compile=False) in Athena
#   application: no files are stripped anymore from inputsandbox, ie. behaviour
#   is the same as for NG=True
#
# Revision 1.63  2007/10/03 17:58:56  elmsheus
# * Add remote group area downloading support
# * Add seconds to filename time stamp
#
# Revision 1.62  2007/10/01 15:12:32  elmsheus
# Set job to failed if job.outputdata.output is empty if ouput was requested
#
# Revision 1.61  2007/09/25 21:40:29  liko
# Improve error messages
#
# Revision 1.60  2007/09/24 15:13:47  elmsheus
# * Change output path in ATLASOutputDataset.retrieve() methode from
#   jobid.subjobid to jobid/subjobid
# * Fix ATLASOutputDataset merging to actually find previously downloaded
#   files
#
# Revision 1.59  2007/08/16 08:19:33  elmsheus
# Remove group area tar -h option and warning in merger
#
# Revision 1.58  2007/08/09 15:12:12  elmsheus
# Fix merger parameter ordering
#
# Revision 1.57  2007/08/02 09:11:31  elmsheus
# * Add ignorefailed variable to AthenaOutputMerger
# * Add 'numfiles_subjob' variable to AthenaSplitterJob
#   Enables job splitting based on the number of files per job
# * Fix logic of j.inputdata.match_ce_all and j.inputdata.min_num_files>X
#   Now min_num_files is chosen over match_ce_all
# * Display complete+incomplete locations as default of
#   DQ2Dataset.list_locations_num_files()
# * Change TAG usage:
#   - j.inputdata.type='TAG' is used now for TAG/AOD reading and ntuple output
#   - j.inputdata.type='TAG_REC' is now used for TAG/AOD reading and
#     AOD production via RecExCommon_topOptions.py
#
# Revision 1.56  2007/07/30 08:41:27  elmsheus
# Move new Merging to main branch
#
# Revision 1.55  2007/07/16 11:40:13  elmsheus
# * Fix groupArea unpacking for Local()
# * Fix 13.0.10 for Local()
# * Change GUIPrefs of Athena.option_file
#
# Revision 1.54.6.1  2007/07/17 15:24:36  elmsheus
# * Migrate to new RootMerger
# * Fix DQ2OutputLocation path problem
#
# Revision 1.54  2007/07/02 12:49:24  elmsheus
# Fix athena 13.0.x support
#
# Revision 1.53  2007/06/19 09:16:57  elmsheus
# Revert back changes
#
# Revision 1.52  2007/05/28 15:11:30  elmsheus
# * Introduce AtlasProduction cache setup with Athena.atlas_production
# * Enable 1 file per job splitting with AthenaSplitterJob.match_subjobs_files=True
# * Catch non-LFC bulk exception
# * Change wrong logging to 'GangaAtlas'
#
# Revision 1.51  2007/05/24 11:39:07  elmsheus
# Prevent overwriting of requirements, Change some logging info
#
# Revision 1.50  2007/05/22 17:21:13  elmsheus
# Introduce GroupAreas
#
# Revision 1.49  2007/05/08 15:51:18  elmsheus
# Fix merge for DQ2OutputDataset
#
# Revision 1.48  2007/04/18 14:45:09  elmsheus
# Change schema version of Athena class
#
# Revision 1.47  2007/04/17 13:54:15  elmsheus
# Another fix
#
# Revision 1.46  2007/04/17 08:43:04  elmsheus
# Fix prepare method for v11
#
# Revision 1.45  2007/04/13 14:04:18  elmsheus
# Fix GUIPrefs typo
#
# Revision 1.44  2007/04/04 08:33:58  elmsheus
# Small fix
#
# Revision 1.43  2007/04/03 11:56:27  elmsheus
# Add Athena.prepare(NG=True) flag to include all subdirectories
# and files of InstallArea and add no-compile flag.
#
# Revision 1.42  2007/04/03 07:40:07  elmsheus
# Add correct number_of_files for jobs and subjobs
#
# Revision 1.41  2007/04/02 09:55:44  elmsheus
# * Add number_of_files option in DQ2Dataset
# * Update splitting etc to new get_contents method
#
# Revision 1.40  2007/04/02 08:07:26  elmsheus
# * Fix directory scanning procedure in Athena.prepare()
# * Fix GUIPrefs problems
#
# Revision 1.39  2007/03/21 15:11:30  elmsheus
# Add GUIPrefs
#
# Revision 1.38  2007/03/19 15:09:33  elmsheus
# Improve tarball creation with athena_compile=False
#
# Revision 1.37  2007/03/13 13:45:21  elmsheus
# * Change default values of Athena.options and max_events and
#   convert max_events to str
# * Change logic of DQ2Dataset submission:
#   - Remove DQ2Dataset.match_ce
#   - by default jobs are sent to complete dataset locations
#   - with DQ2Dataset.match_ce_all=True jobs are sent to complete and
#     incomplete sources
# * Clean code in ganga-stage-in-out-dq2.py,
#   - use lcg-info for storage type identification
#   - VO_ATLAS_DEFAULT_SE as third option for host identification
#
# Revision 1.36  2007/03/07 09:33:46  elmsheus
# Rename fill to postprocess
#
# Revision 1.35  2007/03/07 08:19:00  elmsheus
# Add fill method in application
#
# Revision 1.34  2007/03/05 15:40:48  elmsheus
# Small fixes
#
# Revision 1.33  2007/03/05 09:55:00  liko
# DQ2Dataset leanup
#
# Revision 1.32  2007/02/28 08:52:38  elmsheus
# Add multiple jobOptions files - schema change
#
# Revision 1.31  2007/02/22 12:55:30  elmsheus
# Fix output path and use gridShell
#
# Revision 1.30  2007/02/13 09:12:28  elmsheus
# Add exclude_from_user_area
#
# Revision 1.29  2007/02/12 15:31:42  elmsheus
# Port 4.2.8 changes to head
# Fix job.splitter in Athena*RTHandler
#
# Revision 1.28  2007/01/30 11:19:41  elmsheus
# Port last changes from 4.2.7
#
# Revision 1.27  2007/01/22 09:51:01  elmsheus
# * Port changes from Ganga 4.2.7 to head:
#   - Athena.py: fix bug #22129 local athena jobs on lxplus - cmt interference
#                Fix athena_compile problem
#   - ganga-stage-in-out-dq2.py, athena-lcg:
#     Revise error exit codes correpsonding to ProdSys WRAPLCG schema
#   - DQ2Dataset.py: fix logger hick-ups
#   - Add patch to access DPM SE
#
# Revision 1.26  2006/12/21 17:21:41  elmsheus
# * Remove DQ2 curl functionality
# * Introduce dq2_client library and port all calls
# * Remove curl calls and use urllib instead
# * Remove ganga-stagein-dq2.py and ganga-stageout-dq2.py and merge into
#   new ganga-stage-in-out-dq2.py
# * Move DQ2 splitting from Athena*RTHandler.py into AthenaSplitterJob
#   therefore introduce new field DQ2Dataset.guids
# * Use AthenaMC mechanism to register files in DQ2 also for Athena plugin
#   ie. all DQ2 communication is done in the Ganga UI
#
# Revision 1.25  2006/11/27 12:18:03  elmsheus
# Fix CVS merging errors
#
# Revision 1.24  2006/11/24 15:39:13  elmsheus
# Small fixes
#
# Revision 1.23  2006/11/24 13:32:37  elmsheus
# Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
# Add Frederics changes and improvement for AthenaMC
#
# Revision 1.22.2.5  2006/11/22 14:20:53  elmsheus
# * introduce prefix_hack to lcg-cp/lr calls in
#   ATLASOutputDataset.retrieve()
# * fixed double downloading feature in
#   ATLASOutputDataset.retrieve()
# * move download location for ATLASOutputDataset.retrieve()
#   to job.outputdir from temp directory if local_location is not given
# * Print out clear error message if cmt parsing fails in Athena.py
# * Migrate to GridProxy library in Athena*RTHandler.py
# * Changes in output renaming schema for DQ2OutputDataset files
#
# * Fix proxy name bug in AthenaMCLCGRTHandler.py
# * Fix path problem in wrapper.sh
#
# Revision 1.22.2.4  2006/11/07 09:41:10  elmsheus
# Enable outputdata.retrieve() also for master job
# Add 'addAANT' root tuple merging
#
# Revision 1.22.2.3  2006/11/03 19:16:24  elmsheus
# Fix ATLAS release determination
#
# Revision 1.22.2.2  2006/10/31 11:27:41  elmsheus
# tar -h option fro athena_compile=False
#
# Revision 1.22.2.1  2006/10/27 15:33:16  elmsheus
# * Add compile option to Athena.prepare() method
#   j.application.prepare(athena_compile=False)
#   or j.application.prepare(athena_compile=True)
# * Fix athena setup issues with 12.0.x
# * Fix match_ce issue for subjobs
# * Fix output location issue for Local jobs
#
# Revision 1.22  2006/10/12 09:04:53  elmsheus
# DQ2 code clean-up
#
# Revision 1.21  2006/10/09 09:18:15  elmsheus
# Introduce shared inbox for job submission
#
# Revision 1.20  2006/09/08 16:11:45  elmsheus
# Expand SimpleItem directory variables with expandfilenames
#
# Revision 1.19  2006/09/07 12:41:45  elmsheus
# Fix bug ATLAS_RELEASE, Add cert to inbox for local job
#
# Revision 1.18  2006/09/04 11:54:39  elmsheus
# Fix CERN 11.0.5 setup problem
#
# Revision 1.17  2006/08/14 12:40:29  elmsheus
# Fix dataset handling during job submission, add match_ce flag for DQ2Dataset, enable ATLASDataset also for Local backend
#
# Revision 1.16  2006/08/11 08:22:24  elmsheus
# Fix dq2_get LFC download problem
#
# Revision 1.15  2006/08/10 15:56:10  elmsheus
# Introduction of TAG analysis, dq2_get updates, minor bugfixes
#
# Revision 1.14  2006/08/09 16:22:03  elmsheus
# Introduction of DQ2OutputDataset, fix minor bugs
#
# Revision 1.13  2006/07/31 13:44:16  elmsheus
# DQ2 updates, adapt to framework changes, migrate Ganga-2-7-2 fixes, enable 12.0.x, minor bugfixes
#
# Revision 1.12  2006/07/09 08:41:05  elmsheus
# ATLASOutputDataset introduction, DQ2 updates, Splitter and Merger code clean-up, and more
#
# Revision 1.11  2006/06/16 14:10:14  elmsheus
# Update Merger: new threading mechanism, choose remote download
#
# Revision 1.10  2006/06/13 15:27:25  elmsheus
# Initial Version of AthenaOutputMerger class for root/test merging on local and LCG system
#
# Revision 1.9  2006/05/25 14:25:47  elmsheus
# Fix defvalue of numsubjobs
#
# Revision 1.8  2006/05/09 13:45:30  elmsheus
# Introduction of
#  Athena job splitting based on number of subjobs
#  DQ2Dataset and DQ2 file download
#  AthenaLocalDataset
#
# Revision 1.7  2006/03/21 19:15:29  liko
# Lets do itcd working/ganga/python/GangaAtlascd working/ganga/python/GangaAtlas
#
# Revision 1.6  2006/03/15 15:42:07  liko
# deprecate shell
#
# Revision 1.5  2006/03/15 15:33:35  liko
# Fix ConfigException and some small problem with the options
#
# Revision 1.4  2005/10/11 11:56:37  liko
# Default values for new configuration file
#
# Revision 1.3  2005/09/06 11:37:14  liko
# Mainly the Athena handler
#
