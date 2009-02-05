###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCGSandboxCache.py,v 1.4 2009-02-04 17:01:02 hclee Exp $
###############################################################################
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   January 2007
import os, os.path, sys, re, tempfile, time, random, md5
from types import *
from urlparse import urlparse

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.GridShell import getShell 

from Ganga.Lib.LCG.GridSandboxCache import GridFileIndex, GridSandboxCache
from Ganga.Lib.LCG.MTRunner import MTRunner, Data, Algorithm  
from Ganga.Lib.LCG.Utility import * 

lcg_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy().datadict
lcg_file_index_schema_datadict    = GridFileIndex._schema.inherit_copy().datadict

class LCGFileIndex(GridFileIndex):
    """
    Data object containing LCG file index information. 
    
    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    """

    _schema   = Schema( Version(1,0), lcg_file_index_schema_datadict )
    _category = 'GridFileIndex'
    _name = 'LCGFileIndex'

    def __init__(self, guid, lfc_host, local_fpath, md5sum):

        super(LCGFileIndex,self).__init__()

        self.id     = guid
        self.name   = os.path.basename(local_fpath)
        self.md5sum = md5sum
        self.attributes['lfc_host']    = lfc_host
        self.attributes['local_fpath'] = local_fpath

    def __str__(self):
        return '%s\t%s\t%s\t%s' % (self.id, self.attributes['lfc_host'], self.attributes['local_fpath'], self.md5sum)

class LCGSandboxCache(GridSandboxCache):

    '''Helper class for upladong/downloading/deleting sandbox files using lcg-cr/lcg-cp/lcg-del commands. 
    '''

    lcg_sandbox_cache_schema_datadict.update({
        'se'          : SimpleItem(defvalue='', copyable=1, doc='the LCG SE hostname'),
        'se_type'     : SimpleItem(defvalue='srmv2', copyable=1, doc='the LCG SE type'),
        'se_rpath'    : SimpleItem(defvalue='generated', copyable=1, doc='the relative path to the VO directory on the SE'),
        'lfc_host'    : SimpleItem(defvalue='', copyable=1, doc='the LCG LFC hostname'),
        'srm_token'   : SimpleItem(defvalue='', copyable=1, doc='the SRM space token, meaningful only when se_type is set to srmv2')
        } )

    _schema   = Schema( Version(1,0), lcg_sandbox_cache_schema_datadict )
    _category = 'GridSandboxCache'
    _name = 'LCGSandboxCache'
 
    logger = getLogger()

    def __init__(self):
        super(LCGSandboxCache,self).__init__()
        self.protocol = 'lcg'

    def __setattr__(self, attr, value):
        if attr == 'se_type' and value not in ['','srmv1','srmv2']:
            raise AttributeError('invalid se_type: %s' % value)
        super(LCGSandboxCache,self).__setattr__(attr, value)

    def impl_upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.
        """

        shell = getShell(self.middleware)

        if self.lfc_host:
            shell.env['LFC_HOST'] = self.lfc_host

        self.logger.debug('upload file with LFC_HOST: %s', shell.env['LFC_HOST'])

        ## the algorithm of uploading one file 
        class MyAlgorithm(Algorithm):

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj
                self.dirname  = self.cacheObj.__get_unique_fname__()

            def process(self, file):
                ## decide number of parallel stream to be used
                fsize    = os.path.getsize( urlparse(file)[2] )
                fname    = os.path.basename( urlparse(file)[2] )
                fpath    = os.path.abspath( urlparse(file)[2] )
                md5sum   = get_md5sum(fpath)
                nbstream = int((fsize*1.0)/(10.0*1024*1024*1024))
          
                if nbstream < 1: nbstream = 1 # min stream
                if nbstream > 8: nbstream = 8 # max stream
          
                cmd = 'lcg-cr -t 180 --vo %s -n %d' % (self.cacheObj.vo, nbstream)
                if self.cacheObj.se != None:
                    cmd  = cmd + ' -d %s' % self.cacheObj.se
                if self.cacheObj.se_type == 'srmv2' and self.cacheObj.srm_token:
                    cmd = cmd + ' -T srmv2 -s %s' % self.cacheObj.srm_token
          
                ## specify the physical location
                cmd = cmd + ' -P %s/ganga.%s/%s' % ( self.cacheObj.se_rpath, self.dirname, fname )
          
                ## specify the logical filename
                ## NOTE: here we assume the root dir for VO is /grid/<voname>
                lfc_dir = '/grid/%s/ganga.%s' % (self.cacheObj.vo, self.dirname)
                if not self.cacheObj.__lfc_mkdir__(shell, lfc_dir):
                    self.cacheObj.logger.warning('cannot create LFC directory: %s' % lfc_dir)
                    return None
          
                cmd = cmd + ' -l %s/%s %s' % (lfc_dir, fname, file)
                rc,output,m = self.cacheObj.__cmd_retry_loop__(shell, cmd, self.cacheObj.max_try)
          
                if rc != 0:
                    return False 
                else:
                    match = re.search('(guid:\S+)',output)
                    if match:
                        guid = match.group(1)
                        self.__appendResult__( file, LCGFileIndex(guid,self.cacheObj.lfc_host,fpath,md5sum) )
                        return True
                    else:
                        return False

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(myAlg, myData)
        runner.debug = False 
        runner.start()
        runner.join()

        return runner.getResults().values()

    def impl_download(self, files=[], dest_dir=None, opts=''):
        """
        Downloads multiple files from remote grid storages to 
        a local directory.
        """
        if not dest_dir:
            dest_dir = os.getcwd()
        self.logger.debug('download file to: %s', dest_dir)

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm): 

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj
                self.shell = getShell(self.cacheObj.middleware)

            def process(self, file):

                guid      = file.id
                lfn       = file.attributes['local_fpath']
                lfc_host  = file.attributes['lfc_host']
                fname     = os.path.basename( urlparse(lfn)[2] )

                self.shell.env['LFC_HOST']
                self.cacheObj.logger.debug('download file with LFC_HOST: %s', self.shell.env['LFC_HOST'])

                cmd  = 'lcg-cp -t %d --vo %s -T %s ' % (self.cacheObj.timeout, self.cacheObj.vo, self.cacheObj.se_type)
                cmd += '%s file://%s/%s' % (guid, dest_dir, fname)
             
                rc,output,m = self.cacheObj.__cmd_retry_loop__(self.shell, cmd, self.cacheObj.max_try)
             
                if rc != 0:
                    return False 
                else:
                    self.__appendResult__(file.id, file)
                    return True

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(myAlg, myData)
        runner.debug = False 
        runner.start()
        runner.join()

        return runner.getResults().values()

    def impl_delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages. 
        """

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm): 

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj
                self.shell = getShell(self.cacheObj.middleware)

            def process(self, file):

                guid = file.id

                lfc_host = file.attributes['lfc_host']

                self.shell.env['LFC_HOST'] = lfc_host

                self.cacheObj.logger.debug('delete file with LFC_HOST: %s' % self.shell.env['LFC_HOST'])

                cmd = 'lcg-del -a -t 60 --vo %s %s' % (self.cacheObj.vo, guid)
             
                rc,output,m = self.cacheObj.__cmd_retry_loop__(self.shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    return False 
                else:
                    self.__appendResult__(file.id, file)
                    return True

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(myAlg, myData)
        runner.debug = False 
        runner.start()
        runner.join()

        ## update the local index file
        del_files = runner.getResults().values()
        all_files = self.get_uploaded_files()

        left_files = []
        for f in all_files:
            if f not in del_files:
                 left_files.append(f)

        self.impl_bookkeepUploadedFiles(left_files, append=False)

        return del_files

    def impl_parseIndexFile(self, opts=''):
        """
        implementation for parsing the index file used for bookkeeping the uploaded files. 
        """

        files = [] 

        if not os.path.exists(self.index_file):
            logger.warning('file not found: %s' % self.index_file)
        else:
            f = open(self.index_file,'r')
            lines = map(lambda x:x.strip(), f.readlines())
            f.close()

            for l in lines:
                info   = l.split('\t')
                try:
                    guid   = info[0]
                    lfc    = info[1]
                    fpath  = info[2]
                    md5sum = info[3]
                    files.append(LCGFileIndex(guid, lfc, fpath, md5sum))
                except IndexError, e:
                    pass

        return files 

    # For GUID protocol
    def __lfc_mkdir__(self, shell, path, mode='775'):
        '''Creates a directory in LFC'''

        cmd = 'lfc-mkdir -p -m %s %s' % (mode, path)

        (rc, output, m) = self.__cmd_retry_loop__(shell, cmd, 1)

        if rc != 0:
            return False
        else:
            return True
