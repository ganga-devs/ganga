###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCGSandboxCache.py,v 1.1 2008-09-11 16:55:19 hclee Exp $
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
from Ganga.GPIDev.Credentials import getCredential 

from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import isStringLike
from Ganga.Utility.GridShell import getShell 

from Ganga.Lib.LCG.GridSandboxCache import GridSandboxCache
from Ganga.Lib.LCG.MTRunner import MTRunner, Data, Algorithm  

lcg_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy().datadict


class LCGSandboxCache(GridSandboxCache):

    '''Helper class for upladong/downloading/deleting sandbox files using lcg-cr/lcg-cp/lcg-del commands. 
    '''

    lcg_sandbox_cache_schema_datadict.update({
        'vo'          : SimpleItem(defvalue='dteam', doc='the Grid virtual organization'),
        'se'          : SimpleItem(defvalue='', doc='the LCG SE hostname'),
        'se_type'     : SimpleItem(defvalue='srmv2', doc='the LCG SE type'),
        'se_rpath'    : SimpleItem(defvalue='generated', doc='the relative path to the VO directory on the SE'),
        'lfc_host'    : SimpleItem(defvalue='', doc='the LCG LFC hostname'),
        'srm_token'   : SimpleItem(defvalue='', doc='the SRM space token, meaningful only when se_type is set to srmv2'),
        'middleware'  : SimpleItem(defvalue='EDG', doc='the LCG middleware type')
        } )

    _schema   = Schema( Version(1,0), lcg_sandbox_cache_schema_datadict )
    _category = 'GridSandboxCache'
    _name = 'LCGSandboxCache'
 
    logger = getLogger()

    def __init__(self):
        super(LCGSandboxCache,self).__init__()
        self.protocol = 'lcg'

    def upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.

        files in the format:
          - [ file1, file2, ... ]

        and element can be a File object or a path string.
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
                        self.__appendResult__(file, match.group(1))
                        return True
                    else:
                        return False

        paths = []
        for f in files:
            if f.__class__.__name__ == 'File':
                paths.append('file://%s' % f.name)
            elif f.__class__.__name__ == 'str':
                paths.append('file://%s' % f)
            else:
                logger.warning('unknown file expression: %s' % repr(f))

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=paths)

        runner = MTRunner(myAlg, myData)
        runner.debug = True
        runner.start()
        runner.join()
        return runner.getResults()

    def download(self, files={}, dest_dir=None, opts=''):
        """
        Downloads multiple files from remote grid storages to 
        a local directory.

        files in the format:
          - { lfn1: guid1, lfn2: guid2, ... }
        """

        shell = getShell(self.middleware)
        if self.lfc_host:
            shell.env['LFC_HOST'] = self.lfc_host
        self.logger.debug('download file with LFC_HOST: %s', shell.env['LFC_HOST'])

        if not dest_dir:
            dest_dir = os.getcwd()
        self.logger.debug('download file to: %s', dest_dir)

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm): 

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj

            def process(self, file):

                lfn  = file[0]
                guid = file[1]
                fname = os.path.basename( urlparse(lfn)[2] )

                cmd  = 'lcg-cp -t %d --vo %s -T %s ' % (self.cacheObj.timeout, self.cacheObj.vo, self.cacheObj.se_type)
                cmd += '%s file://%s/%s' % (guid, dest_dir, fname)
             
                rc,output,m = self.cacheObj.__cmd_retry_loop__(shell, cmd, self.cacheObj.max_try)
             
                if rc != 0:
                    return False 
                else:
                    self.__appendResult__(guid, True)
                    return True

        re_guid = re.compile('^guid:\S+$')

        guids = []
        for lfn, guid in files.iteritems():
            if re_guid.match(guid):
                guids.append([lfn, guid])
            else:
                logger.warning('unknown guid: %s' % f)

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=guids)

        runner = MTRunner(myAlg, myData)
        runner.debug = True
        runner.start()
        runner.join()

        return runner.getResults()

    def delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages. 

        files in the format: 
           - [guid1, guid2, ...]
        """

        shell = getShell(self.middleware)
        if self.lfc_host:
            shell.env['LFC_HOST'] = self.lfc_host
        self.logger.debug('delete file with LFC_HOST: %s', shell.env['LFC_HOST'])

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm): 

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj

            def process(self, guid):

                cmd = 'lcg-del -a -t 60 --vo %s %s' % (self.cacheObj.vo, guid)
             
                rc,output,m = self.cacheObj.__cmd_retry_loop__(shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    return False 
                else:
                    self.__appendResult__(guid, True)
                    return True

        re_guid = re.compile('^guid:\S+$')

        guids = []
        for guid in files:
            if re_guid.match(guid):
                guids.append(guid)
            else:
                logger.warning('unknown guid: %s' % guid)

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=guids)

        runner = MTRunner(myAlg, myData)
        runner.debug = True
        runner.start()
        runner.join()

        return runner.getResults()

    # For GUID protocol
    def __lfc_mkdir__(self, shell, path, mode='775'):
        '''Creates a directory in LFC'''

        cmd = 'lfc-mkdir -p -m %s %s' % (mode, path)

        (rc, output, m) = self.__cmd_retry_loop__(shell, cmd, 1)

        if rc != 0:
            return False
        else:
            return True
