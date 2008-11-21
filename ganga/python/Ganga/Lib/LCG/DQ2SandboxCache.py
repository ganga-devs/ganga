###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2SandboxCache.py,v 1.2 2008-11-03 15:27:18 hclee Exp $
###############################################################################
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   January 2007
import os, os.path, sys, re, tempfile, time, random, md5, shutil
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

dq2_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy().datadict
dq2_file_index_schema_datadict    = GridFileIndex._schema.inherit_copy().datadict

class DQ2FileIndex(GridFileIndex):
    """
    Data object containing DQ2 file index information. 
    
    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    """

    _schema   = Schema( Version(1,0), dq2_file_index_schema_datadict )
    _category = 'GridFileIndex'
    _name = 'DQ2FileIndex'

    def __init__(self, surl, dataset, site, name, md5sum):

        super(DQ2FileIndex,self).__init__()

        self.id     = surl
        self.name   = name
        self.md5sum = md5sum
        self.attributes['dataset'] = dataset
        self.attributes['site']    = site

    def __str__(self):
        return '%s\t%s\t%s\t%s\t%s' % (self.attributes['dataset'], self.attributes['site'], self.name, self.id, self.md5sum)

class DQ2SandboxCache(GridSandboxCache):

    '''Helper class for upladong/downloading/deleting files/datasets using DQ2 libraries.
    '''

    dq2_sandbox_cache_schema_datadict.update({
        'setup'          : SimpleItem(defvalue='/afs/cern.ch/atlas/offline/external/GRID/ddm/DQ2Clients/latest/setup.sh', doc='the DQ2 setup script'),
        'local_site_id'  : SimpleItem(defvalue='CERN-PROD_USERDISK', copyable=0, doc='the DQ2 local site id'),
        #'remote_site_id' : SimpleItem(defvalue='CERN-PROD_USERDISK', copyable=0, doc='the DQ2 remote site id'),
        'dataset_name'   : SimpleItem(defvalue='', copyable=0, doc='the DQ2 dataset name')
        } )

    _schema   = Schema( Version(1,0), dq2_sandbox_cache_schema_datadict )
    _category = 'GridSandboxCache'
    _name = 'DQ2SandboxCache'
 
    logger = getLogger()

    def __init__(self):
        super(DQ2SandboxCache,self).__init__()
        self.protocol = 'dq2'

    def impl_upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.
        """

        shell = getShell(self.middleware)

        ## exclude the Ganga-owned external package for LFC python binding
        pythonpaths = []
        for path in shell.env['PYTHONPATH'].split(':'):
            if not re.match('.*\/external\/lfc\/.*', path):
                pythonpaths.append(path)
        shell.env['PYTHONPATH'] = ':'.join(pythonpaths)

        if self.local_site_id:
            shell.env['DQ2_LOCAL_SITE_ID'] = self.local_site_id

        self.logger.debug('DQ2_LOCAL_SITE_ID: %s' % shell.env['DQ2_LOCAL_SITE_ID'])

        # check dq2 existence
        rc,output,m = self.__cmd_retry_loop__(shell, 'source %s; which dq2-put' % self.setup, 1)
        if (rc != 0) or (not output):
            self.logger.error('dq2 client command not found. Please check DQ2 client installation.')
            return None

        # move all local files in the temporary directory
        src_dir = tempfile.mkdtemp(prefix='_ganga_tmp_')
        finfo   = {}
        myuuid  = get_uuid()
        for f in files:
            path  = os.path.dirname( urlparse(f)[2] )
            name  = os.path.basename( urlparse(f)[2] )

            tmp_fname = name + '_' + myuuid
            tmp_fpath = os.path.join(src_dir, tmp_fname )

            shutil.copy(os.path.join(path, name), tmp_fpath)
            md5sum = get_md5sum(tmp_fpath)
            fsize  = os.path.getsize( tmp_fpath )
            finfo[tmp_fname] = {}
            finfo[tmp_fname]['md5sum'] = md5sum
            finfo[tmp_fname]['fsize']  = fsize
            finfo[tmp_fname]['local_fpath']  = urlparse(f)[2]

        # compose dq2-put command 
        cmd = 'source %s; dq2-put -a -d ' % (self.setup)

        if self.local_site_id:
            cmd += '-L %s ' % self.local_site_id

        cmd += '-s %s ' % src_dir
        cmd += '-f %s ' % ','.join(finfo.keys())
        cmd += '%s ' % self.dataset_name

        # run dq2-put
        rc,output,m = self.__cmd_retry_loop__(shell, cmd, self.max_try)

        self.logger.debug('%d %s' % (rc, output))

        # run dq2-ls to query the uploaded files
        # together with local file information, creates GridFileIndex objects
        file_idx = []
        if rc == 0:
            # compose dq2-ls command 
            cmd = 'source %s; dq2-ls -L %s -f -p %s' % (self.setup, self.local_site_id, self.dataset_name)
            rc,output,m = self.__cmd_retry_loop__(shell, cmd, self.max_try)

            if rc == 0:

                lines = map( lambda x:x.strip(), output.split() )
             
                for l in lines:
                    for f in finfo.keys():
                        if l.find('srm://') == 0 and l.find(f) > 0:
                            finfo[f]['surl'] = l
                            break
             
                for f in finfo.keys():
                    name   = os.path.basename( finfo[f]['local_fpath'] )
                    surl   = finfo[f]['surl']
                    md5sum = finfo[f]['md5sum']
                    file_idx.append( DQ2FileIndex(surl=surl, name=name, dataset=self.dataset_name, site=self.local_site_id, md5sum=md5sum) )

        ## removing the temporary directory in any case
        shutil.rmtree(src_dir)

        return file_idx

#    def impl_download(self, files={}, dest_dir=None, opts=''):
#        """
#        Downloads multiple files from remote grid storages to 
#        a local directory.
#        """
#
#        return False

#    def impl_delete(self, files=[], opts=''):
#        """
#        Deletes multiple files from remote grid storages. 
#        """
#
#        return False

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
                    dataset = info[0]
                    site    = info[1]
                    name    = info[2]
                    surl    = info[3]
                    md5sum  = info[4]

                    files.append(DQ2FileIndex(surl=surl, dataset=dataset, site=site, name=name, md5sum=md5sum))
                except IndexError, e:
                    pass

        return files 
