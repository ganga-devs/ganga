###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2SandboxCache.py,v 1.9 2009-06-09 07:54:42 hclee Exp $
###############################################################################
#
# ATLAS/ARDA
#
# Date:   January 2007
import os
import os.path
import re
import tempfile
import shutil
from types import *
from urlparse import urlparse

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.Utility.logging import getLogger
from Ganga.Utility.GridShell import getShell 

from Ganga.Lib.LCG.GridSandboxCache import GridSandboxCache, GridFileIndex
from Ganga.Lib.LCG.Utility import *

dq2_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy().datadict
dq2_file_index_schema_datadict    = GridFileIndex._schema.inherit_copy().datadict

class DQ2FileIndex(GridFileIndex):
    """
    Data object containing DQ2 file index information. 
    
    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    """
    dq2_file_index_schema_datadict.update({
        'dataset'        : SimpleItem(defvalue='', doc='the DQ2 dataset name'),
        'site'           : SimpleItem(defvalue='', doc='the DQ2 site id')
        } )

    _schema   = Schema( Version(1,0), dq2_file_index_schema_datadict )
    _category = 'GridFileIndex'
    _name = 'DQ2FileIndex'

    def __init__(self):
        super(DQ2FileIndex,self).__init__()

#    def __init__(self, surl, dataset, site, name, md5sum):
#
#        self.__init__()
#
#        self.id     = surl
#        self.name   = name
#        self.md5sum = md5sum
#        self.attributes['dataset'] = dataset
#        self.attributes['site']    = site

class DQ2SandboxCache(GridSandboxCache):

    '''
    Helper class for upladong/downloading/deleting files/datasets using DQ2 libraries.

    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    '''

    dq2_sandbox_cache_schema_datadict.update({
        'setup'          : SimpleItem(defvalue='/afs/cern.ch/atlas/offline/external/GRID/ddm/DQ2Clients/setup.sh', doc='the DQ2 setup script'),
        'local_site_id'  : SimpleItem(defvalue='CERN-PROD_SCRATCHDISK', copyable=1, doc='the DQ2 local site id'),
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
        rc,output,m = self.__cmd_retry_loop__(shell, 'source %s 2>&1 > /dev/null; which dq2-put' % self.setup, 1)
        if (rc != 0) or (not output):
            self.logger.error('dq2 client command not found. Please check DQ2 client installation.')
            return None

        # move all local files in the temporary directory
        src_dir = tempfile.mkdtemp(prefix='_ganga_tmp_')
        finfo   = {}
        myuuid  = get_uuid()

        try:
            for f in files:
                path  = os.path.dirname( urlparse(f)[2] )
                name  = os.path.basename( urlparse(f)[2] )

                tmp_fname = name + '_' + myuuid
                tmp_fpath = os.path.join(src_dir, tmp_fname )

                shutil.copy(os.path.join(path, name), tmp_fpath)
                md5sum = get_md5sum(os.path.join(path, name), ignoreGzipTimestamp=True)
                fsize  = os.path.getsize( tmp_fpath )
                finfo[tmp_fname] = {}
                finfo[tmp_fname]['md5sum'] = md5sum
                finfo[tmp_fname]['fsize']  = fsize
                finfo[tmp_fname]['local_fpath']  = urlparse(f)[2]
                finfo[tmp_fname]['surl'] = ''

            # compose dq2-put command
            cmd = 'source %s 2>&1 > /dev/null; export VOMS_PROXY_INFO_DONT_VERIFY_AC=1; dq2-put -a -d -C ' % (self.setup)

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
                cmd = 'source %s 2>&1 > /dev/null; export VOMS_PROXY_INFO_DONT_VERIFY_AC=1; dq2-ls -L %s -f -p %s' % (self.setup, self.local_site_id, self.dataset_name)
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

                        ## create DQ2FileIndex of the uploaded file
                        fidx = DQ2FileIndex()
                        fidx.id = surl
                        fidx.name = name
                        fidx.dataset = self.dataset_name
                        fidx.site = self.local_site_id
                        fidx.md5sum = md5sum

                        file_idx.append( fidx )
        finally:
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
#        Deletes whole dataset from the sit
#        """
#
#        ## ToDo: a better implementation
#        ##   1. get list of SURLs from the dataset
#        ##   2. if the given files is a subset - remove files with lcg-del and call dq2-check-replica-consistency
#        ##   3. if the given files covers all files in dataset - call dq2-delete-replicas
#
#
#        isDone = False
#
#        shell = getShell(self.middleware)
#
#        cmd = 'source %s; dq2-delete-replicas -d %s %s' % (self.setup, self.dataset_name, self.local_site_id)
#
#        rc,output,m = self.__cmd_retry_loop__(shell, cmd, self.max_try)
#
#        logger.debug(output)
#
#        if rc == 0:
#            isDone = True
#
#        return files
