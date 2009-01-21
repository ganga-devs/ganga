###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GridSandboxCache.py,v 1.4 2008-11-05 13:51:57 hclee Exp $
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
from Ganga.Lib.LCG.Utility import *

class GridFileIndex(GangaObject):

    '''Data object for indexing a file on the grid. 
    '''
 
    _schema = Schema(Version(1,0), {
       'id'         : SimpleItem(defvalue='', doc='the main identity of the file'),
       'name'       : SimpleItem(defvalue='', doc='the name of the file'),
       'md5sum'     : SimpleItem(defvalue='', doc='the md5sum of the file'),
       'attributes' : SimpleItem(defvalue={}, doc='a key:value pairs of file metadata')
    })
 
    _category = 'GridFileIndex'
    _name = 'GridFileIndex'

    logger = getLogger()

    def __init__(self):
        super(GridFileIndex,self).__init__()

    def __str__(self):
        my_dict = self.attributes
        my_dict['id'] = self.id

        return rep(my_dict)

    def __eq__(self, other):
        return other.id == self.id

class GridSandboxCache(GangaObject):

    '''Helper class for upladong/downloading/deleting sandbox files on a grid cache. 
    '''
 
    _schema = Schema(Version(1,0), {
       'vo'         : SimpleItem(defvalue='dteam', hidden=1, copyable=0, doc='the Grid virtual organization'),
       'middleware' : SimpleItem(defvalue='EDG', hidden=1, copyable=1, doc='the LCG middleware type'),
       'protocol'   : SimpleItem(defvalue='', doc='file transfer protocol'),
       'max_try'    : SimpleItem(defvalue=1, doc='max. number of tries in case of failures'),
       'timeout'    : SimpleItem(defvalue=180, copyable=0, hidden=1, doc='transfer timeout in seconds'),
       'index_file' : SimpleItem(defvalue='', copyable=0, hidden=1, doc='the file for keepping the index of files on the grid')
    })
 
    _category = 'GridSandboxCache'
    _name = 'GridSandboxCache'
    _exportmethods = ['upload', 'download', 'delete', 'get_uploaded_files', 'cleanup' ] 

    logger = getLogger()

    def __init__(self):
        super(GridSandboxCache,self).__init__()

    def upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.
        
        @param files is a list of local files to be uploaded to the grid.
               The elemement can be a file path or a File object.

        @return True if files are successfully uploaded; otherwise it returns False
        """
        status = False

        paths = []
        for f in files:
            if f.__class__.__name__ == 'File':
                paths.append('file://%s' % f.name)
            elif f.__class__.__name__ == 'str':
                paths.append('file://%s' % f)
            else:
                logger.warning('unknown file expression: %s' % repr(f))

        ## check or create the index file for local bookkeeping
        if not self.index_file:
            self.index_file = tempfile.mkstemp(suffix='.idx', prefix='_ganga_grid_sandbox_')[1]

        uploaded_files = self.impl_upload(files=paths, opts=opts)
        status = self.impl_bookkeepUploadedFiles(uploaded_files, append=True, opts=opts)

        return status

    def download(self, files=[], dest_dir=None, opts=''):
        """
        Downloads multiple files from remote grid storages to 
        a local directory.

        If the file is successfully downloaded, the local file path would be:

            - os.path.join(dest_dir, os.path.basename(local_fname_n)

        @param files is a list of files to be downloaded from the grid.
               The data format of it should be:
               - [index_grid_file_1, index_grid_file_2, ...]

        @param dest_dir is a local destination directory to store the downloaded files.
        
        @return True if files are successfully downloaded; otherwise it returns False
        """
        status  = False
        myFiles = self.__get_file_index_objects(files)
        downloadedFiles = self.impl_download(files=myFiles, dest_dir=dest_dir, opts=opts)

        if len(downloadedFiles) == len(myFiles):
            status = True
        else:
            logger.warning('some files not successfully downloaded')

        return status

    def delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages.

        @param files is a list of files to be deleted from the grid.
               The data format of it should be:
               - [index_grid_file_1, index_grid_file_2, ...]

        @return True if files are successfully deleted; otherwise it returns False
        """
        status  = False
        myFiles = self.__get_file_index_objects(files)
        deletedFiles = self.impl_delete(files=myFiles, opts=opts)

        if len(deletedFiles) == len(myFiles):
            status = True
        else:
            logger.warning('some files not successfully deleted')

        return status

    def cleanup(self, opts=''):
        """
        Cleans up the uploaded files.

        @return True if all grid files listed in the index file are successfully deleted.
        """
        status = False

        all_files = self.get_uploaded_files()

        f_ids = []
        for f in all_files:
            f_ids.append(f.id)

        return self.delete(files=f_ids)

    def get_uploaded_files(self, opts=''):
        """
        Gets the indexes of the uploaded files on the grid. 

        @return the dictionary indexing the uploaded files on the grid.
                The key of the dictionary should be the main index (e.g. GUID) of the grid files.
        """
        return self.impl_parseIndexFile(opts=opts)

    ## methods to be implemented in the child classes 
    def impl_upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.

        @param files is a list of files in URL format (i.e. file://...)

        @return a list of successfully uploaded files represented by GridFileIndex objects
        """
        raise NotImplementedError

    def impl_download(self, files=[], dest_dir=None, opts=''):
        """
        Downloads multiple files from remote grid storages to 
        a local directory.

        @param files is a list of files represented by GridFileIndex objects 
        @param dest_dir is a local destination directory to store the downloaded files.

        @return a list of successfully downloaded files represented by GridFileIndex objects
        """
        raise NotImplementedError

    def impl_delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages. 

        @param files is a list of files represented by GridFileIndex objects 
        @return a list of successfully deleted files represented by GridFileIndex objects
        """
        raise NotImplementedError

    def impl_bookkeepUploadedFiles(self, files=[], append=True, opts=''):
        """
        basic implementation for bookkeeping the uploaded files.
        It simply writes out the given GridFileIndex objects. 

        @param files is a list of files represented by GridFileIndex objects 
        @return True if files are successfully logged in the local index file 
        """

        fmode = 'w'
        if append:
            fmode = 'a'

        f_idx = open(self.index_file, fmode)
        for f in files:
            f_idx.write( '%s\n' % f )
        f_idx.close()

        return True

    def impl_parseIndexFile(self, opts=''):
        """
        implementation for parsing the index file used for bookkeeping the uploaded files

        @return a list of files represented by GridFileIndex objects
        """
        raise NotImplementedError

    ## private methods
    def __get_file_index_objects(self, files=[]):
        '''Gets file index object according to the given file list
             - try to get the GridFileIndex object from the local index file.  

        @param files is a list of file indexes
        @return a list of files represented by GridFileIndex objects
        '''

        cachedFiles = self.get_uploaded_files()
        myFiles = []
        for f in cachedFiles:
            if f.id in files:
                myFiles.append(f)

        return myFiles

    def __get_unique_fname__(self):
        '''gets an unique filename'''
        cred  = getCredential('GridProxy',self.middleware)
        uid   = re.sub(r'[\:\-\(\)]{1,}','',cred.identity()).lower()
        fname = 'user.%s.%s' % (uid, get_uuid())
        return fname

    def __cmd_retry_loop__(self,shell,cmd,maxRetry=3):
        '''Executing system command with retry feature'''
        i      = 0
        rc     = 0
        output = None
        m      = None
        try_again = True
        while try_again:
            i = i + 1
            self.logger.debug('run cmd: %s' % cmd)
            rc, output, m = shell.cmd1(cmd,allowed_exit=[0,255])
            if rc in [0,255]:
                try_again = False
            elif i == maxRetry:
                try_again = False
            else:
                self.logger.warning("trial %d: error: %s" % (i,output))
 
        return (rc, output, m)
