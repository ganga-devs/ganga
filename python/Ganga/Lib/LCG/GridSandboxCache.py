###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GridSandboxCache.py,v 1.10 2009-07-16 10:41:17 hclee Exp $
###############################################################################
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   January 2007
import re

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Credentials import getCredential

from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG.Utility import get_uuid

from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Effects


class GridFileIndex(GangaObject):

    '''
    Data object for indexing a file on the grid. 

    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    '''

    _schema = Schema(Version(1, 0), {
        'id': SimpleItem(defvalue='', doc='the main identity of the file'),
        'name': SimpleItem(defvalue='', doc='the name of the file'),
        'md5sum': SimpleItem(defvalue='', doc='the md5sum of the file'),
        'attributes': SimpleItem(defvalue={}, doc='a key:value pairs of file metadata')
    })

    _category = 'GridFileIndex'
    _name = 'GridFileIndex'

    logger = getLogger()

    def __init__(self):
        super(GridFileIndex, self).__init__()

    def __eq__(self, other):
        return other.id == self.id


class GridSandboxCache(GangaObject):

    '''
    Helper class for upladong/downloading/deleting sandbox files on a grid cache. 

    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    '''

    _schema = Schema(Version(1, 1), {
        'vo': SimpleItem(defvalue='dteam', hidden=1, copyable=0, doc='the Grid virtual organization'),
        'middleware': SimpleItem(defvalue='EDG', hidden=1, copyable=1, doc='the LCG middleware type'),
        'protocol': SimpleItem(defvalue='', copyable=1, doc='file transfer protocol'),
        'max_try': SimpleItem(defvalue=1, doc='max. number of tries in case of failures'),
        'timeout': SimpleItem(defvalue=180, copyable=0, hidden=1, doc='transfer timeout in seconds'),
        'uploaded_files': ComponentItem('GridFileIndex', defvalue=[], sequence=1, protected=1, copyable=0, hidden=1, doc='a repository record for the uploaded files')
    })

    _category = 'GridSandboxCache'
    _name = 'GridSandboxCache'
    _exportmethods = ['upload', 'download', 'delete',
                      'get_cached_files', 'list_cached_files', 'cleanup']

    logger = getLogger()

    def __init__(self):
        super(GridSandboxCache, self).__init__()

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
            if getName(f) == 'File':
                paths.append('file://%s' % f.name)
            elif getName(f) == 'str':
                paths.append('file://%s' % f)
            else:
                self.logger.warning('unknown file expression: %s' % repr(f))

        uploaded_files = self.impl_upload(files=paths, opts=opts)

        if len(uploaded_files) == len(files):
            status = self.impl_bookkeepUploadedFiles(
                uploaded_files, append=True, opts=opts)
        else:
            status = False

        if len(uploaded_files) == len(files):
            status = self.impl_bookkeepUploadedFiles(
                uploaded_files, append=True, opts=opts)
        else:
            status = False

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
        status = False
        myFiles = self.__get_file_index_objects__(files)
        downloadedFiles = self.impl_download(
            files=myFiles, dest_dir=dest_dir, opts=opts)

        if len(downloadedFiles) == len(myFiles):
            status = True
        else:
            self.logger.warning('some files not successfully downloaded')

        return status

    def delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages.

        @param files is a list of files to be deleted from the grid.
               The data format of it should be:
               - [index_grid_file_1, index_grid_file_2, ...]

        @return True if files are successfully deleted; otherwise it returns False
        """
        status = False
        myFiles = self.__get_file_index_objects__(files)
        deletedFiles = self.impl_delete(files=myFiles, opts=opts)

        if len(deletedFiles) == len(myFiles):
            status = True
        else:
            self.logger.warning('some files not successfully deleted')

        return status

    def cleanup(self, opts=''):
        """
        Cleans up the uploaded files.

        @return True if all grid files listed in the index file are successfully deleted.
        """
        status = False

        all_files = self.get_cached_files()

        f_ids = []
        for f in all_files:
            f_ids.append(f.id)

        return self.delete(files=f_ids)

    def get_cached_files(self, opts=''):
        """
        Gets the indexes of the uploaded files on the grid. 

        @return the dictionary indexing the uploaded files on the grid.
                The key of the dictionary should be the main index (e.g. GUID) of the grid files.
        """
        return self.impl_getUploadedFiles(opts=opts)

    def list_cached_files(self, loop=True, opts=''):
        """
        Lists the uploaded files.

        if loop = True, it prints also the uploaded files associated with subjobs.
        """

        fc = 0
        ds = ''

        doColoring = True

        fg = Foreground()
        fx = Effects()

        status_colors = {'inuse': fg.orange,
                         'free': fg.blue,
                         'gone': fg.red}

        status_mapping = {'new': 'inuse',
                          'submitted': 'inuse',
                          'submitting': 'inuse',
                          'running': 'inuse',
                          'completed': 'free',
                          'completing': 'free',
                          'failed': 'free',
                          'killed': 'free'}

        if doColoring:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        def __markup_by_status__(fileIndex, counter, status):

            fmtStr = '\n%4d\t%-30s\t%-12s\t%s' % (
                counter, fileIndex.name, status, fileIndex.id)

            try:
                return markup(fmtStr, status_colors[status])
            except KeyError:
                return markup(fmtStr, fx.normal)

        j = self.getJobObject()

        for f in self.get_cached_files(opts=opts):

            my_status = 'unknown'

            if j:
                try:
                    my_status = status_mapping[j.status]
                except KeyError:
                    pass

            ds += __markup_by_status__(f, fc, my_status)

            fc += 1

        if j and loop:
            for sj in j.subjobs:
                for f in sj.backend.sandboxcache.get_cached_files(opts=opts):

                    my_status = 'unknown'

                    try:
                        my_status = status_mapping[sj.status]
                    except KeyError:
                        pass

                    ds += __markup_by_status__(f, fc, my_status)

                    fc += 1

        return ds

    # methods to be implemented in the child classes
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
        It simply keeps the GridFileIndex objects in the job repository.

        @param files is a list of files represented by GridFileIndex objects 
        @return True if files are successfully logged in the local index file 
        """

        self.uploaded_files = files

        return True

    def impl_getUploadedFiles(self, opts=''):
        """
        basic implementation for getting the previously uploaded files from the
        job repository.

        @return a list of files represented by GridFileIndex objects
        """
        files = self.uploaded_files

        return files

    # private methods
    def __get_file_index_objects__(self, files=[]):
        '''Gets file index object according to the given file list
             - try to get the GridFileIndex object from the local index file.  

        @param files is a list of file indexes
        @return a list of files represented by GridFileIndex objects
        '''

        cachedFiles = self.get_cached_files()
        myFiles = []
        for f in cachedFiles:
            if f.id in files:
                myFiles.append(f)

        return myFiles

    def __get_unique_fname__(self):
        '''gets an unique filename'''
        cred = getCredential('GridProxy', self.middleware)
        uid = re.sub(r'[\:\-\(\)]{1,}', '', cred.identity()).lower()
        fname = 'user.%s.%s' % (uid, get_uuid())
        return fname

    def __cmd_retry_loop__(self, shell, cmd, maxRetry=3):
        '''Executing system command with retry feature'''
        i = 0
        rc = 0
        output = None
        m = None
        try_again = True
        while try_again:
            i = i + 1
            self.logger.debug('run cmd: %s' % cmd)
            rc, output, m = shell.cmd1(cmd, allowed_exit=[0, 255])
            if rc in [0, 255]:
                try_again = False
            elif i == maxRetry:
                try_again = False
            else:
                self.logger.warning("trial %d: error: %s" % (i, output))

        return (rc, output, m)
