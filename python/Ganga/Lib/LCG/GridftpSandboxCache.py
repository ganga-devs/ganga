import os
import os.path
import re
from urlparse import urlparse

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.GridShell import getShell

from Ganga.Lib.LCG.GridSandboxCache import GridSandboxCache, GridFileIndex
from Ganga.Lib.LCG.Utility import urisplit, get_md5sum
from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm

gridftp_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy(
).datadict
gridftp_file_index_schema_datadict = GridFileIndex._schema.inherit_copy(
).datadict


from Ganga.Utility.logging import getLogger


class GridftpFileIndex(GridFileIndex):

    """
    Data object containing Gridftp file index information.

        - id: gsiftp URI
        - name: basename of the file
        - md5sum: md5 checksum
        - attributes['fpath']: path of the file on local machine

    @author: Hurng-Chun Lee
    @contact: hurngchunlee@gmail.com
    """

    _schema = Schema(Version(1, 0), gridftp_file_index_schema_datadict)
    _category = 'GridFileIndex'
    _name = 'GridftpFileIndex'

    def __init__(self):
        super(GridftpFileIndex, self).__init__()


class GridftpSandboxCache(GridSandboxCache):

    '''
    Helper class for upladong/downloading/deleting sandbox files using lcg-cp/lcg-del commands with gsiftp protocol.

    @author: Hurng-Chun Lee
    @contact: hurngchunlee@gmail.com
    '''

    gridftp_sandbox_cache_schema_datadict.update({
        'baseURI': SimpleItem(defvalue='', copyable=1, doc='the base URI for storing cached files')
    })

    _schema = Schema(Version(1, 0), gridftp_sandbox_cache_schema_datadict)
    _category = 'GridSandboxCache'
    _name = 'GridftpSandboxCache'

    logger = getLogger()

    def __init__(self):
        super(GridftpSandboxCache, self).__init__()
        self.protocol = 'gsiftp'

    def impl_upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote gridftp server.
        """

        shell = getShell(self.middleware)

        # making the directory on remove storage at destURI
        dirname = self.__get_unique_fname__()

        # creating subdirectory

        dir_ok = False

        destURI = '%s/%s' % (self.baseURI, dirname)

        uri_info = urisplit(destURI)

        cmd = 'uberftp %s "cd %s"' % (uri_info[1], uri_info[2])

        rc, output, m = self.__cmd_retry_loop__(shell, cmd, 1)

        if rc != 0:

            for l in output.split('\n'):
                l.strip()
                if re.match(r'^550.*', l):
                    # the directory is not found (error code 550), try to creat
                    # the lowest level one
                    cmd = 'uberftp %s "mkdir %s"' % (uri_info[1], uri_info[2])

                    rc, output, m = self.__cmd_retry_loop__(shell, cmd, 1)

                    if rc != 0:
                        self.logger.error(output)
                    else:
                        dir_ok = True

                    break
        else:
            self.logger.debug(
                'parent directory already available: %s' % destURI)
            dir_ok = True

        if not dir_ok:
            self.logger.error('parent directory not available: %s' % destURI)
            return []

        # the algorithm of uploading one file
        class MyAlgorithm(Algorithm):

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj

            def process(self, file):
                # decide number of parallel stream to be used
                fsize = os.path.getsize(urlparse(file)[2])
                fname = os.path.basename(urlparse(file)[2])
                fpath = os.path.abspath(urlparse(file)[2])

                md5sum = get_md5sum(fpath, ignoreGzipTimestamp=True)
                nbstream = int((fsize * 1.0) / (10.0 * 1024 * 1024 * 1024))

                if nbstream < 1:
                    nbstream = 1  # min stream
                if nbstream > 8:
                    nbstream = 8  # max stream

                myDestURI = '%s/%s' % (destURI, fname)

                # uploading the file
                cmd = 'uberftp'
                if nbstream > 1:
                    cmd += ' -c %d' % nbstream

                cmd += ' file:%s %s' % (fpath, myDestURI)

                rc, output, m = self.cacheObj.__cmd_retry_loop__(
                    shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    self.cacheObj.logger.error(output)
                    return False
                else:
                    fidx = GridftpFileIndex()
                    fidx.id = myDestURI
                    fidx.name = fname
                    fidx.md5sum = md5sum
                    fidx.attributes['fpath'] = fpath

                    self.__appendResult__(file, fidx)
                    return True

        myAlg = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(
            name='sandboxcache_gridftp', algorithm=myAlg, data=myData)
        runner.start()
        runner.join(-1)

        return runner.getResults().values()

    def impl_download(self, files=[], dest_dir=None, opts=''):
        """
        Downloads multiple files from gridftp server to 
        a local directory.
        """
        if not dest_dir:
            dest_dir = os.getcwd()
        self.logger.debug('download file to: %s', dest_dir)

        shell = getShell(self.middleware)

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm):

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj

            def process(self, file):

                srcURI = file.id
                fname = os.path.basename(urisplit(srcURI)[2])
                destURI = 'file:%s/%s' % (dest_dir, fname)

                #cmd  = 'uberftp %s %s' % (srcURI, destURI)
                cmd = 'globus-url-copy %s %s' % (srcURI, destURI)

                rc, output, m = self.cacheObj.__cmd_retry_loop__(
                    shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    self.cacheObj.logger.error(output)
                    return False
                else:
                    self.__appendResult__(file.id, file)
                    return True

        myAlg = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(
            name='sandboxcache_gridftp', algorithm=myAlg, data=myData)
        runner.start()
        runner.join(-1)

        return runner.getResults().values()

    def impl_delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote gridftp server
        """

        shell = getShell(self.middleware)

        # the algorithm of downloading one file to a local directory
        class MyAlgorithm(Algorithm):

            def __init__(self, cacheObj):
                Algorithm.__init__(self)
                self.cacheObj = cacheObj

            def process(self, file):

                destURI = file.id

                uri_info = urisplit(destURI)

                cmd = 'uberftp %s "rm %s"' % (uri_info[1], uri_info[2])

                rc, output, m = self.cacheObj.__cmd_retry_loop__(
                    shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    self.cacheObj.logger.error(output)
                    return False
                else:
                    self.__appendResult__(file.id, file)
                    return True

        myAlg = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(
            name='sandboxcache_lcgdel', algorithm=myAlg, data=myData)
        runner.start()
        runner.join(-1)

        # update the local index file
        del_files = runner.getResults().values()
        all_files = self.get_cached_files()

        left_files = []
        for f in all_files:
            if f not in del_files:
                left_files.append(f)

        self.impl_bookkeepUploadedFiles(left_files, append=False)

        return del_files
