from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Credentials import getCredential

from Ganga.Utility.Config import getConfig, ConfigError

from Ganga.Utility.GridShell import getShell

from Ganga.Lib.LCG.GridSandboxCache import GridSandboxCache, GridFileIndex
from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm

gridftp_sandbox_cache_schema_datadict = GridSandboxCache._schema.inherit_copy().datadict
gridftp_file_index_schema_datadict    = GridFileIndex._schema.inherit_copy().datadict

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

    _schema   = Schema( Version(1,0), gridftp_file_index_schema_datadict )
    _category = 'GridFileIndex'
    _name = 'GridftpFileIndex'

    def __init__(self):
        super(GridftpFileIndex,self).__init__()

class GridftpSandboxCache(GridSandboxCache):
    '''
    Helper class for upladong/downloading/deleting sandbox files using lcg-cp/lcg-del commands with gsiftp protocol.

    @author: Hurng-Chun Lee
    @contact: hurngchunlee@gmail.com
    '''

    lcg_sandbox_cache_schema_datadict.update({
        'baseURI' : SimpleItem(defvalue='', copyable=1, doc='the base URI for storing cached files')
    })

    _schema   = Schema( Version(1,0), gridftp_sandbox_cache_schema_datadict )
    _category = 'GridSandboxCache'
    _name = 'GridftpSandboxCache'

    logger = getLogger()

    def __init__(self):
        super(GridftpSandboxCache,self).__init__()

    def impl_upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote gridftp server.
        """

        shell = getShell(self.middleware)

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

                md5sum   = get_md5sum(fpath, ignoreGzipTimestamp=True)
                nbstream = int((fsize*1.0)/(10.0*1024*1024*1024))

                if nbstream < 1: nbstream = 1 # min stream
                if nbstream > 8: nbstream = 8 # max stream

                destURI = '%s/%s' % (baseURI, fname)

                cmd = 'lcg-cp -t 180 --vo %s -n %d %s %s' % (self.cacheObj.vo, nbstream, fpath, destURI)

                rc,output,m = self.cacheObj.__cmd_retry_loop__(shell, cmd, self.cacheObj.max_try)

                if rc != 0:
                    self.cacheObj.logger.error(output)
                    return False
                else:
                    fidx = GridftpFileIndex()
                    fidx.id = destURI
                    fidx.name = fname
                    fidx.md5sum = md5sum
                    fidx.attributes['fpath'] = fpath

                    self.__appendResult__( file, fidx )
                    return True

        myAlg  = MyAlgorithm(cacheObj=self)
        myData = Data(collection=files)

        runner = MTRunner(name='sandboxcache_gridftp_upload', algorithm=myAlg, data=myData)
        runner.start()
        runner.join(-1)

        return runner.getResults().values()