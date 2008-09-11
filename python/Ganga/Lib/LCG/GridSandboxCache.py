###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GridSandboxCache.py,v 1.1 2008-09-11 16:55:19 hclee Exp $
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

class GridSandboxCache(GangaObject):

    '''Helper class for upladong/downloading/deleting sandbox files on a grid cache. 
    '''
 
    _schema = Schema(Version(1,0), {
       'protocol' : SimpleItem(defvalue='', doc='file transfer protocol'),
       'max_try'  : SimpleItem(defvalue=1, doc='max. number of tries in case of failures'),
       'timeout'  : SimpleItem(defvalue=180, doc='transfer timeout in seconds')
    })
 
    _category = 'GridSandboxCache'
    _name = 'GridSandboxCache'
    _exportmethods = ['upload', 'download', 'delete' ] 

    logger = getLogger()

    def __init__(self):
        super(GridSandboxCache,self).__init__()

    def upload(self, files=[], opts=''):
        """
        Uploads multiple files to a remote grid storage.
        """
        raise NotImplementedError

    def download(self, files=[], dest_dir=None, opts=''):
        """
        Downloads multiple files from remote grid storages to 
        a local directory.

        files should be presented in the names desired by the underlying
        implementation.
        """
        raise NotImplementedError

    def delete(self, files=[], opts=''):
        """
        Deletes multiple files from remote grid storages. 

        files should be presented in the names desired by the underlying
        implementation.
        """
        raise NotImplementedError

    ## private methods
    def __uuid__(self,*args):
        ''' Generates a universally unique ID. '''
        t = long( time.time() * 1000 )
        r = long( random.random()*100000000000000000L )
        try:
            a = socket.gethostbyname( socket.gethostname() )
        except:
            # if we can't get a network address, just imagine one
            a = random.random()*100000000000000000L
        data = str(t)+' '+str(r)+' '+str(a)+' '+str(args)
        data = md5.md5(data).hexdigest()
        return data

    def __get_unique_fname__(self):
        '''gets an unique filename'''
        cred  = getCredential('GridProxy',self.middleware)
        uid   = re.sub(r'[\:\-\(\)]{1,}','',cred.identity()).lower()
        fname = 'user.%s.%s' % (uid, self.__uuid__())
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
