###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GridCache.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
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

class GridCache:

    '''File I/O on the GridCache, supporting multiple file transfer protocols'''

    supportedProtocols = ['guid','https','http','gsiftp']
    middleware   = 'EDG'
    # space token is only used when it's meaningful for the protocol
    srmv2_token  = None 
    logger       = getLogger()

    def __init__(self,iocache,vo,shell,middleware='EDG',srmv2_token=None):

        self.middleware  = middleware.upper()
        self.iocache     = iocache
        self.vo          = vo
        self.shell       = shell
        self.srmv2_token = srmv2_token

        # check which protocol to use according to the name of the iocache
        self.protocol = urlparse(self.iocache)[0]
        if self.protocol == '':
            self.protocol = 'guid'

#       check if protocol is supported 
        if not self.protocol in self.supportedProtocols:
            self.logger.warning('protocol %s not supported.' % self.protocol)
            return

    def upload(self,src,maxRetry=3,opts=''):
        '''file upload'''

        if self.protocol == 'guid':
            self.logger.debug('upload file with LFC_HOST: %s',self.shell.env['LFC_HOST'])
            guid = self.__guid_upload__(src,dest=self.iocache,maxRetry=maxRetry)
            return guid
        else:
            ## FIXME!! call the corresponding commands
            dest = os.path.join(self.iocache,os.path.basename(src))
            self.logger.warning('protocol %s not implemented' % self.protocol)
            return dest

    def download(self,src,dest,maxRetry=3,opts=''):
        '''file download'''

        if urlparse(src)[0] != self.protocol:
            self.logger.warning('protocol miss match')
            return False

        if self.protocol == 'guid':
            self.logger.debug('download file with LFC_HOST: %s',self.shell.env['LFC_HOST'])
            src = 'guid:' + src.replace('guid:','')
            return self.__guid_download__(guid=src,localFilePath=dest,maxRetry=maxRetry)
        else:
            ## call the corresponding commands
            self.logger.warning('protocol %s not implemented' % self.protocol)
            return False 

    def delete(self,uri,maxRetry=3,opts=''):
        '''file delete'''

        if urlparse(uri)[0] != self.protocol:
            self.logger.warning('protocol miss match')
            return False

        if self.protocol == 'guid':
            self.logger.debug('delete file with LFC_HOST: %s',self.shell.env['LFC_HOST'])
            uri = 'guid:' + uri.replace('guid:','')
            return self.__guid_delete__(guid=uri,maxRetry=maxRetry)
        else:
            ## call the corresponding commands
            self.logger.warning('protocol %s not implemented' % self.protocol)
            return False 

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

    def __cmd_retry_loop__(self,cmd,maxRetry=3):
        '''Executing system command with retry feature'''
        i      = 0
        rc     = 0
        output = None
        m      = None
        try_again = True
        while try_again:
            i = i + 1
            rc, output, m = self.shell.cmd1(cmd,allowed_exit=[0,255])
            if rc in [0,255]:
                try_again = False
            elif i == maxRetry:
                try_again = False
            else:
                self.logger.warning("trial %d: error: %s" % (i,output))
 
        return (rc, output, m)

    # For GUID protocol
    def __lfc_mkdir__(self, path, mode='775'):
        '''Creates a directory in LFC'''

        cmd = 'lfc-mkdir -p -m %s %s' % (mode, path)

        (rc, output, m) = self.__cmd_retry_loop__(cmd,1)

        if rc != 0:
            return False
        else:
            return True

    def __get_se_info__(self,se):
        '''Gets the path to put data'''

        cmd = 'lcg-info --vo %s --list-se --attrs \'Path,Protocol,SESite\' --query \'SE=%s\' --sed' % (self.vo, se)

        info = {'path':[], 'protocol':[], 'site': None}

        rc,output,m = self.__cmd_retry_loop__(cmd,1)

        if rc == 0 and output:
            (se, paths, protocols, site) = output.split('%')
            info['path']     = paths.split('&')
            info['protocol'] = protocols.split('&')
            info['site']     = site

        return info

    def __guid_upload__(self,localFilePath,dest=None,maxRetry=3):
        '''Upload single file using lcg-cr'''

        ## decide number of parallel stream to be used
        fsize    = os.path.getsize(urlparse(localFilePath)[2])
        nbstream = int((fsize*1.0)/(10.0*1024*1024*1024))

        if nbstream < 1: nbstream = 1 # min stream
        if nbstream > 8: nbstream = 8 # max stream

        cmd = 'lcg-cr -t 180 --vo %s -n %d' % (self.vo, nbstream)
        if dest != None:
            cmd  = cmd + ' -d %s' % dest
        if self.srmv2_token:
            cmd = cmd + ' -T srmv2 -s %s' % self.srmv2_token

        fname = self.__get_unique_fname__()

        ## specify the physical location
        cmd = cmd + ' -P generated/ganga/%s' % fname

        ## specify the logical filename
        ## NOTE: here we assume the root dir for VO is /grid/<voname>
        lfc_dir = '/grid/%s/ganga' % self.vo
        if not self.__lfc_mkdir__(lfc_dir):
            self.logger.warning('cannot create LFC directory: %s' % lfc_dir)
            return None

        cmd = cmd + ' -l %s/%s %s' % (lfc_dir, fname, localFilePath)

        self.logger.debug('data upload cmd: %s' % cmd)
 
        rc,output,m = self.__cmd_retry_loop__(cmd,maxRetry)
 
        if rc != 0:
            return None
        else:
            match = re.search('(guid:\S+)',output)
            if match:
                return match.group(1)
            else:
                return None

    def __guid_download__(self,guid,localFilePath,maxRetry=3):
        '''Download single file'''
 
        cmd = 'lcg-cp -t 60 --vo %s' % self.vo 
        #if self.srmv2_token: 
        #    cmd = cmd + ' -T srmv2 -s %s' % self.srmv2_token
        cmd = cmd + ' %s %s' % (self.vo,guid,localFilePath)
        self.logger.debug('data download cmd: %s' % cmd)
 
        rc,output,m = self.__cmd_retry_loop__(cmd,maxRetry)
 
        if rc != 0:
            return False 
        else:
            return True

    def __guid_delete__(self,guid,maxRetry=3):
        '''Delete all replications corresponding to the given guid'''
 
        cmd = 'lcg-del -a -t 60 --vo %s %s' % (self.vo,guid)
        self.logger.debug('data deletion cmd: %s' % cmd)
 
        rc,output,m = self.__cmd_retry_loop__(cmd,maxRetry)
 
        if rc != 0:
            return False 
        else:
            return True

    # For GRIDFTP protocol
