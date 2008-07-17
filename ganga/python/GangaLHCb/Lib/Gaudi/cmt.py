""" 
  This script allows you to run Gaudi Applications from python and use
"""

#--- CMT interface

import os, sys, string
import tempfile
import shutil
import atexit
import warnings
from threading import Thread
import Ganga.Utility.logging
from Ganga.Utility.Shell import Shell
logger = Ganga.Utility.logging.getLogger()

class cmt:
    def __init__(self,shell):
        self.shell=shell

        self.uses=[]
        self.tmppath   = tempfile.mktemp()
        self.tmpcmtdir = os.path.join(self.tmppath,'cmttemp','v1','cmt')
        self.reqfname  = os.path.join(self.tmpcmtdir,'requirements')
        self.environ = {}
        if self.shell.env.has_key('CMTPATH'):
           logger.debug('cashed CMTPATH: %s',shell.env['CMTPATH']) 
           self.cmtpath=shell.env['CMTPATH'].split(os.pathsep)
        else:
           self.cmtpath=''
        if self.shell.env.has_key('CMTROOT'):
           self.cmtroot=shell.env['CMTROOT']
        else:
           self.cmtroot=''
        if self.shell.env.has_key('CMTBIN'):
           self.cmtbin=shell.env['CMTBIN']
        else:
           self.cmtbin=''
        self.cmtcmd='cmt'
        if self.cmtbin and self.cmtroot:
           self.cmtcmd=self.cmtroot+os.sep+self.cmtbin+os.sep+'cmt'
           logger.debug("Cmt command: %s",self.cmtcmd)


    def use(self,package,version='*',path=''):
            elements = package.split('/')
            if len(elements) == 1 :
                pac = elements[0]
                pat = path
            else :
                pac = elements[-1]
                if( path != '' ) :
                    pat = path+'/'+'/'.join(elements[:-1])
                else :
                    pat = '/'.join(elements[:-1])
            self.uses.append((pac,version,pat))
       
    
    def _setup(self):
        return
        if not os.path.exists(self.tmpcmtdir):
            os.makedirs(self.tmpcmtdir) 
        reqfile = open(self.reqfname,'w')
        for use in self.uses :
            reqfile.write('use '+use[0]+' '+use[1]+' '+use[2]+'\n')
        reqfile.close()
        # execute cmt setup
        command = self.cmtcmd + ' setup -bat -quiet -pack=cmttemp -version=v1 -path='+ self.tmppath
        #ret=self.shell.cmd1(command)
        logger.debug("self.shell.env['CMTPATH']: %s",self.shell.env['CMTPATH'])

        ret=self.shell.cmd1("cmt show uses")

        for line in ret[1].splitlines():
            env = line[4:line.find('=')]
            val = line[line.find('=')+1:]
            if env : self.environ[env] = val.replace('\\', os.sep)
        for key in self.environ.keys() :
            # replace all occurences of %...% by their value
            while self.environ[key].find('%') != -1 :
                value = self.environ[key]
                var = value[value.find('%')+1:value.find('%',value.find('%')+1)]
                if self.environ.has_key(var) :   val = self.environ[var]
                else :
                    val = self.shell.env[var]
                    if not val : val = ''
                environ[key] = value.replace('%'+var+'%',val)
            # set the environment
            # os.putenv(key,environ[key])
            self.shell.env[key]=self.environ[key]
        while 1:
            old_env = self.shell.env.copy()
            for key in self.environ.keys():
                self.shell.env[key] = os.path.expandvars(self.environ[key])
            if old_env == self.shell.env:
                break
        self.cleanup()


    def setup(self):
        _t=Thread(target=self._setup)
        _t.start()
        _t.join()


    def _showuses(self):
        if not os.path.exists(self.tmpcmtdir):
            os.makedirs(self.tmpcmtdir)
        reqfile = open(self.reqfname,'w')
        logger.debug("self.uses: %s", str(self.uses))
        for use in self.uses :
            reqfile.write('use '+use[0]+' '+use[1]+' '+use[2]+'\n')
        reqfile.close()
        logger.debug("self.shell.env['CMTPATH']: %s",self.shell.env['CMTPATH'])
        command = self.cmtcmd + ' show uses -pack=cmttemp -version=v1 -path='+ self.tmppath
        usedict = {}
        ret=self.shell.cmd1(command)
        logger.debug("%s: %s",command, ret)
        for line in ret[1].splitlines():
            if( line[0] != '#' ) :
                elem =  string.split(line[:-1])
                if len(elem) <= 3 : continue
                if elem[3][0] != '(' and len(elem) > 4:
                    pack = elem[3]+'/'+elem[1]
                    path = elem[4]
                else :
                    pack = elem[1]
                    path = elem[3]
                path = path.lstrip( "(" ).rstrip( ")" )
                path = path.rstrip( "/" )
                usedict[pack] = (elem[2], path)
        self.cleanup()
        import pprint
        logger.debug(pprint.pformat(usedict))
        return usedict

    #-----------------------------------------------------------------------------
    def showuses(self) :
    #-----------------------------------------------------------------------------    
        usedict = self._showuses()
        packs = usedict.keys()
        packs.sort()
        for p in packs :
            print '%-20s %-8s %s' % ( p, usedict[p][0], usedict[p][1] )


    #-----------------------------------------------------------------------------
    def cleanup(self) :
    #-----------------------------------------------------------------------------
        if os.path.exists(self.tmppath) :
            shutil.rmtree(self.tmppath)

    def __del__(self):
        self.cleanup()



#
#
##--- Module variables
#if (os.getenv('CMTPATH')):
#  cmtpath = os.getenv('CMTPATH').split(os.pathsep)
#else:
#  cmtpath = ""
#cmtroot = os.getenv('CMTROOT')
#cmtbin  = os.getenv('CMTBIN')
#cmtcmd  = 'cmt'
#if cmtbin and cmtroot :
#    cmtcmd = cmtroot + os.sep + cmtbin + os.sep + 'cmt'
#
#warnings.filterwarnings('ignore', 'tempnam', RuntimeWarning)
#tmppath   = tempfile.mktemp()
#tmpcmtdir = os.path.join(tmppath,'cmttemp','v1','cmt')
#reqfname  = os.path.join(tmpcmtdir,'requirements')
#uses    = []
#environ = {}
#
##-----------------------------------------------------------------------------
#def cleanup() :
##-----------------------------------------------------------------------------
#    if os.path.exists(tmppath) :
#        shutil.rmtree(tmppath)
#    
#atexit.register(cleanup)
#
##-----------------------------------------------------------------------------
#def use( package, version='*',path='') :
##-----------------------------------------------------------------------------
#    elements = package.split('/')
#    if len(elements) == 1 :
#        pac = elements[0]
#        pat = path
#    else :
#        pac = elements[-1]
#        if( path != '' ) :
#            pat = path+'/'+'/'.join(elements[:-1])
#        else :
#            pat = '/'.join(elements[:-1])
#    uses.append((pac,version,pat))
#
##-----------------------------------------------------------------------------
#def _setup():
##-----------------------------------------------------------------------------
#    global cmtcmd
#    if not os.path.exists(tmpcmtdir):
#        os.makedirs(tmpcmtdir) 
#    reqfile = open(reqfname,'w')
#    for use in uses :
#        reqfile.write('use '+use[0]+' '+use[1]+' '+use[2]+'\n')
#    reqfile.close()
#    # execute cmt setup
#    command = cmtcmd + ' setup -bat -quiet -pack=cmttemp -version=v1 -path='+ tmppath
#    for line in os.popen(command).readlines():
#        env = line[4:line.find('=')]
#        val = line[line.find('=')+1:-1]
#        if env : environ[env] = val.replace('\\', os.sep)
#    for key in environ.keys() :
#        # replace all occurences of %...% by their value
#        while environ[key].find('%') != -1 :
#            value = environ[key]
#            var = value[value.find('%')+1:value.find('%',value.find('%')+1)]
#            if environ.has_key(var) :
#                val = environ[var]
#            else :
#                val = os.getenv(var)
#                if not val : val = ''
#            environ[key] = value.replace('%'+var+'%',val)
#        # set the environment
#        # os.putenv(key,environ[key])
#        os.environ[key] = environ[key]
#        
#    while 1:
#        old_env = os.environ.copy()
#        for key in environ.keys():
#            os.environ[key] = os.path.expandvars(environ[key])
#        if old_env == os.environ:
#            break
#
#    if( sys.platform == 'win32') :
#        pp = environ.get('PATH','')
#        if pp:
#            pp = pp + os.pathsep
#        pp = pp + environ.get('LD_LIBRARY_PATH', '')
#        if pp:
#            os.putenv(pp)
#    if 'PYTHONPATH' in environ.keys() :
#        sys.path += environ['PYTHONPATH'].split(os.pathsep)
#    # re-calculate basic variables
#    if (os.getenv('CMTPATH')):
#      cmtpath = os.getenv('CMTPATH').split(os.pathsep)
#    else:
#      cmtpath = ""
#    cmtroot = os.getenv('CMTROOT')
#    cmtbin  = os.getenv('CMTBIN')
#    if cmtbin and cmtroot : cmtcmd = cmtroot + os.sep + cmtbin + os.sep + 'cmt'
#    cleanup()
#
#def setup():
#   _t = Thread( target = _setup )
#   _t.start()
#   _t.join()
#   
#
##-----------------------------------------------------------------------------
#def _showuses() :
##-----------------------------------------------------------------------------
#    if not os.path.exists(tmpcmtdir):
#        os.makedirs(tmpcmtdir)
#    reqfile = open(reqfname,'w')
#    for use in uses :
#        reqfile.write('use '+use[0]+' '+use[1]+' '+use[2]+'\n')
#    reqfile.close()
#    command = cmtcmd + ' show uses -pack=cmttemp -version=v1 -path='+ tmppath
#    usedict = {}
#    for line in os.popen(command).readlines():
#        if( line[0] != '#' ) :
#            elem =  string.split(line[:-1])
#            if len(elem) <= 3 : continue
#            if elem[3][0] != '(' and len(elem) > 4:
#                pack = elem[3]+'/'+elem[1]
#                path = elem[4]
#            else :
#                pack = elem[1]
#                path = elem[3]
#            usedict[pack] = (elem[2], path)
#    cleanup()
#    return usedict
# 
##-----------------------------------------------------------------------------
#def showuses() :
##-----------------------------------------------------------------------------    
#    usedict = _showuses()
#    packs = usedict.keys()
#    packs.sort()
#    for p in packs :
#        print '%-20s %-8s %s' % ( p, usedict[p][0], usedict[p][1] )
