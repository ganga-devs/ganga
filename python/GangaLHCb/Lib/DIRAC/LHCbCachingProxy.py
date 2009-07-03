#!/usr/bin/env python

import os, sys, tempfile, inspect

class __ProxyFile(object):
    """A marker class for inspect to use"""
    pass

def configPath():
    """Add the gangadir to the PYTHONPATH based on the location of this file"""
    import inspect, os, sys
    
    GangaLHCb = os.path.abspath(os.path.dirname(inspect.getsourcefile(__ProxyFile)))
    GangaDir = os.path.abspath(os.path.join(GangaLHCb,'..','..','..'))
    sys.path.append(GangaDir)
configPath()

from Ganga.Utility.Config import makeConfig
config = makeConfig('Shell','We overwrite the Shell config')
config.addOption('IgnoredVars',[],'This is a dummy so we can use the Shell object')
from Ganga.Utility import Shell

#needs to be global
DEBUG_MODE = False

class DiracProxy(object):
    """Object for handling the dirac-proxy-info command so that the output can be cached so speed things up. """

    def __init__(self, diracVersion, devVersion, cache_file, debug, clean):
        
        self.diracVersion = diracVersion
        self.devVersion = devVersion
        
        self.debug = debug        
        self.shell = None
        self.store = None

        if cache_file is not None:
            self.cache_file = cache_file
        else:
            self.cache_file = self.findCacheFile()

        self.debugMsg('Cache file is %s' % self.cache_file)
        self.readCache(clean)
        self.debugMsg('Environment is %s' % str(self.shell.env))
        
    def debugMsg(self, msg):
        if self.debug:
            print msg

    def findCacheFile(self):
        """Create a name and a full path for the cache file."""
        import os, sys, tempfile
        
        tmp_dir = tempfile.gettempdir()
        
        os_name = sys.platform
        uid = os.getuid()
    
        fileName = '.gangaProxyCache-%s-%d' % (os_name, uid)
    
        if self.diracVersion is not None:
            fileName = '%s-%s' % (fileName,self.diracVersion)
        fileName += '.p' #adds the pickle extension 
            
        cache_file = os.path.join(tmp_dir,fileName)
        return cache_file
    
    def readCache(self, clean):
        """Init the shell object from either the Cache or start again"""
        
        import os
        
        if os.path.exists(self.cache_file) and not clean:
            self.debugMsg("Using the cache file %s" % self.cache_file)
            self.initShellCached()
        else:
            self.debugMsg("Starting a fresh cache")
            self.initShell()
            
    def initShell(self):
        """Set up the shell object using SetupProject"""               

        import os, inspect, sys

        args = []
        if self.devVersion:
            args.append('--dev')
        if self.diracVersion is not None:
            args.append(self.diracVersion)
    
        diracEnvSetup = os.path.join(os.path.dirname(inspect.getsourcefile(DiracProxy)),'setupDiracEnv.sh')
        self.debugMsg('SetupProject command was %s' % diracEnvSetup + ' ' + ' '.join(args))
        self.shell = Shell.Shell(diracEnvSetup,args)
        
        if self.shell.env.get('GANGA_DIRAC_SETUP_STATUS',True):
            try:
                status = int(self.shell.env['GANGA_DIRAC_SETUP_STATUS'])
                if status: raise Exception #just go to printout
            except: #printout if its not a number
                print "Setting up the Dirac client failed. Check your Dirac installation."\
                             " The command 'SetupProject Dirac %s' must work correctly from your command prompt." % self.diracVersion
                sys.exit(1)

        self.initStore()
    
    def initShellCached(self):
        """Setup the shell object using the cached value of the environment unless it is old."""
    
        import pickle, time
        f = file(self.cache_file)
        self.store = pickle.load(f)
        f.close()
    
        #invalidate the cache if older than 3 hours
        if (time.time() - self.store['ENV_TIME']) > 3*3600:
            self.initShell()
        else:
            self.shell = Shell.Shell()
            self.shell.env = self.store['ENV']
    
    def initStore(self):
        """Create a fresh store dictionary"""
        
        import os, time
        
        env = self.shell.env
        t = time.time()
        self.store = {'ENV':env,'TIME':t,'PID':os.getpid(),'ENV_TIME':t}

    def writeCacheFile(self):
        """Get a lock on the cache file and then write to it when you are able"""
        
        import os, stat, time, pickle
        
        def writeLockFile():
            f = file(lock_file,'w')
            pid = os.getpid()
            f.write(str(pid))
            f.close()
            
        def writeCache():

            f = file(self.cache_file,'w')
            pickle.dump(self.store,f)
            f.close()
            
        lock_file = '%s.lock' % self.cache_file
        
        if not os.path.exists(lock_file):#file is not locked
            writeLockFile()
            writeCache()
            os.unlink(lock_file)
        else:#file is locked

            mtime = os.stat(lock_file)[stat.ST_MTIME]
            while True:
                now = time.time()
                #lock is an old lock file
                if (now - mtime) > 10:
                    os.unlink(lock_file)
                    writeCache()
                    return
                else:
                    time.sleep(1)
    
    def parseProxyInfo(self, output):
        """Parse the output of dirac-proxy-info to extract timeleft, etc."""
        
        def handleTimeLeft(line):
            return (3600 * int(line[1])) + (60 * int(line[2])) + int(line[3])
        
        def handleProxyPath(line):
            return line[1]
        
        proxy_path = ''
        time_left = 0 
        
        lines = output.split('\n')
        for line in lines:
            tokens = [t.strip() for t in line.split(':')]
            if tokens and len(tokens) > 1:
                if tokens[0].lower() == 'timeleft':
                    time_left = handleTimeLeft(tokens)
                elif tokens[0].lower() == 'path':
                    proxy_path = handleProxyPath(tokens)
    
        return (proxy_path,time_left)
    
    def proxyInfo(self):
        """Print the proxy info so that it can be read by ganga"""
        
        import os,time
        result = 0
        
        info = None
        
        def checkProxy(path):
            result = 0
            if not os.path.exists(path):
                print 'The proxy file %s does not exist' % path
                if self.store.has_key('INFO'):
                    #clear the output cache
                    del self.store['INFO']
                result = 1
            return result
        
        def findProxyFile():
            """Trys to guess what the proxy file is called."""
            proxy_file = os.environ.get('X509_USER_PROXY',None)
            if proxy_file is None:
                proxy_file = '/tmp/x509up_u%i' % os.getuid()
            return proxy_file
                
        
        def printProxyInfo(output, time_left = None, age = None):
            
            if time_left is None: 
                print output,
                return                
            
            lines = output.split('\n')

            def toTimeString(epoch):
                hours = epoch // 3600
                minutes = (epoch % 3600) // 60
                seconds = ((epoch % 3600) % 60)
                return '%.2i:%.2i:%.2i' % (hours,minutes,seconds)
                
            for line in lines:
                tokens = [t.strip() for t in line.split(':')]
                if tokens and len(tokens) > 1:
                    if tokens[0].lower() == 'timeleft':
                        print '%s%s: %s' % (tokens[0],(' ' * (13 - len(tokens[0]))),toTimeString(time_left))
                    else:
                        print '%s: '.join(tokens) % (' ' * (13 - len(tokens[0])))
            print 'Cache age    : %s' % toTimeString(age)
            print ''
        
        def checkProxyAge(proxy_file, cache_file):
            
            import stat
            result = False
            if os.path.exists(proxy_file) and os.path.exists(cache_file):
                
                mtime_proxy = os.stat(proxy_file)[stat.ST_MTIME]
                mtime_cache = os.stat(cache_file)[stat.ST_MTIME]
                
                #proxy is older than the cache
                if mtime_proxy < mtime_cache:
                    result = True

            return result
        
        def checkEnvironment(self):
            #check that the value of X509_USER_PROXY has not changed
            if self.store and self.store.has_key('ENV'):
                env = self.store.get('ENV',{})
                self.debugMsg("Comparing '%s' and '%s'" % (env.get('X509_USER_PROXY',''),os.environ.get('X509_USER_PROXY','')))
                if env.get('X509_USER_PROXY','') != os.environ.get('X509_USER_PROXY',''):
                    self.readCache(True) #refresh the environment

        
        self.debugMsg("The expected proxy location is '%s'" % findProxyFile())
        checkEnvironment(self) #check the proxy is synched with the environment
        
        #check the age of the proxy
        if not checkProxyAge(findProxyFile(),self.cache_file):
            self.debugMsg("The proxy is newer than the cache, so clearing cache.")
            if self.store.has_key('INFO'):
                #clear the output cache
                del self.store['INFO']
        
        #always invalidate the cache after 10 minutes
        diff = (time.time() - self.store['TIME'])
        if self.store.has_key('INFO') and diff < 600:
            
            #use the cached value
            output = self.store['INFO']
            info = self.parseProxyInfo(output)
            result = checkProxy(info[0])
            if not result:
                delta = info[1] - diff
                if delta < 0:
                    delta = 0
                printProxyInfo(output, delta, diff)
        else:
            self.debugMsg("Running the dirac proxy command")
            
            result,output,m = self.shell.cmd1('dirac-proxy-info --checkvalid')
            self.store['TIME'] = time.time()
            info = self.parseProxyInfo(output)
            if info[0]:#check that output was as expected
                self.store['INFO'] = output
            printProxyInfo(output)
        
        self.debugMsg('Store is %s' % str(self.store))
        self.writeCacheFile()
        return result
    
    
def usage(error = False):
    if error:
        print 'Incorrect Usage: ',
    print '--help [-h] (this msg) --version [-v]= (the dirac version to use) --cache-file [-c] (the cache file) -x (debug) -z (clear cache)'

def main():
    import getopt
    try:
        opts, args = getopt.getopt(sys.argv[1:], "zdxhv:c:", ["dev","help", "version=","cache-file"])
    except getopt.GetoptError:
        # print help information and exit:
        usage(error = True)
        sys.exit(2)
    
    cacheFile = None
    version = None
    devVersion = False
    debug = False
    clean = False
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        if o in ("-v", "--version"):
            version = a
        if o in ("-d", "--dev"):
            devVersion = True
        if o in ("-c", "--cache-file"):
            cacheFile = a
        if o in ("-x"):
            debug = True
        if o in ("-z"):
            clean = True
    #sets the debug mode for exception printing
    global DEBUG_MODE        
    DEBUG_MODE = debug

    #check the proxy using the caching class...
    dirac = DiracProxy(version,devVersion,cacheFile,debug=debug,clean=clean)
    sys.exit(dirac.proxyInfo())


if __name__ == "__main__":
    
    import os
    
    try:
        main()
    except SystemExit, e:
        raise e #just propagate exits
    except Exception, e:
        if DEBUG_MODE:
            print 'There was an error while getting the proxy info:',e.__class__.__name__,e
        #fall back on the wrapper script if something goes wrong
        os.system('lhcb-proxy-info')
    
