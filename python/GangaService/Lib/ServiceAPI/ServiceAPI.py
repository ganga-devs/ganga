import os
import sys
import pickle
import traceback
from socket import *
import time
from commands import getstatusoutput

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

# Ganga Service class that provides the interface to the server
class GangaService:
    
    def __init__(self):
        self.port = -1 #43434
        self.timeout = 60 # in minutes
        self.gangadir = os.path.normpath(os.path.expandvars(os.path.expanduser("~/gangadir-server")))
        self.prerun = ""
        self.gangacmd = "ganga"
        self.userscript = None
        self.userscriptwaittime = 300
        pass

    def getServerInfo(self):
        """return a tuple of the hostname and port from the server.info file"""
        if os.path.exists( os.path.join(self.gangadir, "server", "server.info") ):
            host_port = open( os.path.join(self.gangadir, "server", "server.info") ).read().strip()
            if len(host_port.split(":")) == 1:
                hostname = host_port
                port = self.port
                if port == -1:
                    port = 43434
            else:
                hostname = host_port.split(":")[0]
                port = host_port.split(":")[1]

            return (hostname, port)
        return ("", -1)
        
    def killServer(self):
        """send a terminate request to the server"""
        
        # find out if server is local or remote
        if os.path.exists( os.path.join(self.gangadir, "server", "server.info") ):

            # watchdog file there so check timestamp
            host_port = self.getServerInfo()
            hostname = host_port[0]
            port = int(host_port[1])

            if (time.time() - os.path.getmtime(os.path.join(self.gangadir, "server", "server.info") )) < 60:
                if hostname != os.uname()[1]:
                    logger.info("Active server running on host '%s' - creating kill file" % hostname)
                    open(os.path.join(self.gangadir, "server", "server.kill"), "w").write("")
                    wait = 0
                    while os.path.exists(os.path.join(self.gangadir, "server", "server.info")) and wait < 30:
                        time.sleep(1)
                        wait += 1

                    if wait > 29:
                        logger.info("Could not kill server. Please kill manually on host '%s'" % hostname)
                        return False

                    # wait a little longer for the process to finish completely...
                    time.sleep(10)
                    
                    logger.info("Server kill signals sent though you should check the process has exited on host '%s'." % hostname)
                    return True
                else:
                    # try and connect to this port
                    addr = ('localhost',port)
                    sock = socket(AF_INET,SOCK_STREAM)
                    try:
                        sock.connect(addr)
                    except:
                        logger.info("Could not connect to server on port %d" % port)
                        return False

                    sock.send("###STOP###")
                    data = sock.recv(1024)
                    while data.find("###STOPPED###") == -1:
                        data += sock.recv(1024)
                    sock.close()

                    # check the process has gone away
                    logger.info("Stop signal sent. Waiting for Ganga process to finish...")
                    wait = 0
                    ret, out = getstatusoutput("ps -Af | grep ganga -i | grep %d" % port)
                    while ret == 0 and wait < 120:
                        time.sleep(1)
                        wait += 1
                        ret, out = getstatusoutput('ps -Af | grep ganga -i | grep -v "sh -c" | grep %d' % port)
                        
                    if wait > 119:
                        logger.info("Signals sent but ganga still running. Retry killServer or kill process manually.")
                        return False
                    
                    logger.info("Local server stopped")
                    return True
                                                                                                
            else:
                logger.info("Stale server file around. Removing...")
                os.system("rm %s" % os.path.join(self.gangadir, "server", "server.info"))
                return True
        else:
            logger.info("No Server running")
            return True

    def startServer(self):
        """Start the server if required"""

        # is there a server up? check the watchdog file
        if os.path.exists(os.path.join(self.gangadir, "server", "server.info")):

            host_port = self.getServerInfo()
            hostname = host_port[0]
            self.port = int(host_port[1])

            # watchdog file there so check timestamp
            if (time.time() - os.path.getmtime(os.path.join(self.gangadir, "server", "server.info"))) < 60:
                if hostname != os.uname()[1]:
                    logger.info("Active server running on host '%s'" % hostname)
                    return False
                else:
                    logger.info("Active server running on this machine.")
                    return True
            else:
                logger.info("Stale server file around. Removing...")
                os.system("rm %s" % os.path.join(self.gangadir, "server", "server.info"))

        # try to get an unsued port
        if self.port == -1:
            base_port = 40000
            import random
            random.seed()
            base_num = random.randint(0, 50) * 100
            
            # find any runnning ganga instances
            ret, out = getstatusoutput("ps -Af | grep ganga -i | wc -l")
            proc_offset = int(out) * 5

            # and a bit random for luck
            add_offset = random.randint(0, 4)
            
            # final port number
            self.port = base_port + base_num + proc_offset + add_offset
            logger.info("Trying port %d..." % self.port)

        # No, so start it
        logger.info("Starting server...")
        if self.userscript:
            cmd = "%s --daemon -o[PollThread]forced_shutdown_timeout=300 -o[Configuration]ServerPort=%d -o[Configuration]ServerTimeout=%d -o[Configuration]gangadir=%s -o[Configuration]ServerUserScript=%s -o[Configuration]ServerUserScriptWaitTime=%d %s/server-script.py" % (self.gangacmd, self.port, self.timeout, self.gangadir, 
                                                                                                                                                                                                                                                                                 self.userscript, self.userscriptwaittime,
                                                                                                                                                                                                                                                                                 os.path.join(os.path.dirname( os.path.abspath( __file__ ) ), "../Server" ) )
        else:
            cmd = "%s --daemon -o[PollThread]forced_shutdown_timeout=300 -o[Configuration]ServerPort=%d -o[Configuration]ServerTimeout=%d -o[Configuration]gangadir=%s %s/server-script.py" % (self.gangacmd, self.port, self.timeout, self.gangadir, 
                                                                                                                                                                                               os.path.join(os.path.dirname( os.path.abspath( __file__ ) ), "../Server" ) )
            
        if self.prerun != "":
            cmd = "%s && %s" % (self.prerun, cmd)

        logger.info("\nSettings: ")
        logger.info("  - port:               %d" % self.port)
        logger.info("  - timeout:            %d" % self.timeout)
        logger.info("  - gangadir:           %s" % self.gangadir)
        logger.info("  - prerun:             %s" % self.prerun)
        logger.info("  - gangacmd:           %s" % self.gangacmd)
        if self.userscript:
            logger.info("  - userscript:         %s" % self.userscript)
            logger.info("  - userscriptwaittime: %d" % self.userscriptwaittime)
        else:
            logger.info("  - default user script specified from .gangarc")
        
        logger.info("\nStarting server using command:\n%s" % cmd)
        os.system(cmd)
        
        # wait for the server file to be created
        logger.info("Waiting for server to initialise...")

        time_diff = 0
        while True:
            if os.path.exists(os.path.join(self.gangadir, "server", "server.info")):
                break

            time.sleep(1)
            time_diff += 1
            if time_diff > 30:
                logger.info("Server took too long to start.")
                return False
        
        return True

    def sendCmd(self, cmd):

        # check if server is up
        if not self.startServer():
            logger.info("Could not start the Ganga Server.")
            return ""
        
        # try and connect to this port
        addr = ('localhost',self.port)
        sock = socket(AF_INET,SOCK_STREAM)
        try:
            sock.connect(addr)
        except:
            logger.info("Could not connect to server on port %d" % self.port)
            return ""

        sock.send(cmd + "\n###ENDCMD###")
        logger.info("Command sent. Waiting for output from Ganga...")
        data = sock.recv(1024)
        while data.find("###ENDMSG###") == -1:
            data += sock.recv(1024)
        sock.close()

        return data.replace("###ENDMSG###", "")
