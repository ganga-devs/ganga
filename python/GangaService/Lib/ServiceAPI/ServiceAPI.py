import os
import sys
import pickle
import traceback
from socket import *
import time

# Ganga Service class that provides the interface to the server
class GangaService:
    
    def __init__(self):
        self.port = 434343
        self.timeout = 60
        self.gangadir = os.path.normpath(os.path.expandvars(os.path.expanduser("~/gangadir-server")))
        self.prerun = ""
        self.gangacmd = "ganga"
        pass

    def killServer(self):
        """send a terminate request to the server"""
        
        # find out if server is local or remote
        if os.path.exists( os.path.join(self.gangadir, "server", "server.info") ):

            # watchdog file there so check timestamp
            hostname = open( os.path.join(self.gangadir, "server", "server.info") ).read().strip()
            if (time.time() - os.path.getmtime(os.path.join(self.gangadir, "server", "server.info") )) < 60:
                if hostname != os.uname()[1]:
                    print "Active server running on host '%s' - creating kill file" % hostname
                    open(os.path.join(self.gangadir, "server", "server.kill"), "w").write("")
                    wait = 0
                    while os.path.exists(os.path.join(self.gangadir, "server", "server.info")) and wait < 30:
                        time.sleep(1)
                        wait += 1

                    if wait > 29:
                        print "Could not kill server. Please kill manually on host '%'" % hostname
                        return False

                    print "Server killed."
                    return True
                else:
                    # try and connect to this port
                    addr = ('localhost',self.port)
                    sock = socket(AF_INET,SOCK_STREAM)
                    try:
                        sock.connect(addr)
                    except:
                        print "Could not connect to server on port %d" % self.port
                        return False

                    sock.send("###STOP###")
                    data = sock.recv(1024)
                    while data.find("###STOPPED###") == -1:
                        data += sock.recv(1024)
                    sock.close()
                    print "Local server stopped"
                    return True
                                                                                                
            else:
                print "Stale server file around. Removing..."
                os.system("rm %s" % os.path.join(self.gangadir, "server", "server.info"))
                return True
        else:
             print "No Server running"
             return True

    def startServer(self):
        """Start the server if required"""

        # is there a server up? check the watchdog file
        if os.path.exists(os.path.join(self.gangadir, "server", "server.info")):

            # watchdog file there so check timestamp
            hostname = open(os.path.join(self.gangadir, "server", "server.info")).read().strip()
            if (time.time() - os.path.getmtime(os.path.join(self.gangadir, "server", "server.info"))) < 60:
                if hostname != os.uname()[1]:
                    print "Active server running on host '%s'" % hostname
                    return False
                else:
                    print "Active server running on this machine."
                    return True
            else:
                print "Stale server file around. Removing..."
                os.system("rm %s" % os.path.join(self.gangadir, "server", "server.info"))
                
        # No, so start it
        print "Starting server..."
        cmd = "%s --daemon -o[PollThread]forced_shutdown_timeout=300 -o[Configuration]ServerPort=%d -o[Configuration]ServerTimeout=%d -o[Configuration]gangadir=%s %s/server-script.py" % (self.gangacmd, self.port, self.timeout, self.gangadir,
                                                                                                                                                 os.path.join(os.path.dirname( os.path.abspath( __file__ ) ), "../Server" ) )
        if self.prerun != "":
            cmd = "%s && %s" % (self.prerun, cmd)

        print "Starting server using command:\n%s" % cmd
        os.system(cmd)
        
        # wait for the server file to be created
        print "Waiting for server to initialise..."

        time_diff = 0
        while True:
            if os.path.exists(os.path.join(self.gangadir, "server", "server.info")):
                break

            time.sleep(1)
            time_diff += 1
            if time_diff > 30:
                print "Server took too long to start."
                return False
        
        return True

    def sendCmd(self, cmd):

        # check if server is up
        if not self.startServer():
            print "Could not start the Ganga Server."
            return ""
        
        # try and connect to this port
        addr = ('localhost',self.port)
        sock = socket(AF_INET,SOCK_STREAM)
        try:
            sock.connect(addr)
        except:
            print "Could not connect to server on port %d" % self.port
            return ""

        sock.send(cmd + "\n###ENDCMD###")
        print "Command sent. Waiting for output from Ganga..."
        data = sock.recv(1024)
        while data.find("###ENDMSG###") == -1:
            data += sock.recv(1024)
        sock.close()

        return data.replace("###ENDMSG###", "")
