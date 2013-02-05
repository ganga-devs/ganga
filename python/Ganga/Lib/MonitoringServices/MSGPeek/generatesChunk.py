#!/usr/bin/env python

import random, string

""" 
Write files with different sizes in order to check if all the messages sent are
the same message received"
Steps:
    1. Run the collector:
        python "PATH/install/ganga/python/Ganga/Lib/MonitoringServices/MSGMSPeek/Ganga-coll-queue.py"
    2. Loggin into Ganga
    3. Submit a job when the script is the file
        j = Job()
        j.application = Executable(exe=File("PATH/install/ganga/python/Ganga/Lib/MonitoringServices/MSGMSPeek/generatesChunk.py"), args='' )
        j.submit()
            Note: Keep in mind (or write) the nombre of the job submitted
    4. In order to run the program to compare the messages sent and received change in "compare.py":
        a = "PATH/install/ganga/python/Ganga/Lib/MonitoringServices/MSGMSPeek/GANGA-MSG-collector-queue.log"
        b = "~/gangadir/workspace/mchamber/LocalAMGA/NUMBERJOB/output/stdout"
            When NUMBERJOB is the number of the jobs submitted
    5. Run the comparator program
        python "PATH/install/ganga/python/Ganga/Lib/MonitoringServices/MSGMSPeek/comparator.py"
    6. If you don't rely on comparator program use the diff command:
        diff "PATH/install/ganga/python/Ganga/Lib/MonitoringServices/MSGMSPeek/GANGA-MSG-collector-queue.log" "~/gangadir/workspace/mchamber/LocalAMGA/NUMBERJOB/output/stdout" 

Size  between 4KB and 8KB
minLines  = 40
maxLines = 90

Size  between 40KB and 80KB
minLines  = 400
maxLines = 900

Size  between 400KB and 800KB
minLines  = 4000
maxLines = 9000

Size  between 4MB and 8MB 
minLines  = 40000
maxLines = 90000

Size  between 40MB and 80MB 
minLines  = 400000
maxLines = 900000
"""

minLines  = 400
maxLines = 900
n_lines =  random.randint(minLines, maxLines)
f = open ("chunk",'w')
i=0

import sys
while i < int(sys.argv[1]) :
    i += 1 
    s  = ''.join([random.choice(string.letters+string.digits) for j in range(random.randint(30,128))] )
    f.write("Line %d: %s\n" %(i, s))
    print "Line %d: %s" %(i, s)
    if i % 500 == 0 :
        import time
        time.sleep(5)
f.close()
     
#import stomp
#class CompareListener(stomp.ConnectionListener):
#    """
#        This class is a ConnectionListener class used in stompy library
#        its function is storage all the messages in a file that have been 
#        send using the stompy library. Two connections could be used, one to
#        send and anothe to receive, but one is enough 
#    """       
#    def __init__(self):
#        self.errors = 0
#        self.connections = 0
#        self.messages = 0
#        self.f = open("chunk", 'r')
#        self.f.seek(0)
#        self.error = False 
#        
#    def on_disconnected(self):
#        self.f.close()
#        if not self.error :
#            print " There have been no errors"
#        
#    def on_error(self, headers, message):
#        print('received an error in the message%s' % message)
#        self.errors = self.errors + 1
#
#    def on_connecting(self, host_and_port):
#        print('connecting %s %s' % host_and_port)
#        self.connections = self.connections + 1
#
#    def on_message(self, headers, message):
#        self.messages = self.messages + 1
#        line = self.f.readline()
#        print message ,line 
##        if (line <> message) :
##            import sys
##            print "failed in line %d" %self.messages
##            print >> sys.error,  "in line %d there is an error" %self.messages
##            error = True
##            
#
#        
#conn = stomp.Connection([('dashb-mb.cern.ch', 6163)])
#listener = CompareListener()
#conn.add_listener(listener)
#conn.start()
#conn.connect(wait=True)
#conn.subscribe(destination='/queue/mchamber.chunk123')
#
#
#f = open("chunk", 'r')
#line = f.readline()
#while line and conn.is_connected():
#    conn.send(destination="/queue/mchamber.chunk123", message=line)
#    line = f.readline()
#f.close()
#
#conn.disconnect()
#
#import time
#time.sleep(3)


