## Action script to be run in Ganga as a daemon
## picks up new actions and then creates the Task required

## NO MONITORING AT PRESENT!

from __future__ import print_function

import os
import time
from commands import getstatusoutput
nec_file = ".gpytho"
work_dir = "/clusterhome/home/protopop"
wait_time = 60

# ensure the float is high enough
for t in tasks:
    t.float = 100000
    if t.status == "pause":
        t.run()
        
# load from the necessary file
nec_str = open(os.path.join( work_dir, nec_file )).read().strip().strip('#')
mysqlc = "mysql -hhughnon.ppe.gla.ac.uk -ugridbot -p%s -s GridJobs" % nec_str

while not os.path.exists( os.path.join( work_dir, "GangaRun", "ganga.stop" ) ):

    # check for halt in submission
    if os.path.exists( os.path.join( work_dir, "GangaRun", "ganga.halt" ) ):
        for t in tasks:
            t.pause()

        while (os.path.exists( os.path.join( work_dir, "GangaRun", "ganga.halt" ) )):
            time.sleep(wait_time)
            
        for t in tasks:
            t.run()
        
    # wait
    time.sleep(wait_time)
    
    # now look for our own
    for ln in os.listdir( os.path.join( work_dir, "actions" ) ):
        if ln.find("ganga-action") != -1:

            # check for other actions first - odd numbered clone/submit actions means to wait
            rc, out = getstatusoutput("echo \"SELECT COUNT(*) FROM events WHERE attr2 LIKE 'c%%-action-2%%';\" | %s" % mysqlc)
            num_clone = int(out)
            rc, out = getstatusoutput("echo \"SELECT COUNT(*) FROM events WHERE attr2 LIKE 's%%-action-2%%';\" | %s" % mysqlc)
            num_submit = int(out)
            
            if (num_clone % 2 == 1) or (num_submit % 2 == 1):
                print("num. Clone (%d), Num. Submit (%d). Pending requests. Waiting." % (num_clone, num_submit))
                continue

            # we've found an action so run it
            print("Found action. Creating Task.")
            task_str = open( os.path.join( work_dir, "actions", ln ) ).read()
            t = NA62Task()
            t.initFromString(task_str, LCG())
            t.float = 100000
            t.run()

            # shift the action
            print("Moving action file to done")
            os.system("mv %s %s" % (os.path.join( work_dir, "actions", ln ), os.path.join( work_dir, "actions", "done", ln )))

            # update the DB
            rc, out = getstatusoutput("echo \"INSERT INTO events (type, attr1, attr2, attr3) VALUES ('action', 'ganga', '%s', '%d')\" | %s" %
                                      (ln, t.transforms[0].application.num_events * t.transforms[0].num_jobs, mysqlc))

            if (rc != 0):
                print("Error updating DB: %s" % out)
                
            
            
                          
                          
