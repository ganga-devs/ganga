#!/usr/bin/env python

import sys,os,time

print 'running the internal process ... forking'

if len(sys.argv)>1:
    pid = os.fork()

    if pid == 0:
        print 'started child...'
        while 1:
            time.sleep(5)
    else:
        print 'parent: forked',pid
        f = file(sys.argv[1]+'/proc_stat','w')
        f.write(str(pid))
        f.close()
	os.wait()
