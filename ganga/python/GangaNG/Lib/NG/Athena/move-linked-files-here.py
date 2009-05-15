#! /usr/bin/python

import os, dircache

def getCommandOutput2(command):
    child = os.popen(command)
    data = child.read()
    err = child.close()
    if err:
        raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data

d = dircache.listdir(".")

for f in d:
    print "MOVE_LINKS: Checking %s" % f
    if os.path.islink(f):
        if f.find(".root") > -1:
            print "   *** MOVING %s HERE ***" % f
            path = os.path.realpath(f)
            os.unlink(f)
            os.system("cp %s ." % path)




