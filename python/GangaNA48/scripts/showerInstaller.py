#!/usr/bin/python

import os, sys

print "-----------------------------------------"
print "\nShower Library Installer for NA48\n"
print "-----------------------------------------"

# debug
print "Current contents of NA48 SW dir:"
os.system("df -h " + os.environ['VO_NA48_SW_DIR'] )
os.system("du -h " + os.environ['VO_NA48_SW_DIR'] )
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )


# previous install found. Wait in case someone else is messing
import popen2, time
print "Checking for other processes..."
cmd = "du -s %s | cut -f1" % os.environ['VO_NA48_SW_DIR']
stdout, stderr = popen2.popen2( cmd )
prev_mem = eval(stdout.read())
time.sleep(5*60)

stdout, stderr = popen2.popen2( cmd )
curr_mem = eval(stdout.read())
if curr_mem != prev_mem:
    print "Error: Difference in du results (%d -> %d). Exiting." % (prev_mem, curr_mem)
    sys.exit(5)
else:
    print "Nothing changing so now checking shower_lib..."

# check if there's already an install
bad_install = False
os.system("rm -r %s/shower_lib.tar.gz*" % os.environ['VO_NA48_SW_DIR'])
if "shower_lib" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # do a file list and check everything matches
    flist = {}
    for fname in os.listdir( os.environ['VO_NA48_SW_DIR'] + '/shower_lib' ):
        flist[fname] = os.path.getsize(os.environ['VO_NA48_SW_DIR'] + '/shower_lib/' + fname)

    # now compare with 'good' list
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/shower_lib_list.txt -O %s/shower_lib_list.txt" % os.environ['VO_NA48_SW_DIR']):
        print "Error: Unable to download shower_lib file list"
        sys.exit(6)
        
    f = open(os.environ['VO_NA48_SW_DIR'] + '/shower_lib_list.txt', 'r')
    print os.listdir( os.environ['VO_NA48_SW_DIR'] )
    for ln in f.readlines():
        if len(ln.split()) > 8:
            fname = ln.split()[8]
            size = eval(ln.split()[4])
            if not fname in flist.keys() or flist[fname] != size:
                print "Warning: file lists/sizes do not match (" + fname + "). Reinstalling."
                bad_install = True
                break
            else:
                print fname + " OK."
                            
    f.close()
    os.system("rm " + os.environ['VO_NA48_SW_DIR'] + "/shower_lib_list.txt*")
    
# remove any dodgy installs
if bad_install:
    os.system("rm -r " + os.environ['VO_NA48_SW_DIR'] + "/shower_lib*")
    
# check for current install
if bad_install or not "shower_lib" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # shower_lib not present - first check we can create things
    if os.system("touch " + os.environ['VO_NA48_SW_DIR'] + "/shower_lib"):
        print "Error: Unable to create shower_lib directory"
        sys.exit(1)
    
    # now we'll download it
    os.chdir(os.environ['VO_NA48_SW_DIR'])
    os.system("rm -r shower_lib*")
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/shower_lib.tar.gz"):
        print "Error: Unable to download shower_lib tar ball"
        sys.exit(2)

    # and finally untar it
    if os.system("tar -zxf shower_lib.tar.gz"):
        print "Error: Unable to untar shower_lib tar ball."
        sys.exit(3)

    # delete the tar ball
    if os.system("rm shower_lib.tar.gz*"):
        print "Error: Unable to delete the shower_lib tar ball."
        sys.exit(4)

print "End contents of NA48 shower_lib dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] + "/shower_lib" )

sys.exit(0)


