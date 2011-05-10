#!/usr/bin/python

import os, sys

print "-----------------------------------------"
print "\nHEPDB Installer for NA48\n"
print "-----------------------------------------"

# debug
print "Current contents of NA48 SW dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )

# check if there's already an install
bad_install = False
if "hepdb" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # previous install found. Wait in case someone else is messing
    import popen2, time
    print "Checking for other processes..."
    cmd = "du -s %s | cut -f1" % os.environ['VO_NA48_SW_DIR']
    stdout, stderr = popen2.popen2( cmd )
    prev_mem = eval(stdout.read())
    print "Found previous install of hepdb (%d). Waiting..." % prev_mem
    time.sleep(5*60)
    
    stdout, stderr = popen2.popen2( cmd )
    curr_mem = eval(stdout.read())
    if curr_mem != prev_mem:
        print "Error: Difference in du results (%d -> %d). Exiting." % (prev_mem, curr_mem)
        sys.exit(5)
    else:
        print "Nothing changing so now checking hepdb sizes..."
        
    # do a file list and check everything matches
    flist = {}
    for fname in os.listdir( os.environ['VO_NA48_SW_DIR'] + '/hepdb' ):
        flist[fname] = os.path.getsize(os.environ['VO_NA48_SW_DIR'] + '/hepdb/' + fname)

    # now compare with 'good' list
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/hepdb_list.txt -O %s/hepdb_list.txt" % os.environ['VO_NA48_SW_DIR']):
        print "Error: Unable to download hepdb file list"
        sys.exit(6)
        
    f = open(os.environ['VO_NA48_SW_DIR'] + '/hepdb_list.txt', 'r')
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
    os.system("rm " + os.environ['VO_NA48_SW_DIR'] + "/hepdb_list.txt*")
    
# remove any dodgy installs
if bad_install:
    os.system("rm -r " + os.environ['VO_NA48_SW_DIR'] + "/hepdb*")
    
# check for current install
if bad_install or not "hepdb" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # hepdb not present - first check we can create things
    if os.system("touch " + os.environ['VO_NA48_SW_DIR'] + "/hepdb"):
        print "Error: Unable to create hepdb directory"
        sys.exit(1)
    
    # now we'll download it
    os.chdir(os.environ['VO_NA48_SW_DIR'])
    os.system("rm -r hepdb")
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/hepdb.tar.gz"):
        print "Error: Unable to download hepdb tar ball"
        sys.exit(2)

    # and finally untar it
    if os.system("tar -zxf hepdb.tar.gz"):
        print "Error: Unable to untar hepdb tar ball."
        sys.exit(3)

    # delete the tar ball
    if os.system("rm hepdb.tar.gz*"):
        print "Error: Unable to delete the hepdb tar ball."
        sys.exit(4)

# Make alterations to appropriate files
import string

print "Making alterations to hepdb.names..."
text_in = open(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names.old", "r").read()
text_out = string.replace(text_in, "VO_NA48_SW_DIR", os.environ['VO_NA48_SW_DIR'] + "/")

print "-----------------------------__"
print text_in
print "-----------------------------__"
print text_out
print "-----------------------------__"

if os.path.exists(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names"):
    text_comp = open(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names", "r").read()
    print text_comp
    print "-----------------------------__"
    if text_comp != text_out:
        print "Warning: Previous hepdb.names found with errors. Replacing."
        open(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names", "w").write(text_out)
else:
    open(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names", "w").write(text_out)
    
print "Contents of hepdb.names:"
print "-----------------------------------------------"
print open(os.environ['VO_NA48_SW_DIR'] + "/hepdb/hepdb.names", "r").read()
print "-----------------------------------------------"

# Let's see what we've got...
print "End contents of NA48 SW dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )

print "End contents of NA48 hepdb dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] + "/hepdb" )

sys.exit(0)


