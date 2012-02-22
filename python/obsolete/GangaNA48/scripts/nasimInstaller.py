#!/usr/bin/python

import os, sys

print "-----------------------------------------"
print "\nNASIM Installer for NA48\n"
print "-----------------------------------------"

# debug
print "Current contents of NA48 SW dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )

# check if there's already an install
bad_install = False
if "nasim" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # previous install found. Wait in case someone else is messing
    import popen2, time
    print "Checking for other processes..."
    cmd = "du -s %s | cut -f1" % os.environ['VO_NA48_SW_DIR']
    stdout, stderr = popen2.popen2( cmd )
    prev_mem = eval(stdout.read())
    print "Found previous install of nasim (%d). Waiting..." % prev_mem
    time.sleep(5*60)
    
    stdout, stderr = popen2.popen2( cmd )
    curr_mem = eval(stdout.read())
    if curr_mem != prev_mem:
        print "Error: Difference in du results (%d -> %d). Exiting." % (prev_mem, curr_mem)
        sys.exit(5)
    else:
        print "Nothing changing so now checking file sizes..."
        
    # do a file list and check everything matches
    flist = {}
    for root, dirs, files in os.walk(os.path.join(os.environ['VO_NA48_SW_DIR'], 'nasim')):
        for file in files:
            flist[os.path.join(root, file).replace(os.environ['VO_NA48_SW_DIR'] + '/', '')] = os.path.getsize( os.path.join(root, file) )


    for key in flist:
        print key + "   " + str(flist[key])

    # now compare with 'good' list
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/nasim_list.txt -O %s/nasim_list.txt" % os.environ['VO_NA48_SW_DIR']):
        print "Error: Unable to download nasim file list"
        sys.exit(6)
        
    f = open(os.environ['VO_NA48_SW_DIR'] + '/nasim_list.txt', 'r')
    print os.listdir( os.environ['VO_NA48_SW_DIR'] )
    for ln in f.readlines():
        fname = ln.split()[0]
        size = eval(ln.split()[1])
        if not fname in flist.keys() or flist[fname] != size:
            print "Warning: file lists/sizes do not match (" + fname + "). Reinstalling."
            bad_install = True
            break
        else:
            print fname + " OK."
                            
    f.close()
    os.system("rm " + os.environ['VO_NA48_SW_DIR'] + "/nasim_list.txt*")
    
# remove any dodgy installs
if bad_install:
    os.system("rm -r " + os.environ['VO_NA48_SW_DIR'] + "/nasim*")
    
# check for current install
if bad_install or not "nasim" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # nasim not present - first check we can create things
    if os.system("touch " + os.environ['VO_NA48_SW_DIR'] + "/nasim"):
        print "Error: Unable to create nasim directory"
        sys.exit(1)
    
    # now we'll download it
    os.chdir(os.environ['VO_NA48_SW_DIR'])
    os.system("rm -r nasim")
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/nasim.tar.gz"):
        print "Error: Unable to download nasim tar ball"
        sys.exit(2)

    # and finally untar it
    if os.system("tar -zxf nasim.tar.gz"):
        print "Error: Unable to untar nasim tar ball."
        sys.exit(3)

    # delete the tar ball
    if os.system("rm nasim.tar.gz*"):
        print "Error: Unable to delete the nasim tar ball."
        sys.exit(4)

# Let's see what we've got...
print "End contents of NA48 SW dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )

print "End contents of NA48 nasim dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] + "/nasim" )

sys.exit(0)


