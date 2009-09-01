#!/usr/bin/python

import os, sys

_basedir = ''

def checkInstall():
    # attempt to run Fluka
    os.chdir(_basedir)
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/fluka_test.inp -O %s/fluka_test.inp" % _basedir):
        print "Error: Unable to download fluka test script"
        sys.exit(6)
    return os.system("export FLUKA=%s/fluka ; export FLUPRO=$FLUKA ; $FLUKA/flutil/rfluka -M 1 -N 0 fluka_test" % os.environ['VO_NA48_SW_DIR'])
    

print "-----------------------------------------"
print "\nFLUKA Installer for NA48\n"
print "-----------------------------------------"

# debug
print "Current contents of NA48 SW dir:"
os.system("ls -ltr " + os.environ['VO_NA48_SW_DIR'] )
_basedir = os.getcwd()

# check if there's already an install
bad_install = False
if "fluka" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):

    # previous install found. Wait in case someone else is messing
    import popen2, time
    print "Checking for other processes..."
    cmd = "du -s %s | cut -f1" % os.environ['VO_NA48_SW_DIR']
    stdout, stderr = popen2.popen2( cmd )
    prev_mem = eval(stdout.read())
    print "Found previous install of fluka (%d). Waiting..." % prev_mem
    time.sleep(5*60)
    
    stdout, stderr = popen2.popen2( cmd )
    curr_mem = eval(stdout.read())
    if curr_mem != prev_mem:
        print "Error: Difference in du results (%d -> %d). Exiting." % (prev_mem, curr_mem)
        sys.exit(5)

    print "Nothing changing so checking installation..."

    if checkInstall():
        bad_install = True
else:
    bad_install = True

if bad_install == True:
    
    if "fluka" in os.listdir( os.environ['VO_NA48_SW_DIR'] ):
        os.system("rm -r %s/fluka" % os.environ['VO_NA48_SW_DIR'])
        
    # make the directory
    if os.system("mkdir -p " + os.environ['VO_NA48_SW_DIR'] + '/fluka'):
        print "Error creating fluka directory"
        sys.exit(5)

    # download the tarball
    if os.system("wget http://epweb2.ph.bham.ac.uk/user/slater/na48/software/fluka.tar.gz -O %s/fluka/fluka.tar.gz" % os.environ['VO_NA48_SW_DIR']):
        print "Error: Unable to download fluka tar ball"
        sys.exit(6)

    # untar
    os.chdir(os.environ['VO_NA48_SW_DIR'] + '/fluka')
    if os.system("tar -zxf fluka.tar.gz"):
        print "Error: Unable to untar fluka tar ball."
        sys.exit(3)

    # build
    if os.system("export FLUPRO=$PWD ; make"):
        print "Error: Could not build FLUKA"
        sys.exit(4)

    checkInstall()
