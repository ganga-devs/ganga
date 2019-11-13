#!/usr/bin/python
import os
import requests
import commands
import shutil
msg = ""

# put in a massive try..except block so I can get some feedback
try:
    # Request the list of releases
    resp = requests.get('https://api.github.com/repos/ganga-devs/ganga/releases')
    if resp.status_code == 200:
        # assume the first is the latest
        rel_ver = resp.json()[0]['name']

        # Now check if it's installed
        if rel_ver not in  os.listdir('/cvmfs/ganga.cern.ch/Ganga/install/'):
            # we need to install it
            msg = commands.getstatusoutput("/home/cvganga/ganga-cvmfs-install.sh %s" % rel_ver)[1]
            shutil.copy2('/cvmfs/ganga.cern.ch/Ganga/install/%s/lib/python3.6/site-packages/ganga/GangaRelease/tools/ganga-cvmfs-install.sh' % rel_ver, '~/')
            shutil.copy2('/cvmfs/ganga.cern.ch/Ganga/install/%s/lib/python3.6/site-packages/ganga/GangaRelease/tools/ganga-cvmfs-install-dev.sh' % rel_ver, '~/')

    else:
        msg = "Problem getting the list of releases"

except Exception as err:
    msg = str(err)

# send any messages
if msg:
    os.system('echo "%s" | mail -s "Message from CVMFS Installer" "project-ganga-developers@cern.ch"' % msg)
