#!/bin/env python

# -----------------------------------------------
#
# run the maintenance of NA48/62 software on the grid
#
# -----------------------------------------------

import os, string

# globals and setup
_hepdbPkg = ['hepdb', 'hepdbInstaller.py', 'script']
_showerPkg = ['Shower Libs', 'showerInstaller.py', 'script']
_nasimPkg = ['Nasim Install', 'nasimInstaller.py', 'script']
_compactPkg = ['Compact Install', 'compactInstaller.py', 'script']
_nasimJob = ['Nasim Jobs', 'nasimJob.py', 'job']
_flukaPkg = ['Fluka', 'flukaInstaller.py', 'script']

_pkgList = [ _flukaPkg, _hepdbPkg, _showerPkg, _nasimPkg, _compactPkg, _nasimJob ]
#_pkgList = [ _hepdbPkg, _nasimJob ] 

_numJobsPerPkg = 3
_submit = False

runMonitoring()

if _submit:
    
    # First, grab the list of CEs
    os.system("source lcguisetup; lcg-infosites --vo na48 ce > ce_list.txt")

    ce_file = open("ce_list.txt", "r")
    ces = []
    for ln in ce_file.readlines():
        if ln.find("CPU") != -1 or ln.find("-----------") != -1:
            #skip header lines
            continue

        toks = ln.split()
        ces.append( toks[5] )

    ce_file.close()

    # make sure entries in the job tree are present for each CE
    good_chars = string.ascii_letters + string.digits + '-' + '.'

    # decide which package to run on
    if len( jobs ) > 0:
        j = jobs( len(jobs) - 1 )
        i = 0
        while i < len(_pkgList):

            if j.name.find(_pkgList[i][1]) != -1:
                break
            i += 1

        i += 1
        if i == len(_pkgList):
            i = 0
        pkg = _pkgList[i]
        
    else:
        pkg = _pkgList[0]

    print "Running over package " + pkg[0]
    
    for ce in ces:

        # clean ce name
        safe_ce = ''
        for c in ce:
            if not c in good_chars:
                c = '_'

            safe_ce += c

        # check job tree
        jobtree.cd()
        if not jobtree.exists( safe_ce ):
            jobtree.mkdir( safe_ce )

        # pick the next package in order based on the last job submitted
        jobtree.cd( safe_ce )
        if not jobtree.exists( pkg[0] ):
            jobtree.mkdir( pkg[0] )

        # for each package, if # of jobs, delete the oldest
        jobtree.cd( pkg[0] )
        if len( jobtree.getjobs() ) >= _numJobsPerPkg:
            j = jobtree.getjobs()[0]
            if j.status in ['running', 'submitted']:
                j.kill()
            j.remove()

        # Now create a new job
        if pkg[2] == 'script':
            j = Job()
            j.name = pkg[1]
            j.application = Executable()
            j.application.exe = File( os.path.join( '/home/mws/na48/GangaNA48/scripts/', pkg[1] ) )
            j.backend = LCG()
            j.backend.CE = ce
            j.submit()
        else:
            execfile( os.path.join( '/home/mws/na48/GangaNA48/scripts/', pkg[1] ) )
            j.name = pkg[1]
            j.backend.CE = ce
            j.submit()

        # now add to the job tree
        jobtree.add( j )
        
        jobtree.cd('..')
    
jobtree.cd()

# Collect results and update web page
f = open("status.html", "w")

# header info
f.write("<html>\n<body>\n\n")
f.write("<h1>NA48 Gird Test Results</h1>")
f.write("\n<table border=1>\n<tr>")
f.write("\n<td></td>")

for pkg in _pkgList:
    f.write("\n<th colspan=\"" + str(_numJobsPerPkg) + "\">" + pkg[0] + "</th>")

f.write("\n</tr>")

# loop and add package info
for safe_ce in jobtree.listdirs():

    jobtree.cd(safe_ce)
    
    f.write("\n<tr>")
    f.write("\n<td>" + safe_ce + "</td>")

    for pkg in _pkgList:

        if jobtree.exists( pkg[0] ):
            
            jobtree.cd( pkg[0] )
        
            r = 0
            for j in jobtree.getjobs():

                # create id using ce name, pkg and job number
                import md5
                md5in = safe_ce + pkg[0] + str(r)
                id = md5.new(md5in).hexdigest()
                
                # create the status web page and copy stdout/stderr
                os.system("mkdir -p /home/mws/na48/maintenance/results/jobstats/"+id)
                
                if "stdout.gz" in os.listdir(j.outputdir) and "stderr.gz" in os.listdir(j.outputdir):
                    os.system("mv " + j.outputdir + "std*.gz /home/mws/na48/maintenance/results/jobstats/"+id+"/.")
                    os.system("cd /home/mws/na48/maintenance/results/jobstats/"+id+"/. ; gunzip -f stdout.gz ; gunzip -f stderr.gz ; cd - > /dev/null")
                
                f2 = open("/home/mws/na48/maintenance/results/jobstats/"+id+"/stats.html", "w")
                f2.write("<html>\n<body>\n\n")

                # links to stdout, stderr
                if j.status == "completed" or j.status == "failed":
                    f2.write("<h2><a href=\"stdout\">stdout</a></h2>")
                    f2.write("<h2><a href=\"stderr\">stderr</a></h2>")
            
                # job info
            
                f2.write("\n\n</html>\n</body>\n")
                f2.close()

            
                # add links, etc. into table
                if j.status == "failed":
                    f.write("\n<td bgcolor=\"#FF7777\"><a href=\"results/jobstats/"+id+"/stats.html\">F</a></td>")
                elif j.status == "submitted":
                    f.write("\n<td bgcolor=\"#FFFF77\"><a href=\"results/jobstats/"+id+"/stats.html\">S</a></td>")
                elif j.status == "completed":
                    f.write("\n<td bgcolor=\"#7777FF\"><a href=\"results/jobstats/"+id+"/stats.html\">C</a></td>")
                elif j.status == "running" or j.status == "completing":
                    f.write("\n<td bgcolor=\"#77FF77\"><a href=\"results/jobstats/"+id+"/stats.html\">R</a></td>")
                else:
                    f.write("\n<td>" + j.status + "</td>")
                
                r += 1
                if r == _numJobsPerPkg:
                    break
            
            while r < _numJobsPerPkg:
                f.write("\n<td>N/A</td>")
                r += 1
        
            jobtree.cd("..")
        else:
            r = 0
            while r < _numJobsPerPkg:
                f.write("\n<td>N/A</td>")
                r += 1
                
    f.write("\n</tr>")
    jobtree.cd()

f.write("\n\n</html>\n</body>\n")
f.close()


# ----------------------------------------------
# Create the whitelist
pkglist = {'Nasim':_nasimJob, 'Fluka':_flukaPkg}

CEInfo = {}
CEInfo['all'] = []
CEInfo['whitelist'] = {}

for pkg in pkglist:
    
    CEInfo['whitelist'][pkg] = []
    
    for safe_ce in jobtree.listdirs():

        jobtree.cd(safe_ce)
        if jobtree.exists( pkglist[pkg][0] ):
            
            jobtree.cd( pkglist[pkg][0] )

            # find the last Nasim job run and see if it completed
            j = jobtree.getjobs()[len(jobtree.getjobs()) - 2]
            if not j.backend.actualCE in CEInfo['all']:
                CEInfo['all'].append(j.backend.actualCE)
            
            if j.status == 'completed':
                # add to whitelist
                CEInfo['whitelist'][pkg].append(j.backend.actualCE)
            
        jobtree.cd()
    
# save the ce info
import pickle
open("ceinfo.pkl", "w").write(pickle.dumps(CEInfo) )
