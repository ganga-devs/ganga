#! /usr/bin/env python
################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ganga-joboption-parse.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $
################################################################
# Parse Athena jobOptions files

import os, sys, commands

# Append jobid to output filename
# Only for DQ2OutputDataset

# Get output jobid
try:
    output_jobid = os.environ['OUTPUT_JOBID']
except:
    raise "ERROR: OUTPUT_JOBID not defined"
    sys.exit(1)
    
# Read output_files 
try:
    open('output_files','r')
except IOError:
    print 'No output_files requested.'
    sys.exit(0)
output_files = [ line.strip() for line in file('output_files') ]
if not len(output_files):
    print 'No output_files requested.'
    sys.exit(0)
    
#Create output_files.new with trailing jobid
outFile = open('output_files.new','w')
for output_file in output_files:
    outFile.write(output_file+"."+output_jobid)
outFile.close()
       
# Parse jobOptions file to append jobid to output_files
if os.environ.has_key('ATHENA_OPTIONS'):
    joboptions = os.environ['ATHENA_OPTIONS'].split(' ')

    for jfile in joboptions:
        try:
            open(jfile,'r')
        except IOError:
            continue
        for output_file in output_files:
            cmd = "sed 's/"+output_file+"/"+output_file+"."+output_jobid+"/' "+jfile+" > " + jfile+".new"
            rc, output = commands.getstatusoutput(cmd)
            if rc:
                print "ERROR: parsing output file in jobOptions"
                print cmd
                print rc, output
            os.rename(jfile+".new", jfile)   
