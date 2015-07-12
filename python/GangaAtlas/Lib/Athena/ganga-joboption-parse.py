#! /usr/bin/env python
################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ganga-joboption-parse.py,v 1.1 2008-07-17 16:41:18 moscicki Exp $
################################################################
# Parse Athena jobOptions files

import os, sys, commands, time, re, math, random

# error codes
# WRAPLCG_UNSPEC
EC_UNSPEC        = 410000 
# WRAPLCG_STAGEOUT_UNSPEC
EC_STAGEOUT      = 410400

# Get output jobid
try:
    output_jobid = os.environ['OUTPUT_JOBID']
except:
    raise LookupError("ERROR: OUTPUT_JOBID not defined")
    sys.exit(EC_STAGEOUT)

try:
    output_datasetname = os.environ['OUTPUT_DATASETNAME']
except:
    raise LookupError("ERROR: OUTPUT_DATASETNAME not defined")
    sys.exit(EC_STAGEOUT)

# Get output jobid
try:
    use_short_filename = os.environ['GANGA_SHORTFILENAME']
except:
    raise LookupError("ERROR: GANGA_SHORTFILENAME not defined")
    sys.exit(EC_STAGEOUT)

# Get output jobid
try:
    dq2_outputfile_namelength = os.environ['DQ2_OUTPUTFILE_NAMELENGTH']
except:
    print LookupError("ERROR: DQ2_OUTPUTFILE_NAMELENGTH not defined - using 150 characters")
    dq2_outputfile_namelength = 150

    
# Read output_files 
try:
    open('output_files','r')
except IOError:
    print 'No output_files requested.'
    sys.exit(EC_UNSPEC)
output_files = [ line.strip() for line in file('output_files') ]
if not len(output_files):
    print 'No output_files requested.'
    sys.exit(EC_UNSPEC)
    
#Create output_files.new with new outputfilenames
new_output_files = {}
outFile = open('output_files.new','w')
for output_file in output_files:
    temptime = time.gmtime()
    output_datasetname = re.sub('\.[\d]+$','',output_datasetname)
    pattern=output_datasetname+".%04d%02d%02d%02d%02d%02d%04d._%05d."+output_file
    i=output_jobid.split('.')
    rannum = int(math.ceil(10000*random.uniform(0,1)))
    if len(i)>1:
        new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5], rannum,int(i[1])+1)
        short_pattern = ".%04d%02d%02d%02d%02d%02d%04d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5], rannum,int(i[1])+1)
    else:
        new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5], rannum,1)
        short_pattern = ".%04d%02d%02d%02d%02d%02d%04d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5], rannum,1)


    if len(new_output_file)>dq2_outputfile_namelength:
        print '!!!!!!!!! ERROR - JOB ABORTING !!!!!!!!!!!!!!!!!!!!!!!!!! '
        print '!!!!!!!!! Output filename length is larger than %s characters!!!!!!!!!!!!!!!!!' %dq2_outputfile_namelength
        print '!!!!!!!!! %s is too long !!!!!!!!!!!!!!!!!!!!' % new_output_file
        print '!!!!!!!!! Please use a shorter output dataset name !!!!!!!!!!!!!!!!!!'
        sys.exit(EC_STAGEOUT)
    
    new_short_output_file = re.sub(".root", short_pattern+".root" , output_file )
    if new_short_output_file == output_file:
        new_short_output_file =  short_pattern[1:] + "." + output_file

    if use_short_filename:
        new_output_files[output_file] = new_short_output_file
        outFile.write(new_short_output_file)
    else:
        new_output_files[output_file] = new_output_file
        outFile.write(new_output_file)

    outFile.write('\n')
outFile.close()
       
# Parse jobOptions file to append jobid to output_files
if 'ATHENA_OPTIONS' in os.environ:
    joboptions = os.environ['ATHENA_OPTIONS'].split(' ')

    for jfile in joboptions:
        try:
            open(jfile,'r')
        except IOError:
            continue
        for output_file in output_files:
            cmd = "sed 's/"+output_file+"/"+new_output_files[output_file]+"/' "+jfile+" > " + jfile+".new"
            rc, output = commands.getstatusoutput(cmd)
            if rc:
                print "ERROR: parsing output file in jobOptions"
                print cmd
                print rc, output
            os.chmod(jfile+".new", 0o777)
            os.rename(jfile+".new", jfile)   
