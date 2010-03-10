#! /usr/bin/env python


#-------------------------------------------
# Go through the input file list and get TAG info
import os, sys, pickle
from commands import getstatusoutput

maxnumrefs = 1
if os.environ.has_key('MAXNUMREFS'):
    maxnumrefs = eval(os.environ['MAXNUMREFS'])
    
stream_ref = 'StreamAOD_ref'
dataset_type = 'AOD'
if os.environ.has_key('STREAM_REF'):
    stream_ref = 'Stream'+os.environ['STREAM_REF']+'_ref'
    dataset_type = os.environ['STREAM_REF']
    
taginfo = {}
taglfns = [ line.strip() for line in file('input_files') ]
ref_lkup = {}

import dq2
from dq2.clientapi.DQ2 import DQ2
dq = DQ2 ()

# add a blank file to stop multiple runnings
os.system('touch RunSuccess')
    
if os.environ['GANGA_ATHENA_WRAPPER_MODE'] == 'grid':
    tagguids = [ line.strip() for line in file('input_guids') ]
    
    dq2setuppath = '$VO_ATLAS_SW_DIR/ddm/latest/setup.sh'
    inputtxt = 'dq2localid.txt'
    timeout = 600

    try:
        temp_dq2localsiteid = [ line.strip() for line in file(inputtxt) ]
        dq2localsiteid = temp_dq2localsiteid[0]
    except:
        dq2localsiteid = os.environ[ 'DQ2_LOCAL_SITE_ID' ]
        pass

    #-------------------------------------------
    # First, sort out a dataset and file mapping and dq2-get the files
    if not os.environ.has_key('DATASETNAME'):
        raise NameError, "ERROR: DATASETNAME not defined"
        sys.exit(EC_Configuration)
    
    tagdatasetnames = os.environ['DATASETNAME'].split(":")

    for tagdatasetname in tagdatasetnames:

        # find the files in this dataset and compare with the specified file list
        files = dq.listFilesInDataset(tagdatasetname)

        flist = []
        for guid in files[0]:
            if files[0][guid]['lfn'] in taglfns:
                flist.append( files[0][guid]['lfn'] )
            
        if len(flist) == 0:
            continue
        
        flist_str = ','.join(flist)

        # download the tag file
        print "Downloading files '%s' from dataset '%s'" % (flist_str, tagdatasetname)
        cmd = 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_ORIG ; export PATH=$PATH_ORIG ; export PYTHONPATH=$PYTHONPATH_ORIG ; source %s; dq2-get --client-id=ganga --automatic --local-site=%s --no-directories --timeout %s -p lcg -f %s %s' % (dq2setuppath, dq2localsiteid, timeout, flist_str, tagdatasetname)
        rc, out = getstatusoutput(cmd)
        print out

        # check all files have been downloaded
        bad_dq2 = False
        for f in flist:
            if not os.path.exists(f):
                bad_dq2 = True
                
        if (rc!=0) or bad_dq2:
            print "ERROR: error during dq2-get occured."
            continue

        # update the taginfo structure
        print "---------------------------------------------"
        print "Current info:"            
        print taginfo
        print "---------------------------------------------"
        for f in flist:
            taginfo[f] = { 'dataset':tagdatasetname, 'path':'', 'refs': [] }
                
else:
    # sort out the files for local running
    for f in taglfns:
        if os.access( f, os.R_OK ):
            
            if not os.path.exists( os.path.basename(f) + ".root"):
                os.symlink(f, os.path.basename(f) + ".root")
                
            # split the tag file based on file GUIDs
            cmd = "CollSplitByGUID -src " + os.path.basename(f) + " RootCollection -splitref "+stream_ref
            rc, out = getstatusoutput(cmd)
            if (rc!=0):
                print out
                print "ERROR: error during CollSplitByGUID. Restoring original environment variables and retrying...."
                cmd = 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP ; export PATH=$PATH_BACKUP; export PYTHONPATH=$PYTHONPATH_BACKUP ; ' + cmd
                rc, out = getstatusoutput(cmd)

                if (rc!=0):
                    print out
                    print "ERROR: error during CollSplitByGUID. Giving up..."
                    continue


            # now rename the files
            filenum = 0
            for sub_f in os.listdir('.'):

                if sub_f.find('sub_collection') != -1:
                    new_name = os.path.basename(f) + '.subcoll.%d' % filenum
                    print "Moving %s to %s..." % (sub_f, new_name)
                    os.rename(sub_f, new_name)
                    filenum += 1
                    taginfo[new_name] = { 'dataset':'', 'path':os.environ['GANGA_OUTPUT_PATH'], 'refs': [] }

    print "---------------------------------------------"
    print taginfo

bad_taginfo = []

for f in taginfo:
    
    # create a link to <filename>.root
    print "================================================="
    rc, out = getstatusoutput('ls -ltr')
    print out
    if not os.path.exists( f + ".root"):
        print "linking " + f
        os.symlink(f, f + ".root")
        rc, out = getstatusoutput('ls -ltr')
        print out
    print "================================================="
    
    # now run the Collection Utilities
    cmd = "CollListFileGUID -src " + f + " RootCollection -queryopt "+stream_ref+" | grep -E [[:alnum:]]{8}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{12} "
    rc, out = getstatusoutput(cmd)
            
    if (rc!=0):
        print out
        print "ERROR: error during CollListFileGUID. Giving up..."
        bad_taginfo.append(f)
        if os.path.exists(f + ".root"):
            os.system('rm %s' % (f + ".root"))
        continue
               
    ref_guids = []
    for ln in out.split('\n'):
        try:
            ref_guids.append(ln.split()[0])
        except:
            continue

    print "GUIDs referenced by " + f
    print repr(ref_guids)
    if len(ref_guids) == 0:
        bad_taginfo.append(f)
        if os.path.exists(f + ".root"):
            os.system('rm %s' % (f + ".root"))
            continue

    for ref_guid in ref_guids:

        if len( taginfo[f]['refs'] ) >= maxnumrefs:
            print "Found maximum number of refs. Stopping search."
            break
        
        print "Getting dataset referenced by GUID %s" % ref_guid
        
        ref_name = ''
        ref_dataset = ''

        # check the cache first
        for lkup_name in ref_lkup:
            if ref_lkup[lkup_name].has_key(ref_guid):

                present = False
                for ref_prev in taginfo[f]['refs']:
                    if ref_prev[0] == ref_lkup[lkup_name][ref_guid]['lfn'] and ref_prev[1] == lkup_name:
                        
                        present = True

                ref_name = ref_lkup[lkup_name][ref_guid]['lfn']
                ref_dataset = lkup_name
                
                if not present:
                    print "Using cache for " + f
                    taginfo[f]['refs'].append( [ ref_lkup[lkup_name][ref_guid]['lfn'], lkup_name, ref_guid ] )
                    
                
        if ref_name != '' and ref_dataset != '':
            continue
        
        ref_vuids = dq.contentClient.queryDatasetsWithFileByGUID(ref_guid)
        if len(ref_vuids) == 0:
            continue

        for ref_vuid in ref_vuids:
                    
            try:
                print "Trying VUID %s for GUID %s... " % (ref_vuid, ref_guid)
                ref_dataset = dq.repositoryClient.resolveVUID(ref_vuid)
                ref_name = ref_dataset.get('dsn')
                if ref_name != '' and len(dq.listDatasetReplicas(ref_name)) != 0 and ref_name.find("." + dataset_type + ".") != -1:
                    break
                else:
                    ref_name = ''
                    
            except dq2.repository.DQRepositoryException.DQUnknownDatasetException:
                print "ERROR Finding dataset for vuid for " + ref_vuid
            
        if ref_name == '':
            continue

        # store useful stuff
        ref_files = dq.listFilesInDataset(ref_name)
        if ref_files[0].has_key(ref_guid):
            present = False
            for ref_prev in taginfo[f]['refs']:
                if ref_prev[0] == ref_files[0][ref_guid]['lfn'] and ref_prev[1] == ref_name:
                    present = True
                    
            if not present:
                taginfo[f]['refs'].append( [ ref_files[0][ref_guid]['lfn'], ref_name, ref_guid ])
                print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])
                # cache this dataset in case it's referenced elsewhere...
                ref_lkup[ref_name] = ref_files[0]

    if len(taginfo[f]['refs']) == 0:
        print "ERROR: Couldn't find any references for TAG file %s." % f
        bad_taginfo.append(f)
    
for info in bad_taginfo:
    del taginfo[info]
    
if len(taginfo) == 0:
    print "ERROR: Empty taginfo structure. Serious problems!"
    sys.exit(-1)

if os.environ['GANGA_ATHENA_WRAPPER_MODE'] != 'grid':
    # tar up the sub collections
    rc, out = getstatusoutput("tar -zcf subcoll.tar.gz " + ' '.join( taginfo ))

    if (rc != 0):
        print out
        print "ERROR: Couldn't tar up the sub collections."
        sys.exit(-1)
        

pickle.dump( taginfo, open("taginfo.pkl", "w") )
