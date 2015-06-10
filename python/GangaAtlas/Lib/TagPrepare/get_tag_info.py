#! /usr/bin/env python

# -------------------------------------------------------------
# globals and imports
import os, sys, pickle
from commands import getstatusoutput
import dq2
import shutil
from dq2.clientapi.DQ2 import DQ2
from struct import *

_refCache = {}
_streamRef = ''
_datasetType = 'AOD'
_dq = DQ2 ()

# -------------------------------------------------------------
# useful functions
def findReferences( infile ):
    """Find the references from this input TAG file"""
    global _refCache, _streamRef

    # Find the links to the stream required
    cmd = "CollListFileGUID -src " + infile + " RootCollection -queryopt "+ _streamRef +" | cut -d' ' -f 1"
    rc, out = getstatusoutput(cmd)
    if (rc!=0):
        print "ERROR: error during CollListFileGUID:"
        print out
        return {}

    # get a list of the guids
    ref_guids = []
    refs = []
    for ln in out.split('\n'):
        try:
            ref_guids.append(ln.split()[0])
        except:
            continue

    print "GUIDs referenced by " + infile
    print repr(ref_guids)
    if len(ref_guids) == 0:
        print "ERROR: Could not find any references of type '%s' in file %s" % (_streamRef, infile)
        return {}

    # go over the guids and find all references
    for ref_guid in ref_guids:
        print "Getting dataset referenced by GUID %s" % ref_guid
        
        ref_name = ''
        ref_dataset = ''

        # check the cache first
        for name in _refCache:
            if ref_guid in _refCache[name]:

                ref_name = _refCache[name][ref_guid]['lfn']
                ref_dataset = name
                refs.append( [_refCache[name][ref_guid]['lfn'], name, ref_guid ] )
                    
                
        if ref_name != '' and ref_dataset != '':
            continue

        # not in cache so try to find a valid dataset
        ref_vuids = _dq.contentClient.queryDatasetsWithFileByGUID(ref_guid)
        if len(ref_vuids) == 0:
            continue

        for ref_vuid in ref_vuids:
                    
            try:
                print "Trying VUID %s for GUID %s... " % (ref_vuid, ref_guid)
                ref_dataset = _dq.repositoryClient.resolveVUID(ref_vuid)
                ref_name = ref_dataset.get('dsn')
                if ref_name != '' and len(_dq.listDatasetReplicas(ref_name)) != 0 and ref_name.find("." + _datasetType + ".") != -1:
                    break
                else:
                    ref_name = ''
                    
            except dq2.repository.DQRepositoryException.DQUnknownDatasetException:
                print "ERROR Finding dataset for vuid for " + ref_vuid
            
        if ref_name == '':
            continue

        # store useful stuff
        ref_files = _dq.listFilesInDataset(ref_name)
        if ref_guid in ref_files[0]:
            refs.append([ ref_files[0][ref_guid]['lfn'], ref_name, ref_guid ] )
            print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])

            # cache this dataset in case it's referenced elsewhere...
            _refCache[ref_name] = ref_files[0]


    return refs


#-------------------------------------------
# Go through the input file list and get TAG info
if 'STREAM_REF' in os.environ:
    _datasetType = os.environ['STREAM_REF']
    
_streamRef = 'Stream'+os.environ['STREAM_REF']+'_ref'

taginfo = {}
taglfns = [ line.strip() for line in file('input_files') ]
tag_ref_files = []

#-------------------------------------------
# Check which TAG files we're looking - DQ2 or local/ELSSI
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

    # First, sort out a dataset and file mapping and dq2-get the files
    if 'DATASETNAME' not in os.environ:
        raise NameError("ERROR: DATASETNAME not defined")
        sys.exit(EC_Configuration)
    
    tagdatasetnames = os.environ['DATASETNAME'].split(":")

    for tagdatasetname in tagdatasetnames:

        # find the files in this dataset and compare with the specified file list
        files = _dq.listFilesInDataset(tagdatasetname)

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
        for f in flist:
            taginfo[f] = { 'dataset':tagdatasetname, 'path':'', 'refs': [], 'compress':False }

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


            try:
                refs = findReferences(f)
                taginfo[f]['refs'] = refs
            
            except:
                bad_taginfo.append(f)
                if os.path.exists(f + ".root"):
                    os.system('rm %s' % (f + ".root"))
                continue
    
    for info in bad_taginfo:
        del taginfo[info]
    
else:
    #-------------------------------------------
    # sort out the files for local running
    num = 0
    file_refs = {}

    for f in taglfns:
        if os.access( f, os.R_OK ):
            
            if not os.path.exists( os.path.basename(f) + ".root"):
                os.symlink(f, os.path.basename(f) + ".root")
                        
            # first, list all the guids that we care about
            refs = findReferences(os.path.basename(f))

            # now gather the referenced GUIDs by dataset
            ref_datasets = {}
            for ref in refs:
                if not ref[1] in ref_datasets:
                    ref_datasets[ref[1]] = []

                ref_datasets[ref[1]].append(ref[2])

            # create the guid split list
            guid_str = ""
            for dataset in ref_datasets:
                j = 0

                while j < len(ref_datasets[dataset]):
                    
                    guid_str += ":mycoll_%d\n" % num
                    guid_str += '\n'.join(ref_datasets[dataset][j:j+eval(os.environ['MAXNUMREFS'])])
                    guid_str += '\n'
                    file_refs["mycoll_%d" % num] = []
                    for ref in refs:
                        if ref[2] in ref_datasets[dataset][j:j+eval(os.environ['MAXNUMREFS'])]:
                            file_refs["mycoll_%d" % num].append(ref)

                    j += eval(os.environ['MAXNUMREFS'])
                    num += 1
            
            open("guid_list.txt", "w").write(guid_str)

            print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
            print file_refs
            print guid_str
            
            # split the tag file based on these dataset boundaries
            #cmd = "./CollSplitByGUID.exe -src " + os.path.basename(f) + " RootCollection -guidfile guid_list.txt -streamref " + _streamRef + " -queryopt "+_streamRef
            compress = False
            if (os.path.getsize(f) > 20):
                print "COMPRESSING TAG FILE..."
                cmd = "./CollCompressEventInfo.exe -src " + os.path.basename(f) + " RootCollection -guidfile guid_list.txt -splitref " + _streamRef + " -queryopt "+_streamRef
                rc, out = getstatusoutput(cmd)
                print cmd
                print out
                if (rc!=0):
                    print out
                    print "ERROR: error during CollCompressEventInfo. Restoring original environment variables and retrying...."
                    cmd = 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP ; export PATH=$PATH_BACKUP; export PYTHONPATH=$PYTHONPATH_BACKUP ; ' + cmd
                    rc, out = getstatusoutput(cmd)

                    if (rc!=0):
                        print out
                        print "ERROR: error during CollCompressEventInfo. Giving up..."
                        continue

                rc, out = getstatusoutput("ls -ltr")
                print out
                compress = True
            else:
                cmd = "./CollSplitByGUID.exe -src " + os.path.basename(f) + " RootCollection -guidfile guid_list.txt -splitref " + _streamRef + " -queryopt "+_streamRef
                rc, out = getstatusoutput(cmd)
                print cmd
                print out
                if (rc!=0):
                    print out
                    print "ERROR: error during CollSplitByGUID. Restoring original environment variables and retrying...."
                    cmd = 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP ; export PATH=$PATH_BACKUP; export PYTHONPATH=$PYTHONPATH_BACKUP ; ' + cmd
                    rc, out = getstatusoutput(cmd)

                    if (rc!=0):
                        print out
                        print "ERROR: error during CollSplitByGUID. Giving up..."
                        continue

                rc, out = getstatusoutput("ls -ltr")
                print out

            # now rename the files
            filenum = 0
            append_list = []
            for sub_f in os.listdir('.'):

                if sub_f.find('mycoll_') != -1 and sub_f.find('.ref.root') == -1:
                    if compress:
                        new_name = os.path.basename(f) + '.subcoll.%d.dat' % filenum
                    else:
                        new_name = os.path.basename(f) + '.subcoll.%d.root' % filenum
                        
                    print "Moving %s to %s..." % (sub_f, new_name)
                    os.rename(sub_f, new_name)
                    filenum += 1
                    if sub_f.find(".root") != -1:
                        sub_f = sub_f[: sub_f.find(".root") ]
                        
                    taginfo[new_name] = { 'dataset':'', 'path':os.environ['GANGA_OUTPUT_PATH'], 'refs': file_refs[sub_f], 'compress' : compress  }

    for sub_f in os.listdir('.'):
        if sub_f.find('mycoll_') != -1 and sub_f.find('.ref.root') != -1:
            append_list.append(sub_f)
            os.symlink(sub_f, sub_f + ".root")

    print "CREATING MASTER REF FILE..."
    cmd = "CollAppend -src " 
    for sub_f in append_list:
        cmd += sub_f + " RootCollection "
    cmd += " -dst " + os.path.basename(taglfns[0]) + ".ref RootCollection"
    tag_ref_files.append(os.path.basename(taglfns[0])+".ref.root")
    rc, out = getstatusoutput(cmd)
    print cmd
    print out
    if (rc!=0):
        print out
        print "ERROR: error during CollAppend. Restoring original environment variables and retrying...."
        cmd = 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP ; export PATH=$PATH_BACKUP; export PYTHONPATH=$PYTHONPATH_BACKUP ; ' + cmd
        rc, out = getstatusoutput(cmd)

        if (rc!=0):
            print out
            print "ERROR: error during CollAppend. Giving up..."
            

        if (rc!=0):
            for sub_f in append_list:
                os.system("rm " + sub_f)

    # make copies of the master ref file
    print tag_ref_files
    for f in taglfns:
        print "Checking:  " + os.path.basename(f)+".ref.root"
        if not os.path.exists(os.path.basename(f)+".ref.root"):
            print "Copying..."
            tag_ref_files.append(os.path.basename(f)+".ref.root")
            shutil.copyfile(os.path.basename(taglfns[0]) + ".ref.root", os.path.basename(f)+".ref.root")
            
    rc, out = getstatusoutput("ls -ltr")
    print out

    
    print "---------------------------------------------"
    print taginfo
    print tag_ref_files
    
if len(taginfo) == 0:
    print "ERROR: Empty taginfo structure. Serious problems!"
    sys.exit(-1)

if os.environ['GANGA_ATHENA_WRAPPER_MODE'] != 'grid':

    # if we're dealing with ELSSI files, strip everything to a text file and zip
    new_taginfo = {}
    #for fname in taginfo:
    #    getstatusoutput("gzip %s" % fname)
    #    new_taginfo[datname + ".gz"] = taginfo[fname]
    
    pickle.dump( taginfo, open("taginfo.pkl", "w") )
    
    # tar up the sub collections
    print "tar -zcf subcoll.tar.gz taginfo.pkl " + ' '.join( taginfo ) + ' ' + ' '.join(tag_ref_files)
    rc, out = getstatusoutput("tar -zcf subcoll.tar.gz taginfo.pkl " + ' '.join( taginfo ) + ' ' + ' '.join(tag_ref_files) )

    if (rc != 0):
        print out
        print "ERROR: Couldn't tar up the sub collections."
        sys.exit(-1)
        

else:
    pickle.dump( taginfo, open("taginfo.pkl", "w") )
