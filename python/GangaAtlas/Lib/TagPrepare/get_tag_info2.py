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

def findReferences( infile ):
    """Find the references from this input TAG file"""
    global _refCache, _streamRef, _datasetType

    # Find the links to the stream required
    cmd = "CollListFileGUID -src " + infile + " RootCollection -queryopt "+ _streamRef +" | cut -d' ' -f 1"
    rc, out = getstatusoutput(cmd)
    if (rc!=0) or (out.find("command not found")!=-1):
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

    #print "GUIDs referenced by " + infile
    #print repr(ref_guids)
    if len(ref_guids) == 0:
        print "ERROR: Could not find any references of type '%s' in file %s" % (_streamRef, infile)
        return {}

    # go over the guids and find all references
    for ref_guid in ref_guids:
        #print "Getting dataset referenced by GUID %s" % ref_guid
        
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
                #print "Trying VUID %s for GUID %s... " % (ref_vuid, ref_guid)
                ref_dataset = _dq.repositoryClient.resolveVUID(ref_vuid)
                ref_name = ref_dataset.get('dsn')

                if ref_name != '' and ref_name[:8].find("panda.") != 0 and len(_dq.listDatasetReplicas(ref_name)) != 0 and ((ref_name.find("." + _datasetType + ".") != -1) or ((_datasetType == 'RAW') and ref_name.find("." + _datasetType) != -1)):
                    break
                else:
                    ref_name = ''
                    
            except dq2.repository.DQRepositoryException.DQUnknownDatasetException:
                pass
                #print "ERROR Finding dataset for vuid for " + ref_vuid

        if ref_name == '':
            continue

        # store useful stuff
        ref_files = _dq.listFilesInDataset(ref_name)
        if ref_guid in ref_files[0]:
            refs.append([ ref_files[0][ref_guid]['lfn'], ref_name, ref_guid ] )
            #print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])

            # cache this dataset in case it's referenced elsewhere...
            _refCache[ref_name] = ref_files[0]


    return refs

def findReferences2( infile ):
    """Find the references from this input TAG file"""
    global _refCache, _streamRef, _datasetType

    # Find the links to the stream required
    cmd = "CollListToken -src " + infile + " RootCollection | grep -E \"Tokens|StreamTAG|%s\"" % _streamRef
    rc, out = getstatusoutput(cmd)

    if (rc!=0):
        print "ERROR: error during CollListFileGUID:"
        print out
        return {}

    # get a list of the guids
    ref_guids = {}
    refs = []
    all_ref_guids = []
    guid = ""
    tag_guid = ""
    for ln in out.split('\n'):

        # go over and find each event
        if ln.find("Tokens:") != -1:

            if tag_guid != "" and guid != "":
                if not tag_guid in ref_guids.keys():
                    ref_guids[tag_guid] = []

                if not guid in ref_guids[tag_guid]:
                    ref_guids[tag_guid].append(guid)

            guid = ""
            tag_guid = ""
            continue

        if ln.find(_streamRef) != -1 and ln.find("INFO") == -1:
            guid = ln[ ln.find("DB=")+3 : ln.find("]") ]

        if ln.find("StreamTAG") != -1 and ln.find("INFO") == -1:
            tag_guid = ln[ ln.find("DB=")+3 : ln.find("]") ]

    #print "GUIDs referenced by " + infile
    #print repr(ref_guids)
    if len(ref_guids) == 0:
        print "ERROR: Could not find any references of type '%s' in file %s" % (_streamRef, infile)
        return {}


    # go over the guids and find all references -  TAG files first
    tag_info = {}
    for ref_guid in ref_guids:
        #print "Getting dataset referenced by GUID %s" % ref_guid
        
        ref_name = ''
        ref_dataset = ''
        
        # check the cache first
        for name in _refCache:
            if ref_guid in _refCache[name]:

                ref_name = _refCache[name][ref_guid]['lfn']
                ref_dataset = name
                
                #refs.append( [_refCache[name][ref_guid]['lfn'], name, ref_guid ] )
                tag_info[_refCache[name][ref_guid]['lfn']] = {}
                tag_info[_refCache[name][ref_guid]['lfn']]['dataset'] = name
                tag_info[_refCache[name][ref_guid]['lfn']]['guid'] = ref_guid
                tag_info[ref_files[0][ref_guid]['lfn']]['refs'] = {}
                
        if ref_name != '' and ref_dataset != '':
            continue

        # not in cache so try to find a valid dataset
        ref_vuids = _dq.contentClient.queryDatasetsWithFileByGUID(ref_guid)
        #print ref_vuids
        if len(ref_vuids) == 0:
            continue

        for ref_vuid in ref_vuids:
                    
            try:
                #print "Trying VUID %s for GUID %s... " % (ref_vuid, ref_guid)
                ref_dataset = _dq.repositoryClient.resolveVUID(ref_vuid)
                ref_name = ref_dataset.get('dsn')
                if ref_name != '' and len(_dq.listDatasetReplicas(ref_name)) != 0 and ref_name.find(".TAG.") != -1:
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
            #refs.append([ ref_files[0][ref_guid]['lfn'], ref_name, ref_guid ] )
            tag_info[ref_files[0][ref_guid]['lfn']] = {}
            tag_info[ref_files[0][ref_guid]['lfn']]['dataset'] = ref_name
            tag_info[ref_files[0][ref_guid]['lfn']]['guid'] = ref_guid
            tag_info[ref_files[0][ref_guid]['lfn']]['refs'] = {}
            
            print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])

            # cache this dataset in case it's referenced elsewhere...
            _refCache[ref_name] = ref_files[0]


    # NOW do the data files
    for tag_name in tag_info:
        
        tag_guid = tag_info[tag_name]['guid']
        
        for ref_guid in ref_guids[tag_guid]:

            print "Getting dataset referenced by GUID %s" % ref_guid

            ref_name = ''
            ref_dataset = ''

            # check the cache first
            for name in _refCache:
                if ref_guid in _refCache[name]:

                    ref_name = _refCache[name][ref_guid]['lfn']
                    ref_dataset = name

                    tag_info[tag_name]['refs'].append( [_refCache[name][ref_guid]['lfn'], name, ref_guid ] )


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
                    if ref_name != '' and len(_dq.listDatasetReplicas(ref_name)) != 0 and ref_name.find(".TAG.") != -1:
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
                tag_info[tag_name]['refs'].append( [_refCache[name][ref_guid]['lfn'], name, ref_guid ] )

                print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])

                # cache this dataset in case it's referenced elsewhere...
                _refCache[ref_name] = ref_files[0]


    
    print tag_info
    
    return {}

#-------------------------------------------
# Go through the input file list and get TAG info
# stream_ref: AOD/ESD/RAW
def createTagInfo( stream_ref, infiles ):

    global _refCache, _streamRef, _datasetType
    
    if not stream_ref in ['AOD','ESD','RAW']:
        raise TypeError("Incorrect stream ref specification %s" % stream_ref)

    _datasetType = stream_ref
    _streamRef = 'Stream'+_datasetType+'_ref'

    taginfo = {}
    taglfns = infiles
    tag_ref_files = []

    #-------------------------------------------
    # sort out the files for local running
    num = 0
    file_refs = {}
    print_num = 0
    
    for f in taglfns:        
        print f
        if os.access( f, os.R_OK ):

            if not os.path.exists( os.path.basename(f) + ".root"):
                os.symlink(f, os.path.basename(f) + ".root")

            # first, list all the guids that we care about
            refs = findReferences(os.path.basename(f))

            # try to match on names
            tagCache = {}
            for ref in refs:
                file_done = False
                tagds = ref[1].replace(_datasetType, 'TAG').replace('.recon.', '.merge.')
                poss_tagds =_dq.listDatasets(tagds + "*")

                del_list = []
                for tagds2 in poss_tagds.keys():
                    if tagds2.find("/") != -1 or (tagds.find("_tid") == -1 and tagds2.find("_tid") != -1) or tagds2.find("_sub") != -1 or tagds2.find("TAG.x") != -1:
                        del_list.append(tagds2)

                for tagds2 in del_list:
                    del poss_tagds[tagds2]

                if len(poss_tagds.keys()) == 0:
                    print "ERROR: Couldn't find matching TAG dataset."
                    return {}
                elif len(poss_tagds.keys()) > 1:
                    if print_num < 10:
                        print "WARNING: More than one TAG dataset found. Picking the first"
                        print "Possible Datasets were %s" % poss_tagds.keys()
                        print_num += 1
                        if print_num == 10:
                            print "WARNING: Reached printout limit..."
                            
                tagds = poss_tagds.keys()[0]
                
                if not tagds in tagCache:                    
                    tagCache[tagds] = _dq.listFilesInDataset(tagds)[0]


                for tf in tagCache[tagds]:
                    if ref[0].replace(_datasetType, 'TAG') == tagCache[tagds][tf]['lfn']:
                        taginfo[ref[0].replace(_datasetType, 'TAG')] = {}
                        taginfo[ref[0].replace(_datasetType, 'TAG')]['dataset'] = tagds
                        taginfo[ref[0].replace(_datasetType, 'TAG')]['guid'] = tf
                        if not 'refs' in taginfo[ref[0].replace(_datasetType, 'TAG')].keys():
                            taginfo[ref[0].replace(_datasetType, 'TAG')]['refs'] = []

                        taginfo[ref[0].replace(_datasetType, 'TAG')]['refs'].append(ref)
                        file_done = True

                if not file_done:
                    if print_num < 10:
                        print "WARNING: No direct matching of files from data to TAG."
                        print_num += 1
                        if print_num == 10:
                            print "WARNING: Reached printout limit..."
                    
                    for tf in tagCache[tagds]:
                        if not tagCache[tagds][tf]['lfn'] in taginfo.keys():
                            taginfo[ tagCache[tagds][tf]['lfn'] ] = {}
                        taginfo[tagCache[tagds][tf]['lfn']]['dataset'] = tagds
                        taginfo[tagCache[tagds][tf]['lfn']]['guid'] = tf
                        if not 'refs' in taginfo[tagCache[tagds][tf]['lfn']].keys():
                            taginfo[tagCache[tagds][tf]['lfn']]['refs'] = []

                        taginfo[tagCache[tagds][tf]['lfn']]['refs'].append(ref)

                    #raise TypeError("COULDN'T FIND LINK FOR FILE:  %s (%s)" % (ref[0], ref[1]))
                    #tag_info = {}
                    #return {}

    return taginfo

if __name__ == "__main__":

    taglfns = [ line.strip() for line in file('input_files') ]

    if 'STREAM_REF' in os.environ:
        taginfo = createTagInfo( os.environ['STREAM_REF'], taglfns )
    else:
        taginfo = createTagInfo( 'AOD', taglfns )

    if len(taginfo) == 0:
        print "ERROR: Empty taginfo structure"
        pickle.dump( taginfo, open("taginfo.pkl", "w") )
        sys.exit(-1)
    
    pickle.dump( taginfo, open("taginfo.pkl", "w") )
        
