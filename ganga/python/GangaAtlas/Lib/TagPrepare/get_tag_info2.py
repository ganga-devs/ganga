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
            if _refCache[name].has_key(ref_guid):

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
                if ref_name != '' and len(_dq.listDatasetReplicas(ref_name)) != 0 and ref_name.find("." + _datasetType + ".") != -1:
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
        if ref_files[0].has_key(ref_guid):
            refs.append([ ref_files[0][ref_guid]['lfn'], ref_name, ref_guid ] )
            #print "Found dataset ref: %s, %s" % (ref_name, ref_files[0][ref_guid]['lfn'])

            # cache this dataset in case it's referenced elsewhere...
            _refCache[ref_name] = ref_files[0]


    return refs

def findReferences2( infile ):
    """Find the references from this input TAG file"""
    global _refCache, _streamRef

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
            if _refCache[name].has_key(ref_guid):

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
        if ref_files[0].has_key(ref_guid):
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
                if _refCache[name].has_key(ref_guid):

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
            if ref_files[0].has_key(ref_guid):
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
    global _refCache, _streamRef
    
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

    for f in taglfns:
        if os.access( f, os.R_OK ):

            if not os.path.exists( os.path.basename(f) + ".root"):
                os.symlink(f, os.path.basename(f) + ".root")

            # first, list all the guids that we care about
            refs = findReferences(os.path.basename(f))

            # try to match on names
            tagCache = {}
            for ref in refs:
                file_done = False
                tagds = ref[1].replace(_datasetType, 'TAG')

                if not tagds in tagCache:                    
                    tagCache[tagds] = _dq.listFilesInDataset(tagds)[0]

                for tf in tagCache[tagds]:
                    if ref[0].replace(_datasetType, 'TAG') == tagCache[tagds][tf]['lfn']:
                        taginfo[ref[0].replace(_datasetType, 'TAG')] = {}
                        taginfo[ref[0].replace(_datasetType, 'TAG')]['dataset'] = tagds
                        taginfo[ref[0].replace(_datasetType, 'TAG')]['guid'] = tf
                        if not 'refs' in taginfo[ref[0].replace(_datasetType, 'TAG')]:
                            taginfo[ref[0].replace(_datasetType, 'TAG')]['refs'] = []

                        taginfo[ref[0].replace(_datasetType, 'TAG')]['refs'].append(ref)
                        file_done = True

                if not file_done:
                    raise TypeError("COULDN'T FIND LINK FOR FILE:  %S (%s)" % (ref[0], ref[1]))
                    tag_info = {}
                    return

    return taginfo

if __name__ == "__main__":

    taglfns = [ line.strip() for line in file('input_files') ]
    print taglfns
    if os.environ.has_key('STREAM_REF'):
        taginfo = createTagInfo( os.environ['STREAM_REF'], taglfns )
    else:
        taginfo = createTagInfo( 'AOD', taglfns )

    pickle.dump( taginfo, open("taginfo.pkl", "w") )
        
