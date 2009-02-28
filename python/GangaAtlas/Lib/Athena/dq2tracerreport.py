#!/usr/bin/env python

import os, re, time, md5, commands
from dq2.tracer.client.TracerClient import TracerClient
from dq2.common import generate_uuid

GANGA_VERSION = '5.1.7'


def fill_report(eventVersion = None, remoteSite=None, localSite=None, timeStart=None, catStart=None, relativeStart=None, transferStart=None, validateStart=None, timeEnd=None,  duid=None,version=None, dataset=None, protocol=None, filename=None, filesize=None, guid=None, usr=None ):
    report = {
        'uuid': generate_uuid().replace('-',''),
        'eventType': 'drct_ganga',
        'eventVersion': eventVersion,
        'remoteSite': remoteSite,
        'localSite': localSite,
        'timeStart': timeStart,
        'catStart': catStart,
        'relativeStart': relativeStart,
        'transferStart': transferStart,
        'validateStart': validateStart,
        'timeEnd': timeEnd,
        'duid': duid.replace('-',''),
        'version': version,
        'dataset': dataset,
        'clientState': 'INIT_REPORT',
        'protocol': protocol,
        'filename': filename,
        'filesize': filesize,
        'guid': guid.replace('-',''),
        'usr': usr.replace('-','')
    }
    
    return report


########################################################################
if __name__ == '__main__':

    starttime = time.time()
    print '--------------'
    print 'DQ2 tracer preparation ...'

    tracertimes = [ line.strip() for line in file('dq2tracertimes.txt') ]

    # User hash
    m=md5.new()
    m.update(commands.getstatusoutput('voms-proxy-info -identity')[1])
    usrhex = m.hexdigest()

    numfiles3 = 0
    filepat = '"PFN:(.+)"'
    selectorpat = 'ServiceMgr.EventSelector.InputCollections = \[(.+),\]'
    selectorpat2 = 'mySampleList = \[(.+)\]'
    sitepat = 'DQ2_LOCAL_SITE_ID: (.+)'
    datasetnamepat = '^DATASETNAME=(.+)'
    
    input_files = []
    input_files_sel = []

    if 'stdout' in os.listdir('.'):  
        zfile = os.popen('cat '+os.path.join('.','stdout' ))
        for line in zfile:
            # Find EventSelector
            match = re.search(selectorpat, line)
            if match:
                selfiles = match.group(1)
                selfiles = re.sub('"','', selfiles)
                input_files_sel = selfiles.split(',')
                
            # Find mySampleList
            match = re.search(selectorpat2, line)
            if match:
                selfiles = match.group(1)
                selfiles = re.sub('"','', selfiles)
                input_files_sel = selfiles.split(',')
            
            # Find actually processed input files
            match = re.search(filepat, line)
            if match:
                filename = match.group(1)
                if not filename in input_files:
                    input_files.append(filename)
                    numfiles3 = numfiles3 + 1
                    
            match = re.search(sitepat, line)       
            if match:
                siteid = match.group(1)

            match = re.search(datasetnamepat, line)       
            if match:
                datasetname = match.group(1)

    print '%s out of %s file of dataset %s have been processed.' %(numfiles3, len(input_files_sel), datasetname)
    print 'Number could be wrong for certain input modes!'
    
    if os.environ.has_key('GANGA_VERSION'):
        ganga_version = os.environ['GANGA_VERSION']
    else:
        ganga_version = 'GANGA'

    try:
        lfns = [ line.strip() for line in file('input_files') ]
        guids = [ line.strip() for line in file('input_guids') ]
        input_files_txt = [ line.strip() for line in file('input.txt') ]
    except:
        lfns = []
        guids = []
        input_files_txt = []
                
    ddmFileMap = {}
    for i in xrange(0,len(lfns)):
        ddmFileMap[lfns[i]] = guids[i]

    from dq2.clientapi.DQ2 import DQ2
    dq2=DQ2()
    duid = dq2.listDatasets(datasetname)[datasetname]['duid']
    contents = dq2.listFilesInDataset(datasetname)[0]

    protpat = re.compile(r'^(\w+):/')
    filenamepat = re.compile(r'.*/(.+)$')    

    # Fail over for empty input_files
    if not input_files:
        input_files = input_files_txt
        
    for longfilename in input_files:
        prot = re.findall(protpat, longfilename)
        if prot:
            prot = prot[0]
        else:
            prot = 'gridcopy'
        
        lfn = re.findall(filenamepat, longfilename)
        if lfn:
            lfn = lfn[0]
            lfn = re.sub('^tcf_','',lfn)
            lfn = re.sub('(__DQ2.*)$','',lfn)
        
        report = fill_report(eventVersion  = ganga_version,
                             remoteSite    = siteid,
                             localSite     = siteid,
                             timeStart     = tracertimes[0],
                             catStart      = tracertimes[1],
                             relativeStart = tracertimes[2],
                             transferStart = tracertimes[3],
                             timeEnd       = starttime,
                             dataset       = datasetname,
                             protocol      = prot,
                             filename      = lfn,
                             guid = ddmFileMap[lfn],
                             duid = duid,
                             filesize = contents[ddmFileMap[lfn]]['filesize'],
                             version = 0,
                             usr = usrhex
                             )
        #print report
        try:
            TracerClient().addReport(report)
        except:
            print 'An Error during TracerClient().addReport(report) occured'
            print report
            pass

    print '%s DQ2 tracer file reports have been sent.' %(len(input_files))
    print '--------------'

