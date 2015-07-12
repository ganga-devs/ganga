#!/usr/bin/env python

import os, re, time, commands, socket
from dq2.tracer.client.TracerClient import TracerClient
from dq2.common import generate_uuid

try:
    import hashlib
    md = hashlib.md5()
except ImportError:
    # for Python << 2.5
    import md5
    md = md5.new()

########################################################################
def fill_report(uuid = None, eventVersion = None, remoteSite=None,
                localSite=None, timeStart=None, catStart=None,
                relativeStart=None, transferStart=None, validateStart=None,
                timeEnd=None, duid=None,version=None, dataset=None,
                protocol=None, filename=None, filesize=None,
                guid=None, usr=None, 
                hostname=None, ip=None, suspicious=0, appid=None, usrdn=None  ):
    if uuid:
        uuid = uuid.replace('-','')
    if duid:
        duid = duid.replace('-','')
    if guid:
        guid = guid.replace('-','')
    if usr:
        usr = usr.replace('-','')
    report = {
        'uuid': uuid,
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
        'duid': duid,
        'version': version,
        'dataset': dataset,
        'clientState': 'INIT_REPORT',
        'protocol': protocol,
        'filename': filename,
        'filesize': filesize,
        'guid': guid,
        'usr': usr, 
        'hostname': hostname,
        'ip': ip,
        'suspicious': suspicious, 
        'appid': appid,
        'usrdn': usrdn 
    }
    
    return report

########################################################################
if __name__ == '__main__':

    starttime = time.time()
    print '--------------'
    print 'DQ2 tracer preparation ...'

    usrDN = commands.getstatusoutput('voms-proxy-info -identity')[1]
    # Event uuid
    uuid = generate_uuid()
    # Read LFC and transfer time produced in ganga-stage-in-out-dq2.py
    # or make-filestager-joboption.py
    tracertimes = [ line.strip() for line in file('dq2tracertimes.txt') ]
    # User hash
    #m=md5.new()
    md.update(usrDN)
    usrhex = md.hexdigest()

    numfiles3 = 0
    filepat = '"PFN:(.+)"'
    selectorpat = 'ServiceMgr.EventSelector.InputCollections = \[(.+),\]'
    selectorpat2 = 'mySampleList = \[(.+)\]'
    sitepat = 'detected DQ2_LOCAL_SITE_ID: (.+)'
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
                if filename and not filename in input_files:
                    input_files.append(filename)
                    numfiles3 = numfiles3 + 1
                    
            match = re.search(sitepat, line)       
            if match:
                siteid = match.group(1)
                print siteid
                
            match = re.search(datasetnamepat, line)       
            if match:
                datasetname = match.group(1)

        if zfile:        
            zfile.close()

    # Read in info from athena summary AthSummary.txt and use this info 
    athinfo = {}
    if 'AthSummary.txt' in os.listdir('.'):  
        readline = False
        zfile = os.popen('cat '+os.path.join('.','AthSummary.txt' ))
        for line in zfile:
            if line.find('Files read:')==0:
                numfiles3 = int(re.match('Files read:(.*)',line).group(1))
            if line.find('Exit Status:')==0:
                readline=True
                continue
            try:
                if readline:
                    athinfo = eval(line)
                    readline = False
            except:
                pass

        if zfile:        
            zfile.close()

        try:
            dum = athinfo['files']['read']
            if dum:
                input_files = dum.split(',')
        except:
            pass

    print '%s out of %s file of dataset %s have been processed.' %(numfiles3, len(input_files_sel), datasetname)
    print 'Number could be wrong for certain input modes!'
    
    if 'GANGA_VERSION' in os.environ:
        ganga_version = os.environ['GANGA_VERSION']
    else:
        ganga_version = 'GANGA-5'

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

    #from dq2.clientapi.DQ2 import DQ2
    #dq2=DQ2()
    #duid = dq2.listDatasets(datasetname)[datasetname]['duid']

    protpat = re.compile(r'^(\w+):/')
    filenamepat = re.compile(r'.*/(.+)$')    

    # Fail over for empty input_files
    if not input_files:
        input_files = input_files_txt

    # Determine new variables
    hostName = socket.gethostbyaddr(socket.gethostname())[0]
    ipAddr = socket.gethostbyaddr(socket.gethostname())[2][0]

    nreport = 0

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
        else:
            lfn = longfilename

        report = fill_report(uuid = uuid,
                             eventVersion  = ganga_version,
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
                             #duid = duid,
                             #filesize = contents[ddmFileMap[lfn]]['filesize'],
                             duid = None,
                             filesize = -1,
                             version = 0,
                             usr = usrhex,
                             hostname = hostName,
                             ip = ipAddr,
                             suspicious = 0, 
                             appid = None,
                             usrdn = usrDN
                             )
        #print report
        try:
            #TracerClient().addReport(report)
            nreport+=1
        except:
            print 'An Error during TracerClient().addReport(report) occured'
            print report
            pass

    print '%s DQ2 tracer file reports have been sent.' %nreport
    print '--------------'

