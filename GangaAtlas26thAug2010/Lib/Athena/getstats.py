#!/usr/bin/env python

import os, re, pickle

########################################################################
if __name__ == '__main__':

    stats = {}

    print '-------------------'
    print 'getstats called ...'

    # Determine dataset access mode
    try:
        datasettype = os.environ['DATASETTYPE']
    except:
        print "ERROR : DATASETTYPE not defined, using DQ2_LOCAL"
        datasettype = 'DQ2_LOCAL'
        pass
    # use DQ2_LOCAL as default
    if not datasettype in [ 'DQ2_LOCAL', 'DQ2_COPY', 'FILE_STAGER' ]:
        datasettype = 'DQ2_LOCAL'

    numfiles3 = 0
    filepat = '"PFN:(.+)"'
    selectorpat = 'ServiceMgr.EventSelector.InputCollections = \[(.+),\]'
    selectorpat2 = 'mySampleList = \[(.+)\]'
    sitepat = 'DQ2_LOCAL_SITE_ID: (.+)'
    datasetnamepat = '^DATASETNAME=(.+)'
    
    input_files = []
    input_files_sel = []

    # collect stats from stderr
    percentcpu = 0
    ipercentcpu = 0
    wallclock = 0
    usertime = 0
    systemtime = 0
    try:
        if 'stderr' in os.listdir('.'):  
            zfile = os.popen('cat '+os.path.join('.','stderr' ))
            for line in zfile:
                
                if line.find('Percent of CPU this job got')>-1:
                    try:
                        percentcpu = percentcpu + int(re.match('.*got: (.*).',line).group(1))
                    except ValueError:
                        percentcpu = 0  
                    ipercentcpu = ipercentcpu + 1
                if line.find('Elapsed (wall clock) time')>-1:
                    try:
                        iwallclock = re.match('.*m:ss\): (.*)\.\d\d',line).group(1).split(':')
                        wallclock = wallclock + int(iwallclock[0])*60+int(iwallclock[1])
                    except:
                        iwallclock = re.match('.*m:ss\): (.*)',line).group(1).split(':')
                        wallclock = wallclock + int(iwallclock[0])*3600+int(iwallclock[1])*60+int(iwallclock[2])
                if line.find('User time (seconds)')>-1:
                    iusertime = float(re.match('.*User time \(seconds\): (.*)',line).group(1))
                    usertime = usertime + iusertime
                if line.find('System time (seconds)')>-1:
                    isystemtime = float(re.match('.*System time \(seconds\): (.*)',line).group(1))
                    systemtime = systemtime + isystemtime
                if line.find('Exit status')>-1:
                    stats['exitstatus'] = re.match('.*status: (.*)',line).group(1)
                if line.find('can not be opened for reading (Timed out)')>-1:
                    stats['filetimedout'] = True

            if ipercentcpu > 0:            
                stats['percentcpu'] = percentcpu / ipercentcpu
                stats['usertime'] = usertime
                stats['systemtime'] = systemtime
                stats['wallclock'] = wallclock
            else:
                stats['percentcpu'] = 0
                stats['wallclock'] = 0
                stats['usertime'] = 0
                stats['systemtime'] = 0
            if zfile:        
                zfile.close()

    except MemoryError:
        print 'ERROR stderr logfiles too large to be parsed.'
        pass
    
    # collect stats from stdout
    totalevents = 0
    itotalevents = 0
    jtotalevents = 0
    numfiles = 0
    numfiles2 = 0
    numfiles3 = 0
    try:
        if 'stdout' in os.listdir('.'):  
            zfile = os.popen('cat '+os.path.join('.','stdout' ))
            for line in zfile:
                if line.find('Storing file at:')>-1:
                    stats['outse'] = re.match('.*at: (.*)',line).group(1)
                if line.find('SITE_NAME=')>-1:
                    stats['site'] = re.match('SITE_NAME=(.*)',line).group(1)
                #if line.find('Database being retired...')>-1:
                #    stats['dbretired'] = True
                if line.find('Core dump from CoreDumpSvc')>-1:
                    stats['coredump'] = True
                if line.find('Cannot load entry')>-1:
                    stats['cannotloadentry'] = True
                if line.find('cannot open a ROOT file in mode READ if it does not exists')>-1:
                    stats['filenotexist'] = True
                if line.find('FATAL finalize: Invalid state "Configured"')>-1:
                    stats['invalidstateconfig'] = True
                if line.find('failure in an algorithm execute')>-1:
                    stats['failalg'] = True
                if line.find('events processed so far')>-1:
                    try:
                        itotalevents = int(re.match('.* run #\d+ (\d+) events processed so far.*',line).group(1))
                        jtotalevents = int(itotalevents)
                    except:
                        pass
                if line.find('cObj_DataHeader...')>-1:
                    numfiles2 = numfiles2 + int(re.match('.* #=(.*)',line).group(1))
                if line.find('"PFN:')>-1:
                    numfiles3 = numfiles3 + 1
                if line.find('rfio://')>-1 and line.find('Always Root file version')>-1:
                    try:
                        stats['server'] = re.match('(.+://.+)//.*',line).group(1)
                    except:
                        stats['server'] = 'unknown'

                if line.find('Info Database being retired...')>-1:
                    numfiles = numfiles + 1
                    totalevents = totalevents + itotalevents
                    itotalevents = 0
                if line.find('GANGATIME1')==0:
                    stats['gangatime1'] = int(re.match('GANGATIME1=(.*)',line).group(1))
                if line.find('GANGATIME2')==0:
                    stats['gangatime2'] = int(re.match('GANGATIME2=(.*)',line).group(1))
                if line.find('GANGATIME3')==0:
                    stats['gangatime3'] = int(re.match('GANGATIME3=(.*)',line).group(1))
                if line.find('GANGATIME4')==0:
                    stats['gangatime4'] = int(re.match('GANGATIME4=(.*)',line).group(1))
                if line.find('GANGATIME5')==0:
                    stats['gangatime5'] = int(re.match('GANGATIME5=(.*)',line).group(1))

                try:
                    if line.find('NET_ETH_RX_PREATHENA')==0:
                        stats['NET_ETH_RX_PREATHENA'] = int(re.match('NET_ETH_RX_PREATHENA=(.*)',line).group(1))
                    if line.find('NET_ETH_RX_AFTERATHENA')==0:
                        stats['NET_ETH_RX_AFTERATHENA'] = int(re.match('NET_ETH_RX_AFTERATHENA=(.*)',line).group(1))
                except:
                    stats['NET_ETH_RX_PREATHENA'] = 0
                    stats['NET_ETH_RX_AFTERATHENA'] = 0

                try:
                    if line.find('### node info:')==0:
                        stats['arch'] = re.match('### node info: .*,(.*, .*),.*,.*,.*,.*', line).group(1).strip()
                except:
                    stats['arch'] = ''

            stats['numfiles2'] = numfiles2

            if datasettype == 'DQ2_COPY':
                stats['numfiles'] = numfiles / 2
                stats['totalevents'] = totalevents
                stats['numfiles3'] = numfiles3 / 2
            elif datasettype == 'FILE_STAGER':
                stats['numfiles'] = (numfiles - 2)/2
                stats['totalevents'] = jtotalevents
                stats['numfiles3'] = numfiles3 - 1
            else:
                stats['numfiles'] = numfiles - 1
                stats['totalevents'] = jtotalevents
                stats['numfiles3'] = numfiles3 - 1

            if zfile:        
                zfile.close()

    except MemoryError:
        print 'ERROR stderr logfile too large to be parsed.'
        pass

    # collect stats from AthSummary.txt
    try:
        if 'AthSummary.txt' in os.listdir('.'):  
            zfile = os.popen('cat '+os.path.join('.','AthSummary.txt' ))
            for line in zfile:
                if line.find('Files read:')==0:
                    stats['numfiles'] = int(re.match('Files read:(.*)',line).group(1))
                    stats['numfiles2'] = stats['numfiles'] 
                    stats['numfiles3'] = stats['numfiles'] 
                if line.find('Events Read:')==0:
                    stats['totalevents'] = int(re.match('Events Read:(.*)',line).group(1))

            if zfile:        
                zfile.close()

    except MemoryError:
        print 'ERROR AthSummary.txt logfile too large to be parsed.'
        pass

    print stats

    f = open('stats.pickle','w')
    pickle.dump(stats,f)
    f.close()

    print 'getstats finished...' 
    print '-------------------'

