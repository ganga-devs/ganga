#! /usr/bin/env python
#
# ganga-readlfc.py
#
#
import os, re, commands, sys, getopt, signal

EC_Configuration = 410100

try:
    # ignore Python C API version mismatch
    sys.stderr = open("/dev/null", "w")
    # import
    import lfc
except:
    print 'ERROR: Could not import lfc' 
    sys.exit(EC_Configuration)

# repair stderr
sys.stderr = sys.__stderr__

try:
    sys.stderr = open("/dev/null", "w")
    import dq2.info.TiersOfATLAS as TiersOfATLAS
    from dq2.clientapi.DQ2 import DQ2
    from dq2.common.dao.DQDaoException import DQDaoException
    from dq2.info.TiersOfATLAS import _refreshToACache, ToACache
except:
    print 'ERROR: Could not import DQ2' 
    sys.exit(EC_Configuration)

# repair stderr
sys.stderr = sys.__stderr__

import random

_refreshToACache()
verbose = False

def handler(signum, frame):
    print "lfc.lfc_getreplicas timeout!"
    
##############################
def setLfcHost(lfcstring):
    # Set the LFC info
    # log.info('LFC endpoint is %s'%lfcstring)
    # Must be of the form  lfc://lfc0448.gridpp.rl.ac.uk:/grid/atlas
    lfc_re = re.compile('^lfc://([\w\d\-\.]+):([\w\-/]+$)')
    lfcre = lfc_re.match(lfcstring)
    # Set just the environ. It`s used be lfc api
    if lfcre:
        lfctup = {}
        lfctup['host'] = lfcre.group(1)
        realtime = commands.getoutput('date')
        #print realtime
        #print 'Setting env LFC_HOST to %s'%lfcre.group(1)
        os.environ['LFC_HOST'] = lfcre.group(1)
        # Root path, usually '/grid/atlas'
        lfctup['root'] = lfcre.group(2)
        # log.info('Setting env LFC_HOME to %s'%lfcre.group(2))
        os.environ['LFC_HOME'] = lfcre.group(2)
    else:
        print 'Cannot parse LFC string: %s'%lfcstring
        return 'WRAPLCG_WNCHECK_LFCSTRING'
    # Force lcg-utils to use LFC
    os.environ['LCG_CATALOG_TYPE']='lfc'
    return 'OK'

##############################
def getreplicas_bulk(guids, allLFCs):
    """
    allLFCs=[]
    for cl in TiersOfATLAS.ToACache.dbcloud:
        id=TiersOfATLAS.ToACache.dbcloud[cl]
        l = TiersOfATLAS.getLocalCatalog(id)
        if l:
            allLFCs += [l]
    # shuffle these
    random.shuffle(allLFCs)
    # Add the old central LFC last - only use if no-other replica found
    centralLFC = TiersOfATLAS.getLocalCatalog('OLDLCG')
    allLFCs += [ centralLFC ]
    #print allLFCs
    """
    
    guidSizes    = {}
    guidReplicas = {}
    guidmd5sum = {}
    for lfcstring in allLFCs:
        if setLfcHost(lfcstring) == 'OK':  
            print lfcstring
            sys.stdout.flush()
            signal.signal(signal.SIGQUIT, handler)
            signal.alarm(30)
            try:
                (res, rep_entries) = lfc.lfc_getreplicas(guids, '')
            except AttributeError:
                print 'ERROR LCG UI does not support LFC bulk reading - please upgrade !'
                sys.exit(EC_Configuration)
                
            signal.alarm(0)
                   
            if res != 0:  
                print 'lfc_getreplicas :  Error ' + str(res) + "\n"
            else:
                done = False  
                for rep in rep_entries:
                    if rep.errcode == 0 and rep.sfn!='':
                        if rep.sfn.startswith("srm"):

                            # Add 8443 port
                            surl = rep.sfn.split("//")
                            surl[1]= rep.sfn.split("//")[1].split("/")[0]+":8443/"+'/'.join(rep.sfn.split("//")[1].split("/")[1:])
                            surl_8443 = '//'.join(surl)
                            # Use original sfn
                            surl_8443 = rep.sfn
                        else:
                            surl = rep.sfn.split("//")
                            surl[0]= 'gsiftp:'
                            surl[1]= rep.sfn.split("//")[1].split("/")[0]+":2811/"+'/'.join(rep.sfn.split("//")[1].split("/")[1:])
                            surl_8443 = '//'.join(surl)
          
                        surl_8443 = re.sub(':*\d*/srm/managerv2\?SFN=','', surl_8443 )
                        if rep.guid in guidReplicas.keys():
                            guidReplicas[rep.guid].append(surl_8443) 
                        else:
                            guidSizes[rep.guid] = rep.filesize
                            #guidReplicas[rep.guid] = [rep.sfn] 
                            guidReplicas[rep.guid] = [surl_8443]
                            guidmd5sum[rep.guid] = rep.csumvalue
  
    return 'OK',guidReplicas, guidSizes,guidmd5sum

##############################
def getinputreplicas(lfn_guid, allLFCs):

    exitcode, guidReplicas,guidSizes,guidmd5sum = getreplicas_bulk(lfn_guid.values(), allLFCs)

    for lfn,guid in lfn_guid.items():
        if guid not in guidReplicas.keys():
            #print 'Fail to get replicas for inputfile '+guid
            #print 'Try single query. '
            return 'Not Found',  guidReplicas,guidSizes,guidmd5sum
    
    retcode = pre_stagein(guidReplicas)
    if retcode !='OK':
        return 'Not Pre-stage', guidReplicas,guidSizes,guidmd5sum        
    else:
        return 'OK', guidReplicas,guidSizes,guidmd5sum
    
##############################
def pre_stagein(guidReplicas):

    site = commands.getoutput('/bin/hostname')
    if site.find('cern.ch'):
        for guid in guidReplicas.keys():
            for surl in guidReplicas[guid]:
                if surl.split('//')[1].startswith('srm.cern.ch'):
                    #file need to be pre-stagein
                    castor_path = '/'+'/'.join(surl.split('//')[1].split("/")[1:])
                    cmd = 'stager_get -M '+castor_path
                    (exitcode, output) = commands.getstatusoutput(cmd)
                    if exitcode == 0: # if fail to stager_get
                        cmd = 'stager_qry -M '+castor_path
                        (exitcode, output) = commands.getstatusoutput(cmd)
                        if exitcode == 0: # if fail to stager_qry
                            print castor_path+' has been pre-staged.'        
                        else:
                            return 'Not OK' 
                    else:
                        return 'Not OK'                  
                    
                    break
        return 'OK'
    else:
        "For submit node out of cern, pre-stagein is not needed." 

#####################################
def _usage():
    print """ganga-readlfc.py datasetname complete=0/1"""

#####################################
if __name__ == '__main__':

    dataset = ''
    complete = -1
    removefromlfclist = [ ]
    list = False
    list_guids = False

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hvr:lg", ["help","verbose","remove","list","list_guids"])
    except:
        _usage()
        print "ERROR : Invalid options"
        sys.exit(EC_Configuration)    

    # set options
    for o, a in opts:
        if o in ("-h","--help"):
            _usage()
            sys.exit(EC_Configuration)
        if o in ("-v","--verbose"):
            verbose = True
        if o in ("-l","--list"):
            list = True
        if o in ("-g","--list_guids"):
            list = True
            list_guids = True
        if o in ("-r","--remove"):
            removefromlfclist = a.split(",")

    if args and len(args)>1:
        dataset = args[0]
        complete = int(args[1])
    else:
        print 'ERROR no dataset name given !'
        sys.exit(EC_Configuration)

    dq2=DQ2()
    content = dq2.listDatasets(dataset)
    if content=={}:
        print 'ERROR Dataset %s is not defined in DQ2 database !' %dataset
        sys.exit(0)
                                                                   

    contents = dq2.listFilesInDataset(dataset)
    if contents:
        contents = contents[0]
    else:
        contents = {}

    if complete==1:
        locations = dq2.listDatasetReplicas(dataset, complete=1)
    elif complete==0:
        locations = dq2.listDatasetReplicas(dataset, complete=0)
    else:
        locations = dq2.listDatasetReplicas(dataset)
    datasetinfo = dq2.listDatasets(dataset)
    datasetvuid = datasetinfo[dataset]['vuids'][0]

    if locations:
        locations = locations[datasetvuid]
    else:
        print 'ERROR no location'
        sys.exit(0)

    if complete == -1: 
        locrange = xrange(0,2)
    elif complete == 0: 
        locrange = xrange(0,1)
    elif complete == 1:
        locrange = xrange(1,2)

    locnum = 0
    for i in locrange:
        locnum += len(locations[i])
    if locnum == 0:
        print 'ERROR no location'
        sys.exit(0)
            
    lfn_guid = {}
    for guid, info in contents.iteritems():
        lfn_guid[info['lfn']] = guid

    allLFCs = []


    for i in locrange:
        for location in locations[i]:
            l = TiersOfATLAS.getLocalCatalog(location)
            if l and l not in allLFCs and l.startswith('lfc') and l not in removefromlfclist:
                allLFCs.append(l)
            

    status, guidReplicas, guidSizes, guidmd5sum = getinputreplicas(lfn_guid, allLFCs)

    print guidReplicas
    locations_srm = {}
    for i in locrange:
        for location in locations[i]:
            try:
                if 'srm' in TiersOfATLAS.ToACache.sites[location]:
                    tempsrm = TiersOfATLAS.ToACache.sites[location]['srm']
                    tempsrm = re.sub('token:*\w*:','',tempsrm)
                    tempsrm = re.sub(':*\d*/srm/managerv2\?SFN=','', tempsrm)
                    print tempsrm
                    locations_srm[location]=tempsrm
            except KeyError:
                pass
            
    locations_num = {}
    location_list = {}
    guid_list = {}
    for i in locrange:
        for location in locations[i]:
            locations_num[location]=0
            location_list[location] = []

    for guid, surllist in guidReplicas.iteritems():
        for location, location_srm in locations_srm.iteritems():
            for surl in surllist:
                if surl.startswith(location_srm):
                    locations_num[location]=locations_num[location]+1
                    break
                elif location=='DESY-HH' and surl.startswith("srm://srm-dcache.desy.de"):
                    locations_num['DESY-HH']=locations_num['DESY-HH']+1
                    break
                
    for guid, surllist in guidReplicas.iteritems():
        guid_list[guid] = []
        for location, location_srm in locations_srm.iteritems():
            for surl in surllist:
                if surl.startswith(location_srm):
                    item = [ guid, surl ]
                    newitem = location_list[location] + item 
                    location_list[location] = newitem
                    guid_list[guid].append( location )

    if not list:
        for location, num in locations_num.iteritems():
            print '#%s:%s' %(location, num)
            sys.stdout.flush()
    else:
        if not list_guids:
            for location, file in location_list.iteritems():
                line = '#'+location
                for ifile in file: 
                    line = line + ',%s' %ifile
                print line
                sys.stdout.flush()
        else:
            for guid, locations in guid_list.iteritems():
                line = '#'+guid
                for location in locations: 
                    line = line + ',%s' %location
                print line
                sys.stdout.flush()
