#! /usr/bin/env python
#
# determine SE type
#

import os, urllib, sys, imp, commands, re, socket

# error codes
# WRAPLCG_UNSPEC
EC_UNSPEC        = 410000
# WRAPLCG_WNCHECK_UNSPEC
EC_Configuration = 410100
# WRAPLCG_STAGEIN_UNSPEC
EC_QueryFiles    = 410300
# WRAPLCG_STAGEIN_LCGCP
EC_DQ2GET        = 410302
# WRAPLCG_WNCHECK_PROXY
EC_PROXY         = 410101
# WRAPLCG_STAGEOUT_UNSPEC
EC_STAGEOUT      = 410400


########################################################################
def getTiersOfATLASCache():
    """Download TiersOfATLASCache.py"""
    
    url = 'http://atlas.web.cern.ch/Atlas/GROUPS/DATABASE/project/ddm/releases/TiersOfATLASCache.py'
    local = os.path.join(os.environ['PWD'],'TiersOfATLASCache.py')
    try:
        urllib.urlretrieve(url, local)
    except IOError:
        print 'Failed to download TiersOfATLASCache.py'
        
    try:
        tiersofatlas = imp.load_source('',local)
    except SyntaxError:
        print 'Error loading TiersOfATLASCache.py'
        sys.exit(EC_UNSPEC)

    return tiersofatlas

##########################
def findsetype(sitesrm):
    setype= 'NULL'
    
    if sitesrm.find('castor')>=0:
        setype = 'CASTOR'
    elif sitesrm.find('dpm')>=0:
        setype = 'DPM'
    elif sitesrm.find('pnfs')>=0:
        setype = 'DCACHE'
    elif sitesrm.find('/nfs/')>=0:
        setype = 'NFS'

    return setype

##########################
def localsitename():
    # Determine local domainname
    hostname = None
    domainname = None
    # First choice: EDG_WL_RB_BROKERINFO or GLITE_WMS_RB_BROKERINFO
    if 'EDG_WL_RB_BROKERINFO' in os.environ:
        try:
            f = open(os.environ['EDG_WL_RB_BROKERINFO'], "r")
            lines = f.readlines()
            for line in lines:
                match = re.search('name = "(\S*):2119', line)
                if match:
                    hostname =  [ match.group(1) ]
        except:
            pass

    if 'GLITE_WMS_RB_BROKERINFO' in os.environ:
        try:
            f = open(os.environ['GLITE_WMS_RB_BROKERINFO'], "r")
            lines = f.readlines()
            for line in lines:
                match = re.search('name = "(\S*):2119', line)
                if match:
                    hostname =  [ match.group(1) ]
        except:
            pass

    # Second choice: GANGA_LCG_CE
    if not hostname and 'GANGA_LCG_CE' in os.environ:
        try:
            hostname = re.findall('(\S*):2119',os.environ['GANGA_LCG_CE'])
            lcgcename = re.findall('(\S*):2119',os.environ['GANGA_LCG_CE'])[0]
            #print hostname, lcgcename
        except:
            pass

    # Third choice: VO_ATLAS_DEFAULT_SE
    if not hostname and 'VO_ATLAS_DEFAULT_SE' in os.environ:
        hostname = os.environ['VO_ATLAS_DEFAULT_SE']
        if hostname.find('grid.sara.nl')>=0: hostname = ''
            
    # Fourth choice: local hostname
    if not hostname:
        hostname = socket.gethostbyaddr(socket.gethostname())

    if hostname.__class__.__name__=='list' or hostname.__class__.__name__=='tuple':
        hostname = hostname[0]

    domainname = re.sub('^[\w\-]+\.','',hostname)
    if domainname=='gridka.de':
        domainname = 'fzk.de'

    tiersofatlas = getTiersOfATLASCache()
    localsites = []
    localsiteseq = []

    # See if local domainname is in TiersOfAtlasCache.py
    for site, desc in tiersofatlas.sites.iteritems():
        sitename = desc['domain'].strip('.*')
        if sitename=='':
            continue
        try:
            srm = desc['srm'].strip()
        except KeyError:
            continue
        pat1 = re.compile(sitename)
        found1 = re.findall(pat1,domainname)
        pat2 = re.compile(domainname)
        found2 = re.findall(pat2,sitename)
        if sitename == domainname: 
            localsites.append(site)
            localsiteseq.append(site)
        elif found1 or found2:
            localsites.append(site)

    # Get location list of dataset
    try:
        datasetlocation = os.environ['DATASETLOCATION'].split(":")
    except:
        #print "ERROR : DATASETLOCATION not defined"
        datasetlocation = []
        pass

    # Choose local site configuration (if possible eq datasetlocation)
    localsite = ''
    # 1st pick: if domainname eq sitename(ToA)  
    for site in localsiteseq:
        localsite = site
        if site in datasetlocation:
            break
    # 2nd pick: if domainname is in sitename(ToA) or the other way around  
    if not localsite:
        for site in localsites:
            localsite = site
            if site in datasetlocation:
                break

    localsiteid = localsite 
    # change CERNCAF to CERN
    localsiteid = re.sub('^CERNCAF$','CERN',localsiteid)
    # change TIER0 to CERN
    localsiteid = re.sub('^TIER0','CERN',localsiteid)
    # remove TAPE
    localsiteid = re.sub('TAPE$','',localsiteid)
    # remove DISK
    localsiteid = re.sub('DISK$','',localsiteid)
    # remove PANDA
    localsiteid = re.sub('PANDA$','',localsiteid)

    return localsiteid

##########################
if __name__ == '__main__':

    setype = 'NULL'
    
    tiersofatlas = getTiersOfATLASCache()

    if 'VO_ATLAS_DEFAULT_SE' in os.environ:
        sitese=os.environ['VO_ATLAS_DEFAULT_SE']
        #print 'ganga_setype.py: VO_ATLAS_DEFAULT_SE=%s'%sitese
    elif sys.argv[1:]:
        sitese=sys.argv[1]
    else:
        print 'ERROR no SE specified !'
        sys.exit(4)

    # Find TiersOfAtlasCache entry
    if sitese:
        for site, desc in tiersofatlas.sites.iteritems():
            try:
                sitesrm = desc['srm'].strip()
            except KeyError:
                continue
            if sitesrm.find(sitese)>=0:
                setype = findsetype(sitesrm)

            if setype == 'NULL':
                sitesrm_domain = re.sub('^[\w\-]+\.','',sitesrm)
                sitese_domain = re.sub('^[\w\-]+\.','',sitese)
                if sitesrm_domain.find(sitese_domain)>=0:
                    setype = findsetype(sitesrm_domain)


    # If not in TiersOfAtlasCache
    if setype == 'NULL':
        cmd = 'lcg-info --list-se --query SE=%s --attr Accesspoint --sed 2>/dev/null' %sitese
        rc, out = commands.getstatusoutput(cmd)
        out2 = out.split('%')
        if len(out2)>1:
            setype = findsetype(out2[1])

    localsiteid = localsitename()
    if localsiteid == 'RAL':
        setype = 'DCACHE'
    if localsiteid == 'NIKHEF':
        setype = 'DPM'
    
        
    print setype
    
