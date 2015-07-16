import re, os, sys, types

from Ganga.Core.exceptions import BackendError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG import LCGRequirements

from dq2.info.TiersOfATLAS import _refreshToACache, ToACache, _resolveSites, getSites 

logger = getLogger()

config = getConfig('Athena')

try:
    excluded_sites = config['ExcludedSites'].split()
except ConfigError:
    excluded_sites = []

slc3_req = '''(( other.GlueHostOperatingSystemName  == "CentOS" || 
      other.GlueHostOperatingSystemName  == "RedHatEnterpriseAS" 
    ) &&
    ( other.GlueHostOperatingSystemRelease >= 3.0 && 
      other.GlueHostOperatingSystemRelease < 4.0 ) 
    ) ||
    ( other.GlueHostOperatingSystemName  == "Scientific Linux" || 
      other.GlueHostOperatingSystemName  == "Scientific Linux CERN"
    )'''

slc4_req = '''( other.GlueHostOperatingSystemName  == "CentOS" ||
     other.GlueHostOperatingSystemName  == "RedHatEnterpriseAS"  ||
     other.GlueHostOperatingSystemName  == "ScientificSL" ||
     other.GlueHostOperatingSystemName  == "ScientificCERNSLC" 
   ) &&
   ( other.GlueHostOperatingSystemRelease >= 4.0 &&
     other.GlueHostOperatingSystemRelease < 5.0 
   )'''

CESEInfo      = None
CESEInfoURL   = 'http://ganga.web.cern.ch/ganga/ATLAS/cese_info.dat.gz'
CESEInfoLocal = '/tmp/ganga.cese_info.dat.gz_%d' % os.getuid()

def _loadCESEInfo():

    import gzip
    import cPickle as pickle

    result = {}
    try:
        input = gzip.open(CESEInfoLocal,'rb')
        result['time']          = pickle.load(input)
        result['ce_info']       = pickle.load(input)
        result['se_info']       = pickle.load(input)
        result['lcg_site_info'] = pickle.load(input)
        try:
            result['blacklist'] = pickle.load(input)
        except EOFError:
            result['blacklist'] = []
        try:
            result['access_info'] = pickle.load(input)
        except EOFError:
            result['access_info'] = []
        try:
            result['blacklist_release_info'] = pickle.load(input)
        except EOFError:
            result['blacklist_release_info'] = []
        try:
            result['creamce_info']  = pickle.load(input)
        except EOFError:
            result['creamce_info']  = []
            
        input.close()
    except Exception:
        logger.error('Cannot read CE-SE association from file.')
        result = None

    return result

def _downloadCESEInfo():

    import urllib, socket, gzip
    from cStringIO import *
    import cPickle as pickle
    from stat import *

#   timeouts are not supported in python 2.2

    try:
        dto = socket.getdefaulttimeout()
        socket.setdefaulttimeout(10)
    except AttributeError:
        pass

    retry = 0
    data = None

    while not data and retry < 3:
        retry += 1
        try:
            data = urllib.urlopen(CESEInfoURL).read()
        except Exception as e:
            logger.warning(e)
            pass

    try:    
        socket.setdefaulttimeout(dto)
    except AttributeError:
        pass

    if not data:
        logger.error('Could not download CE-SE association from the GANGA website.')
        return None

    result = {}
    try:
        input = gzip.GzipFile(fileobj=StringIO(data))
        result['time']          = pickle.load(input)
        result['ce_info']       = pickle.load(input)
        result['se_info']       = pickle.load(input)
        result['lcg_site_info'] = pickle.load(input)
        try:
            result['blacklist'] = pickle.load(input)
        except EOFError:
            result['blacklist'] = []
        try:
            result['access_info'] = pickle.load(input)
        except EOFError:
            result['access_info'] = []
        try:
            result['blacklist_release_info'] = pickle.load(input)
        except EOFError:
            result['blacklist_release_info'] = []
        try:
            result['creamce_info']  = pickle.load(input)
        except EOFError:
            result['creamce_info']  = []
        input.close()
    except Exception as e:
        logger.error(e)
        logger.error('Cannot read CE-SE association downloaded from the GANGA website..')
        return None
    
    local = open(CESEInfoLocal,'wb')
    local.write(data)
    local.close()
    os.chmod(CESEInfoLocal,S_IRUSR | S_IWUSR)

    return result

def _refreshCESEInfo():
    '''Refresh CE-SE association information'''

    import time
    from stat import *

    global CESEInfo

    if CESEInfo and time.time() - CESEInfo['time'] < 3600: return

    CESEInfo = None

    if os.path.exists(CESEInfoLocal):
        ctime = os.stat(CESEInfoLocal)[ST_CTIME]
        if time.time() - ctime < 3600:
            logger.info('Reading local copy of CE-SE association file.')
            CESEInfo = _loadCESEInfo()
            if not CESEInfo:
                logger.debug('CE-SE association file is removed and a new copy will be downloaded.')
                os.unlink(CESEInfoLocal)
    
    retry = 0
    while not CESEInfo and retry < 3:
        retry += 1
        logger.info('Downloading remote copy of CE-SE association file.')
        CESEInfo = _downloadCESEInfo()
  
    if not CESEInfo:
        logger.warning('CE-SE association could not be downloaded. Using the old file')
        CESEInfo = _loadCESEInfo()
        if not CESEInfo:
            logger.debug('CE-SE association file is not usable.')
            os.unlink(CESEInfoLocal)

    else:
#       just warn if the file is older then one day
        if time.time() - CESEInfo['time'] > 3600*24:
            logger.error('CE-SE associations are stale. Please report the issue to the mailing list.')
        CESEInfo['time'] = time.time()
    
def getSEsForSites(ids):
    '''Retrieve the SEs for a site'''

    _refreshToACache()

#   sites can be a colon seperated list like 'CERN:LYON:ASGC'

    re_srm = re.compile('srm://([^/]+)(/.+)')
    se_dict = {}
    for id in ids:

        sites = _resolveSites(id.upper())
        if not sites:
            logger.error('Site %s not found in TiersOfATLAS',id)
            continue

        for site in sites:
            site_info = ToACache.sites[site]
            if 'srm' not in site_info: 
                logger.error('Site %s has no srm info in TiersOfATLAS',site)
                continue
            sitesrm = site_info['srm']
            if sitesrm == '': 
                logger.debug('Site %s has no srm info in TiersOfATLAS',site)
                continue
            sitesrm = re.sub('token:*\w*:','', sitesrm)
            sitesrm = re.sub(':*\d*/srm/managerv2\?SFN=','', sitesrm)
            match = re_srm.match(sitesrm)
            if not match:
                logger.debug('Cannot extract host from %',sitesrm)
                continue
            se_dict[match.group(1)] = True

    return se_dict.keys()

def getCEsForSites(ids, excluded_ids = [], CREAM = False, cputime=0 ):
    '''Retrieve the CEs for a site'''

    _refreshToACache()
    _refreshCESEInfo()

    re_srm = re.compile('srm://([^/]+)(/.+)')
    ce_dict = {}
    for id in ids:

#       allow the full use of all ToA names as FZKSITES etc.
 
        sites = _resolveSites([id.upper()])
        if not sites:
            logger.error('Site %s not found in TiersOfATLAS',id)
            continue

        # remove excluded sites
        try:
            excluded_sites = config['ExcludedSites'].split()
        except ConfigError:
            excluded_sites = [ ]

        if excluded_ids:
            excluded_sites = excluded_ids + excluded_sites 
        
        for site in excluded_sites:
            if site in sites:
                logger.warning('Site %s has been excluded.',site)
                sites.remove(site)

#       try to find CEs associated to a site via srm tag and/or associated names tag

        for site in sites:
            site_info = ToACache.sites[site] 
            ces = []
            if 'srm' in site_info:
                sitesrm = site_info['srm']
                if sitesrm == '': 
                    logger.debug('Site %s has no srm info in TiersOfATLAS',site)
                    continue
                sitesrm = re.sub('token:*\w*:','', sitesrm)
                sitesrm = re.sub(':*\d*/srm/managerv2\?SFN=','', sitesrm)
                match = re_srm.match(sitesrm)
                if not match:
                    logger.debug('Cannot extract host from %s',sitesrm)
                else:
                    try:
                        if CREAM:
                            ces = CESEInfo['se_info'][match.group(1)]['close_creamce']
                            if cputime > 0:
                                for ces_tmp in ces:
                                    cescputime = CESEInfo['creamce_info'][ces_tmp]['cputime']
                                    if cescputime:
                                        cescputime = int(cescputime)
                                    if cputime < cescputime:
                                        ces = [ ces_tmp ]
                                        break
                        else:
                            ces = CESEInfo['se_info'][match.group(1)]['close_ce']
                    except KeyError:
                        logger.debug('Did not find CE-SE association for %s',match.group(1))

            if not ces:
                try:
                    lcg_site = site_info['alternateName'][-1].upper()
                    ces = CESEInfo['lcg_site_info'][lcg_site]
                except Exception:
                    logger.debug('No CE information on site %s. Maybe it failes the SAM test.',site)

            for ce in ces: 
                ce_dict[ce] = True
                
    return ce_dict.keys()

def _resolveSites(sites):

    new_sites = []
    for site in sites:
        if site in ToACache.topology:
            new_sites += _resolveSites(ToACache.topology[site])
        else:
            new_sites.append(site)

    return new_sites

def getAllSites(only_lcg=True,resolve=False, excluded_clouds=[], excluded_sites=[], blacklist=True):
    '''list all sites defined in TiersOfATLAS'''

    _refreshToACache()
    _refreshCESEInfo()

    sites = []
    if not 'TO' in excluded_clouds:
        sites += getSites('CERN')
    if not 'IT' in excluded_clouds:
        sites += getSites('ITALYSITES')
    if not 'ES' in excluded_clouds:
        sites += getSites('SPAINSITES')
    if not 'FR' in excluded_clouds:
        sites += getSites('FRANCESITES')
    if not 'UK' in excluded_clouds:
        sites += getSites('UKSITES')
    if not 'DE' in excluded_clouds:
        sites += getSites('FZKSITES')
    if not 'NL' in excluded_clouds:
        sites += getSites('NLSITES')
    if not 'TW' in excluded_clouds:
        sites += getSites('TAIWANSITES')
    if not 'CA' in excluded_clouds:
        sites += getSites('CANADASITES')    

    if not only_lcg:
        sites += getSites('USASITES')
        sites += getSites('NDGF')

    # exclude sites
    for site in excluded_sites:
        if site in sites:
            sites.remove(site)
    
    if resolve: sites = _resolveSites(sites)

    # exclude sites - check again after site resolution
    for site in excluded_sites:
        if site in sites:
            sites.remove(site)

    sites.sort()

    if (blacklist):
        for bad_site in CESEInfo['blacklist']:
            try:
                sites.remove(bad_site)
            except ValueError:
                pass
     
    return sites

def listT2s(name,t1=None):

    t2s = ToACache.topology[name]
    try:
        if t1: t2s.remove(t1)
    except:
        pass

    return t2s

def getCloudInfo():
    '''get cloud information'''

    info =  [ ( 'T0', 'T0', 'CERN') ]
    info += [ ( 'T1', 'IT', 'CNAF' ) ]
    info += [ ( 'T2', 'IT', site ) for site in listT2s('ITALYSITES','CNAF') ]
    info += [ ( 'T1', 'ES', 'PIC' ) ]
    info += [ ( 'T2', 'ES', site ) for site in listT2s('SPAINSITES','PIC') ]
    info += [ ( 'T1', 'FR', 'LYON' ) ]
    info += [ ( 'T2', 'FR', site ) for site in listT2s('FRTIER2S') ]
    info += [ ( 'T1', 'UK', 'RAL' ) ]
    info += [ ( 'T2', 'UK', site ) for site in listT2s('UKTIER2S') ]
    info += [ ( 'T1', 'DE', 'FZK' ) ]
    info += [ ( 'T2', 'DE', site ) for site in listT2s('FZKSITES','FZK') ]
    info += [ ( 'T1', 'NL', 'SARA' ) ]
    info += [ ( 'T2', 'NL', site ) for site in listT2s('NLSITES','SARA') ]
    info += [ ( 'T1', 'TW', 'ASGC' ) ]
    info += [ ( 'T2', 'TW', site ) for site in listT2s('TAIWANSITES','ASGC') ]
    info += [ ( 'T1', 'CA', 'TRIUMF' ) ]
    info += [ ( 'T2', 'CA', site ) for site in listT2s('CANADASITES','TRIUMF') ]
    info += [ ( 'T1', 'US', 'BNL' ) ]
    info += [ ( 'T2', 'US', site ) for site in listT2s('USASITES','BNL') ] 
    info += [ ( 'T1', 'NG', 'NDGF' ) ] 
    
    return info

def getAccessInfo(sitename=''):
    """List prefered input access method of site"""

    _refreshCESEInfo()

    if CESEInfo['access_info'] and sitename in CESEInfo['access_info']:
        return CESEInfo['access_info'][sitename]
    else:
        return []

def getReleaseBlacklistInfo():
    """List release blacklist"""

    _refreshCESEInfo()

    if CESEInfo['blacklist_release_info']:
        return CESEInfo['blacklist_release_info']
    else:
        return []

class AtlasLCGRequirements(LCGRequirements):
    '''LCG requirements for ATLAS.

    See also: JDL Attributes Specification at http://cern.ch/glite/documentation
    '''

    _schema = Schema(Version(1,2), { 
        'software'        : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Software Installations'),
        'nodenumber'      : SimpleItem(defvalue=1,doc='Number of Nodes for MPICH jobs'),
        'memory'          : SimpleItem(defvalue=None, typelist=['type(None)','int'], doc='Mininum available memory (MB)'),
        'cputime'         : SimpleItem(defvalue=None, typelist=['type(None)','int'], doc='Minimum available CPU time (min)'),
        'walltime'        : SimpleItem(defvalue=None, typelist=['type(None)','int'], doc='Mimimum available total time (min)'),
        'ipconnectivity'  : SimpleItem(defvalue=False,doc='External connectivity'),
        'dq2client_version'  : SimpleItem(defvalue='',doc='DQ2 client version on the computing element'),
        'other'           : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Other Requirements'),
        'sites'           : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='ATLAS site names'),
        'excluded_sites'  : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='ATLAS site names to be excluded'),
        'excluded_clouds' : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='ATLAS cloud names to be excluded'),
        'cloud'           : SimpleItem(defvalue = 'ALL', doc='ATLAS cloud name: CERN, IT, ES, FR, UK, DE, NL, TW, CA, US, NG'),
        'anyCloud'        : SimpleItem(defvalue = True, doc='Set to True to allow cross-cloud submission. Use with the \'ALL\' cloud option.'),
        'os'              : SimpleItem(defvalue ='', doc='Operation Systems')
    })

    _category = 'LCGRequirements'
    _name = 'AtlasLCGRequirements'
    _exportmethods = ['list_ce', 'list_se','list_sites','list_clouds', 'list_sites_cloud', 'cloud_from_sites', 'list_access_info', 'list_release_blacklist' ]

    _cloudNameList = { 'T0' : 'CERN',
                       'IT' : 'ITALYSITES',
                       'ES' : 'SPAINSITES',
                       'FR' : 'FRANCESITES',
                       'UK' : 'UKSITES',
                       'DE' : 'FZKSITES',
                       'NL' : 'NLSITES',
                       'TW' : 'TAIWANSITES',
                       'CA' : 'CANADASITES',
                       'US' : 'USASITES',
                       'NG' : 'NDGF'
                       }
    
    _GUIPrefs = [ 
         { 'attribute' : 'software',       'widget' : 'String_List' },
         { 'attribute' : 'nodenumber',     'widget' : 'Int' },
         { 'attribute' : 'memory',         'widget' : 'Int' },
         { 'attribute' : 'cputime',        'widget' : 'Int' },
         { 'attribute' : 'walltime',       'widget' : 'Int' },
         { 'attribute' : 'ipconnectivity', 'widget' : 'Bool' },
         { 'attribute' : 'other',          'widget' : 'String_List' },
         { 'attribute' : 'sites',          'widget' : 'String_List' },
         { 'attribute' : 'cloud',          'widget' : 'String' },
         { 'attribute' : 'excluded_sites', 'widget' : 'String_List' },
         { 'attribute' : 'os',             'widget' : 'String' } 
    ]

    def __init__(self):
      
        super(AtlasLCGRequirements,self).__init__()

    def merge(self,other):
        '''Merge requirements objects'''
      
        if not other: return self
      
        merged = AtlasLCGRequirements()

        for name in self._schema.datadict.keys():
            try:
                attr = getattr(other,name)
            except AttributeError:
                attr = None
            if not attr: attr = getattr(self,name)
            setattr(merged,name,attr)
         
        return merged

    def convert(self):
        '''Convert the condition in a JDL specification'''
      
        requirements = super(AtlasLCGRequirements,self).convert()

        if self.sites:
            ce_requirement = ' ||\n     '.join([ 'other.GlueCEUniqueID == "%s"' % ce for ce in getCEsForSites(self.sites, self.excluded_sites)])
            if not ce_requirement:
                raise BackendError('LCG','Job cannot be submitted as no valid site has been specified.')
            requirements.append('( %s )' % ce_requirement)

        if self.os:
            os_name = self.os.lower()
            if os_name == 'slc3':
                requirements.append(slc3_req)
            elif os_name == 'slc4':
                requirements.append(slc4_req)
            else:
               raise BackendError('LCG','Job cannot be submitted as unknown OS %s has been requested.'%self.os)
            
        return requirements

    def list_se(self,ids):

        if isinstance(ids,str):
            ids = ids.split(':')

        return getSEsForSites(ids)

    def list_ce(self,ids):

        if isinstance(ids,str):
            ids = ids.split(':')

        return getCEsForSites(ids)

    def list_sites(self,only_lcg=True,resolve=False, req_str = ''):

        sites = getAllSites(only_lcg,resolve,self.excluded_clouds,self.excluded_sites)

        if (req_str != '') and (self._name == 'AtlasLCGRequirements'):
            # check release requirements
            old_software = self.software
            self.software = [req_str]
            be = self._getParent()
            matches = be.get_requirement_matches()

            # find the ces for each site
            new_sites = []
            for site in sites:
                ces = getCEsForSites([site])
                for ce in ces:
                    if ce in matches:
                        new_sites.append(site)
                        break

            sites = new_sites
            
        return sites

    def list_clouds(self):

        #return getCloudInfo()
        return self._cloudNameList.keys()

    def list_sites_cloud(self, cloudName='', blacklist=True, req_str = ''):


        if cloudName:
            cloudID = cloudName
        else:
            cloudID = self.cloud

        try:
            cloud = self._cloudNameList[cloudID]
        except:
            cloud = cloudID
           
        sites = getSites(cloud)
        
        # exclude sites
        for site in self.excluded_sites:
            if site in sites:
                sites.remove(site)

        # blacklist
        if (blacklist):
            _refreshCESEInfo()
            for bad_site in CESEInfo['blacklist']:
                try:
                    sites.remove(bad_site)
                except ValueError:
                    pass
                
        if (req_str != '') and (self._name == 'AtlasLCGRequirements'):
            # check release requirements
            old_software = self.software
            self.software = [req_str]
            be = self._getParent()
            matches = be.get_requirement_matches()

            # find the ces for each site
            new_sites = []
            for site in sites:
                ces = getCEsForSites([site])
                for ce in ces:
                    if ce in matches:
                        new_sites.append(site)
                        break

            sites = new_sites
            
        if sites:
            return sites
        raise BackendError('LCG','Could not find any sites for selected cloud %s. Allowed clouds: %s'%(cloud,self._cloudNameList.keys()))

    def cloud_from_sites( self, sites = [] ):
        
        # Return the cloud given the site
        if not sites:
            sites = self.sites

        if sites.__class__.__name__ == 'str':
            sites = [sites]

        if len(sites) == 0:
            return {}
        
        mapping = {}
        
        # sort info for site -> cloud
        for cloud in self.list_clouds():
            cloud_sites = getSites(self._cloudNameList[cloud])
            for site in cloud_sites:
                mapping[ site ] = cloud

        # now output the mapping requested
        output = {}
        for site in sites:
            try:
                output[site] = mapping[site]
            except:
                logger.warning("Could not find cloud associated with site '%s'." % site)
                if site == 'CERN-MOCKTEST':
                    output[site] = 'T0'

        return output
                                               
    def list_access_info(self,sitename=''):

        _refreshToACache()

        if self.sites:
            sitename = self.sites

        if sitename.__class__.__name__ == 'str':
            try:
                site_info = ToACache.sites[sitename]
                alternateName = site_info['alternateName'][-1].upper()
            except:
                alternateName = sitename
            return { alternateName : getAccessInfo(alternateName) }
        else:
            info = {} 
            for site in sitename:
                try:
                    site_info = ToACache.sites[site]
                    alternateName = site_info['alternateName'][-1].upper()
                except:
                    alternateName = site
                info[alternateName] = getAccessInfo(alternateName)
            return info
                
    def list_release_blacklist(self):
        return getReleaseBlacklistInfo()
            

atlascreamrequirements_schema_datadict = AtlasLCGRequirements._schema.inherit_copy().datadict

class AtlasCREAMRequirements(AtlasLCGRequirements):

    atlascreamrequirements_schema_datadict.update( {
        'cputime'         : SimpleItem(defvalue=0, typelist=['type(None)','int'], doc='Minimum available CPU time (min)'),
        } )

    _schema   = Schema(Version(1,2), atlascreamrequirements_schema_datadict)
    _category = 'LCGRequirements'
    _name = 'AtlasCREAMRequirements'
    _exportmethods = ['list_ce', 'list_se','list_sites','list_clouds', 'list_sites_cloud', 'cloud_from_sites', 'list_access_info', 'list_release_blacklist', 'getce' ]

    def __init__(self):
        super(AtlasCREAMRequirements,self).__init__()

    def merge(self,other):
        '''Merge requirements objects'''
      
        if not other: return self
      
        merged = AtlasCREAMRequirements()

        for name in self._schema.datadict.keys():
            try:
                attr = getattr(other,name)
            except AttributeError:
                attr = None
            if not attr: attr = getattr(self,name)
            setattr(merged,name,attr)
         
        return merged

    def convert(self):
        '''Convert the condition in a JDL specification'''
      
        requirements = []

        if self.sites:
            ce_requirement = ' ||\n     '.join([ 'other.GlueCEUniqueID == "%s"' % ce for ce in getCEsForSites(self.sites, self.excluded_sites, CREAM=True)])
            if not ce_requirement:
                raise BackendError('CREAM','Job cannot be submitted as no valid site has been specified.')
            requirements.append('( %s )' % ce_requirement)

        if self.os:
            os_name = self.os.lower()
            if os_name == 'slc3':
                requirements.append(slc3_req)
            elif os_name == 'slc4':
                requirements.append(slc4_req)
            else:
               raise BackendError('LCG','Job cannot be submitted as unknown OS %s has been requested.'%self.os)
            
        return requirements

    def getce(self):

        if self.cputime:
            mycputime = self.cputime
        else:
            mycputime = 0
        return getCEsForSites(self.sites, self.excluded_sites, CREAM=True, cputime=mycputime )

    def list_ce(self,ids):

        if isinstance(ids,str):
            ids = ids.split(':')

        return getCEsForSites(ids, CREAM=True)
