###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SimpleStagerBroker.py,v 1.1 2008-11-27 15:34:22 hclee Exp $
###############################################################################
# StagerBroker

from sets import Set

from Ganga.Utility.logging import getLogger

logger = getLogger()

class JobInfo:
    
    """
    Data object containing file-based brokering information.
    """
    _attributes = ('files', 'sites')
    
    def __init__(self, files, sites):
        
        """
        initialized the job info data object.
        """
        self.files = files
        self.sites = sites
        
    def addFile(self, file):
        if file not in self.files:
            self.files.append(file)
            
    def removeFile(self, file):
        if file in self.files:
            self.files.remove(file)
            
    def addSite(self, site):
        if site not in self.sites:
            self.sites.append(site)
            
    def removeSite(self, site):
        if site in self.sites:
            self.sites.remove(site)
            
    def __str__(self):
        return '%s: %s' % (repr(self.files), repr(self.sites))

class SimpleStagerBroker:
    """
    Class to generate a resource brokering plan based on file locations. 
    
    @since: 0.0.1
    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com
    """   
    _attributes = ('file_locations', 'sites_wlist', 'sites_rlist')   
    
    def __init__(self, file_locations, restricted_site_list=None, white_site_list=None):
        
        """
        initializes the broker.
        
        @since: 0.0.1
        @author: Hurng-Chun Lee
        @contact: hurngchunlee@gmail.com
        @param file_locations is a dictionary with GUIDs as keys, file locations as values
        
               e.g. file_locations = { 'guid1': [siteA, siteB, siteC],
                                       'guid2': [siteB, siteD],
                                       ... ... }
        @param restricted_site_list restricts to certain sites
        @param white_site_list is a list of good sites                               
        """
               
        self.file_locations = file_locations
        self.sites_rlist = None
        self.sites_wlist = None

        if restricted_site_list:
            self.sites_rlist = Set( restricted_site_list )

        if white_site_list != None:
            self.sites_wlist = Set( white_site_list )
    
    def doBroker(self, numjobs=1):
        
        """
        executes the brokering and prints out the brokering plan.
        
        @param numjobs sets the number of jobs to be created
        @return a list of JobInfo data object. Each one contains a list of files and the corresponding sites
        """
        
        jobInfo = []
        
        ## creates empty job info data object
        for i in range(numjobs):
            jobInfo.append(JobInfo(files=[], sites=[]))
        
        i = 0
        for f in self.file_locations.keys():
            jinfo = jobInfo[i % numjobs]
            
            jinfo.addFile(f)
            if not jinfo.sites:

                mysites = Set(self.file_locations[f]) 

                # limited to the restricted sites
                if self.sites_rlist:
                    mysites = list( mysites & self.sites_rlist )

                # limited to the sites in the white list
                if self.sites_wlist:
                    mysites = list( mysites & self.sites_wlist )

                jinfo.sites = mysites

            else:
                set1    = Set(jinfo.sites)
                set2    = Set(self.file_locations[f])
                jinfo.sites = list(set1 & set2)
                
            i+=1
                
        return jobInfo
            
                 
            
        
        
        
        
        
