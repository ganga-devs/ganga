from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.Lib.Mergers.Merger import *

import urlparse

# ------------------------------------------------------
# Main Spider application class
class Spider(IApplication):
    """The main Spider executable"""

    _schema = Schema(Version(2,0), {
        'domains'       : SimpleItem(defvalue=[''],doc='The domains to spider'),
        'safe_domains'  : SimpleItem(defvalue=[''],doc='Safe domains to spider'),
        'image_ext'     : SimpleItem(defvalue=[''],doc='Image Extensions to record'),
        'file_ext'      : SimpleItem(defvalue=[''],doc='File Extensions to record'),
        'repository_loc': SimpleItem(defvalue='', doc = 'The location of the spider repository'),
        'max_links'     : SimpleItem(defvalue=1000, doc = 'Maximum links per domain'),
        'payload'       : FileItem(doc = 'Payload file to use'),
        'payload_output': SimpleItem( defvalue = '', doc='payload output')
        })
    _category = 'applications'
    _name = 'Spider'
    _exportmethods = []
    _queuelistfilename = 'queue_list.txt'
    _inputlistfilename = 'input_list.txt'
    _extensionfilename = 'ext.txt'
    _safedomsfilename = 'safe_doms.txt'
    
    def configure(self,masterappconfig):
        logger.debug('Spider configure called')
        return (None,None)
    
    def master_configure(self):
        logger.debug('Spider master_configure called')
        
        # check domains have been asked for
        if len(self.domains) == 0:
            raise ApplicationConfigurationError(None,'No domains specified')

        # check the payload
        if self.payload.name != "":
            payload = __import__(os.path.basename(self.payload.name.strip('.py')))
            payload.__getattribute__('init')
            payload.__getattribute__('new_html')
            payload.__getattribute__('end')

        return (0, None)

    def postprocess(self):
        """Sort out the results of the spider"""
        from Ganga.GPIDev.Lib.Job import Job
        j = self.getJobObject()

        # extract all the spidered links and images, etc from the job
        viewed_file = open( os.path.join(j.outputdir, 'viewed_list.txt'), 'r')
        viewed_list = viewed_file.readlines()
        viewed_file.close()
        self.ExtractViewedLinks( viewed_list )
            
        queued_file = open( os.path.join(j.outputdir, 'links_list.txt'), 'r')
        queued_list = queued_file.readlines()
        queued_file.close()
        self.ExtractQueuedLinks( queued_list )
            
        dom_list_file = open( os.path.join(j.outputdir, 'dom_list.txt'), 'r')
        dom_list = dom_list_file.readlines()
        dom_list_file.close()
        if len(dom_list) != 0:
            self.ExtractNewDomains( dom_list )
                        
        image_list_file = open( os.path.join(j.outputdir, 'img_list.txt'), 'r')
        image_info_file = open( os.path.join(j.outputdir, 'image_info.txt'), 'r')
        self.ExtractInfo(image_list_file, image_info_file, 'ImageInfo.txt')
        image_list_file.close()
        image_info_file.close()

        ext_list_file = open( os.path.join(j.outputdir, 'ext_list.txt'), 'r')
        ext_info_file = open( os.path.join(j.outputdir, 'ext_info.txt'), 'r')
        self.ExtractInfo(ext_list_file, ext_info_file, 'ExtInfo.txt')
        ext_list_file.close()
        ext_info_file.close()

    def ExtractViewedLinks(self, viewed_list ):

        # first, get together all similar domains
        viewed_arr = {}
        for ln in viewed_list:
            
            url = urlparse.urlparse( ln )
            if not url[1] in viewed_arr.keys():
                viewed_arr[url[1]] = []
            viewed_arr[url[1]].append( ln )

        # now add these to the lists in the directories
        for list_key in viewed_arr:

            dom = os.path.join( self.repository_loc, urlparse.urlparse( viewed_arr[list_key][0].strip() )[1] )
        
            if not os.path.exists( dom ):
                print "Error in ExtractViewedResults: Could not find domain " + dom
                continue
        
            viewed_file = open( os.path.join(dom, 'viewed_list.txt'), 'a')
        
            for ln in  viewed_arr[list_key]:
                viewed_file.write(ln)

            viewed_file.close()

        return

    def ExtractQueuedLinks( self, queued_list ):

        import sys
        
        # first, get together all similar domains
        queued_arr = { }
        temp_list = { }
    
        for ln in queued_list:
        
            url = urlparse.urlparse( ln )
            if not url[1] in temp_list.keys():
                temp_list[url[1]] = [ ]
            temp_list[url[1]].append( ln.strip('\n') )

        # create the sets
        for list_key in temp_list:
            queued_arr[list_key] = set( temp_list[list_key] )
            
        # now add these to the lists in the directories
        for list_key in temp_list:

            dom = os.path.join( self.repository_loc, urlparse.urlparse( temp_list[list_key][0].strip() )[1])
            if not os.path.exists( dom ):
                print "Error in ExtractQueuedResults: Could not find domain " + dom + " (" + temp_list[list_key][0].strip() + ")"
                continue

            queued_file = open( os.path.join(dom, self._queuelistfilename), 'r')
            temp_list2 = [ ]
            for ln in queued_file.readlines():
                temp_list2.append( ln.strip('\n') )
            queued_file.close()
            old_queue_list = set( temp_list2 )
         
            viewed_file = open( os.path.join(dom, 'viewed_list.txt'), 'r')

            temp_list2 = []
            for ln in viewed_file.readlines():
                temp_list2.append( ln.strip('\n') )
                
            viewed_list = set( temp_list2 )
            
            # find the links that aren't in the viewed list and is the union of the queued list
            new_queued_list = (queued_arr[list_key] - viewed_list) | old_queue_list

            queued_file = open( os.path.join(dom, self._queuelistfilename), 'w')
            for ln in new_queued_list:
                queued_file.write(ln + '\n')
            queued_file.close()

        return

    def ExtractNewDomains( self, dom_list ):

        # extract any new domains and add them to the domain list
        import re
        nonalpha = re.compile('[^a-zA-Z.]')
        for dom in dom_list:
            if not nonalpha.search(dom):
                self.domains.append(dom.strip('\n'))
    
        return

    def ExtractInfo( self, image_list_file, image_info_file, output_file ):
    
        # go through image list
        info_list = image_info_file.readlines()
        num = 0
    
        for ln in image_list_file.readlines():
        
            url = urlparse.urlparse( ln )
            dir = os.path.join( self.repository_loc, url[1] )
            if os.path.exists( dir ):
                try: 
                    image_file = open( os.path.join(dir, output_file), 'r')
                    image_list = image_file.readlines()
                    image_file.close()
                except:
                    image_file = open( os.path.join(dir, output_file), 'w')
                    image_list = []
                    image_file.close()
                
                if not ln in image_list:
                    #open( os.path.join(dir, output_file), 'a').write( ln )

                    # now find the entry in the image_info.txt file
                    i = 0
                    for im in info_list:
                        if im.find(ln.strip()) != -1:
                            out_info = open( os.path.join(dir, output_file), "a")
                            out_info.write( info_list[i-1] )
                            out_info.write( info_list[i] )
                            out_info.write( info_list[i+1] )
                            out_info.write( info_list[i+2] )
                            out_info.write( info_list[i+3] )
                            out_info.write( info_list[i+4] )
                            out_info.close()
                            
                            break
                    
                        i=i+1
