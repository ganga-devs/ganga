from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.Lib.LCG import LCGJobConfig
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.Lib.Mergers.Merger import *

# ------------------------------------------------------
# Spider RTHandler
def GetConfig(app):
    """prepare the subjob specific configuration"""
    
    #print "Preparing camont spider job"
    import time
    job = app._getParent()
    
    # set up the executable script
    exe = os.path.join(os.path.dirname(__file__),'web_spider.py')

    # Create the input list from the selected domains
    queue_lists = {}
    max_queue = 0     # current maximum number of links per domain

    for dom in app.domains:

        # Check the domain repository is there
        dir =  os.path.join( app.repository_loc, dom )
        if not os.path.exists( dir ):
            os.system("mkdir -p " + dir)
            open( os.path.join(dir, app._queuelistfilename ), "w" ).write( "http://" + dom + "/\n" )

        # load up the queue lists for each of the domains and find the maximum
        queue_file = open( os.path.join(dir, app._queuelistfilename ) )

        list = queue_file.readlines()

        if len(list) > max_queue:
            max_queue = len(list)

        queue_lists[dom] = list
        queue_file.close()

    # check for daily limit
    if max_queue > app.max_links:
        max_queue = app.max_links

    # loop and fill the input list
    q = 0
    input_list = []
    while q < max_queue:

        for dom in app.domains:

            list = queue_lists[dom]
            if q  < len( list ):
                input_list.append(queue_lists[dom][q])
            else:
                input_list.append('SLEEP\n')

        q = q + 1

    # remove these links from the queue list
    for dom in app.domains:
        
        dir =  os.path.join( app.repository_loc, dom )
        queue_file = open( os.path.join(dir, app._queuelistfilename ), "w" )
        list = queue_lists[dom]
        
        if len(list) <= max_queue:
            queue_file.close()
        else:
            for ln in list[max_queue:]:
                queue_file.write(ln)
            queue_file.close()

    # Create the input list file
    inputfile = open( app._inputlistfilename, "w")
    if len(input_list) == 0:
        raise ApplicationConfigurationError(None,'No input links found')

    for ln in input_list:
        inputfile.write(ln)

    inputfile.close()

    # create the extension list
    ext_file = open(app._extensionfilename, "w")
    for e in app.image_ext:
        ext_file.write(e + '\n')

    ext_file.write("--\n")
    for e in app.file_ext:
        ext_file.write(e + '\n')

    ext_file.close()

    # create the safe domains list
    safedoms_file = open(app._safedomsfilename, "w")
    for sd in app.safe_domains:
        safedoms_file.write(sd + '\n')

    safedoms_file.close()

    # set the environment
    environment = {'SPIDER_PAYLOAD' : os.path.basename( app.payload.name ) }

    # generate the config
    inputbox = [ File(os.path.join(os.path.dirname(__file__),'BeautifulSoup.py')),
                 File(app._inputlistfilename),
                 File(app._extensionfilename),
                 File(app._safedomsfilename),
                 File(app.payload.name )]
    inputbox += job.inputsandbox
    
    outputbox = [ 'image_info.txt', 'ext_info.txt', 'ext_list.txt', 'img_list.txt', 'links_list.txt', 'viewed_list.txt', 'dom_list.txt' ]

    outputbox.append( app.payload_output )

    return exe, inputbox, outputbox, environment

class SpiderLCGRTHandler(IRuntimeHandler):
    """Spider LCG RT Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        exe, inputsandbox, outputsandbox, env = GetConfig(app)
        lcg_config = LCGJobConfig(File(exe), inputsandbox, [], outputsandbox, env, [], None)
        
        return lcg_config

class SpiderLocalRTHandler(IRuntimeHandler):
    """Spider LCG RT Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        exe, inputsandbox, outputsandbox, env = GetConfig(app)
        local_config = StandardJobConfig(File(exe), inputsandbox, [], outputsandbox, env)
        return local_config


allHandlers.add('Spider','LCG', SpiderLCGRTHandler)
allHandlers.add('Spider','PBS', SpiderLocalRTHandler)
allHandlers.add('Spider','Local', SpiderLocalRTHandler)
