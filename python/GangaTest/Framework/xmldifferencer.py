#XML differencer - reads the XML files for two test versions
# and looks for differences between test case success failures
from Ganga.Utility.Config import getConfig
import os, xml, fnmatch
from Ganga.Utility.logging import getLogger
logger = getLogger()

newreportdir = None
newversion = None
newconfig = None
oldversion = None
oldconfig = None

def writeXMLDifferenceFile(newtests, oldtests, filename):
    global newreportdir, newversion, oldversion, newconfig, oldconfig
    #Initialise XMLWriter
    from pytf.testoob.compatibility.SimpleXMLWriter import XMLWriter
    #strip relevant part of filename
    ind = filename.find("__")
    filename = filename[:ind].strip()
    #write new difference file
    differencefile = os.path.join(newreportdir,filename+"__Diff_"+newversion+"-"+oldversion+"_"+newconfig+"_"+oldconfig+".xml")
    logger.info("writing:"+differencefile)
    #differencefile = "/home/alex/differencefile.xml"
    f = open(differencefile, 'w')
    w = XMLWriter(f)
    w.start('results')
    w.start('testsuites')
    #Now we have two dicts - now to match keys
    for test in newtests:
        #print test
        testcase_new = newtests[test]
        try: testcase_old = oldtests[test]
        except KeyError: 
            w.start('testcase', {'name':testcase_new.attributes.items()[0][1]} )
            result = testcase_new.getElementsByTagName("result")[0].childNodes[0].data
            w.element('result', result )
            w.start(result)
            w.data("Brand New Test")
            w.end(result)
            w.end('testcase')
            continue 
        #work out what to do
        result_new = testcase_new.getElementsByTagName("result")[0].childNodes[0].data
        result_old = testcase_old.getElementsByTagName("result")[0].childNodes[0].data
        test_name = testcase_new.attributes.items()[0][1]
        test_time = testcase_new.attributes.items()[1][1]+" / "+testcase_old.attributes.items()[1][1]
        if ( result_new == 'failure' and result_old == 'failure' ):
            test_result = 'success'
            #write XML element
            w.start('testcase', { 'name':test_name , 'time':test_time } )
            w.element('result',test_result)
            w.start('success', { 'message':'Both tests failed' , 'type': 'F->F'} )
            #failureNode = testcase_new.getElementsByTagName("failure")
            #new_data = "Latest test: \n "+failureNode[0].childNodes[0].data+"\n"
            #failureNode = testcase_old.getElementsByTagName("failure")
            #old_data = "Previous test: \n "+failureNode[0].childNodes[0].data+"\n"                         
            #w.data(new_data+" "+"\n"+old_data)
            w.end('success')
            w.end('testcase')
        elif ( result_new == 'failure' and result_old == 'success' ): 
            test_result = 'failure'
            failureNode = testcase_new.getElementsByTagName("failure")
            new_data = "Changed from success to failure: \n "+failureNode[0].childNodes[0].data+"\n"+" - "+"oops"
            w.start('testcase', { 'name':test_name , 'time':test_time } )
            w.element('result',test_result)
            w.start('failure',{ 'message':'New test failed, old one passed' , 'type': 'S->F'} )
            w.data(new_data)
            w.end('failure')
            w.end('testcase')
        elif ( result_new == 'success' and result_old == 'failure' ):
            test_result = 'failure'
            old_data = "Changed from failure to success in new release"+" - "+"correct"
            w.start('testcase', { 'name':test_name , 'time':test_time } )
            w.element('result',test_result)
            w.start('failure', { 'message':'Old test failed, new one passed' , 'type': 'F->S'} )
            w.data(old_data)
            w.end('failure')
            w.end('testcase')
        elif ( result_new == 'success' and result_old == 'success' ):
            test_result = 'success'
            w.start('testcase', { 'name':test_name , 'time':test_time } )
            w.element('result',test_result)
            w.end('testcase')
        else:
            pass
        
    #close XMLWriter methods
    #w.flush()
    w.end('testsuites')
    w.end('results')
    f.close()
    
    
#======================================================================
def comparetestfiles(newfiledict, oldfiledict):
    #print newfiledict
    #print oldfiledict
    #print filelist
    #go through list of tests
    for filename in newfiledict:
        newtests = {}
        oldtests = {}
        #go through new report
        newdoc = newfiledict[filename]
        testcases = newdoc.getElementsByTagName("testcase")
        for testcase in testcases:
           if testcase.nodeType == testcase.ELEMENT_NODE:
                #print testcase.attributes.items()
                for (name, value) in testcase.attributes.items():
                    #print name+"= "+value
                    if name == 'name':
                        #testcase_name=value.split()[0].split("/")
                        ind = value.find("[")
                        test = value[:ind].strip()
                        #print test
                #add to dictionary
                newtests[test] = testcase
        # Go through old report
        olddoc = oldfiledict[filename]
        testcases = olddoc.getElementsByTagName("testcase")
        for testcase in testcases:
           if testcase.nodeType == testcase.ELEMENT_NODE:
                #print testcase.attributes.items()
                for (name, value) in testcase.attributes.items():
                    #print name+"= "+value
                    if name == 'name':
                        #testcase_name=value.split()[0].split("/")
                        ind = value.find("[")
                        test = value[:ind].strip()
                        #print test
                #add to dictionary
                oldtests[test] = testcase
        #        
        #Call writeXMLfile and pass the two dicts
        writeXMLDifferenceFile(newtests, oldtests, filename)

#======================================================================
def decide_new(datalist):
    version_one = datalist[0]
    version_two = datalist[2]
    config_one = datalist[1]
    config_two = datalist[3]
    a = version_one.split(".")
    b = version_two.split(".")
    
    if ( a[0] == b[0] ):
        if ( a[1] == b[1] ):
            if ( a[2] == b[2] ):
                newversion = version_one
                oldversion = version_two
                newconfig = config_one
                oldconfig = config_two                        
            elif ( a[2] < b[2] ):
                newversion = version_two
                oldversion = version_one
                newconfig = config_two
                oldconfig = config_one
            elif ( a[2] > b[2] ):
                newversion = version_one
                oldversion = version_two
                newconfig = config_one
                oldconfig = config_two        
        elif ( a[1] < b[1] ):
            newversion = version_two
            oldversion = version_one
            newconfig = config_two
            oldconfig = config_one
        elif ( a[1] > b[1] ):
            newversion = version_one
            oldversion = version_two
            newconfig = config_one
            oldconfig = config_two        
    elif ( a[0] < b[0] ):
        newversion = version_two
        oldversion = version_one
        newconfig = config_two
        oldconfig = config_one
    elif ( a[0] > b[0] ):
        newversion = version_one
        oldversion = version_two
        newconfig = config_one
        oldconfig = config_two        

    return [newversion, newconfig, oldversion, oldconfig]


#======================================================================
def start(cmd_args=None):    
    #import global variables
    global newreportdir, newversion, oldversion, newconfig, oldconfig
    #
    config = getConfig('System')
    if cmd_args:
        print cmd_args
    else:
        logger.error("no args passed")
        return
    
    #process args
    if ( len(cmd_args) == 1 ):
        #is of format: number
        oldversion = cmd_args[0]
        thisversion = config['GANGA_VERSION']
        a = []
        for i in range(len(thisversion)):
            if (thisversion[i] == '-'):
                a += thisversion[i+1]
        newversion = a[0]+'.'+a[1]+'.'+a[2] 
        oldconfig = 'ALL'
        newconfig = 'ALL'
    elif ( len(cmd_args) == 2 ):
        # is of format: newversion, oldversion
        newversion = cmd_args[0]
        oldversion = cmd_args[1]        
        oldconfig = 'ALL'
        newconfig = 'ALL'
    elif ( len(cmd_args) == 3 ):
        #is of format: number, config, config
        oldversion = cmd_args[0]
        thisversion = config['GANGA_VERSION']
        a = []
        for i in range(len(thisversion)):
            if (thisversion[i] == '-'):
                a += thisversion[i+1]
        newversion = a[0]+'.'+a[1]+'.'+a[2] 
        oldconfig = cmd_args[1]
        newconfig = cmd_args[2]
    elif ( len(cmd_args) == 2 ):
        #is of format: number, number, config, config
        newversion = cmd_args[0]
        oldversion = cmd_args[1]        
        oldconfig = cmd_args[2]
        newconfig = cmd_args[3]
    
    datalist = decide_new([newversion, newconfig, oldversion, oldconfig])
    newversion = datalist[0]
    newconfig = datalist[1]
    oldversion = datalist[2]
    oldconfig = datalist[3]
    logger.info( "new version = "+newversion )
    logger.info( "oldversion = "+oldversion )
    
    #Get Newreportdir and oldreportdir
    topdir = os.sep.join(config['GANGA_PYTHONPATH'].split(os.sep)[:-2])
    for dir in os.listdir(topdir):
        if ( dir == newversion ):
            newreportdir = os.path.join(topdir,dir,'reports','latest')
        if ( dir == oldversion ):
            oldreportdir = os.path.join(topdir,dir,'reports','latest')
    try:
       logger.info("newreportdir = "+newreportdir)
    except:
       logger.warning("new version: "+str(newversion)+" not found in:"+str(newreportdir))
       return
    try:
        logger.info("oldreportdir = "+oldreportdir)
    except:
        logger.warning("old version: "+str(oldversion)+" not found in:"+str(oldreportdir))
        return

    logger.info("checking: "+newreportdir)
    logger.info("checking: "+oldreportdir)
    newfiles = {}
    oldfiles = {}
    filelist = []
    #Parse xml files
    for newfile in os.listdir(newreportdir): 
        #print newfile
        ind = newfile.find("__")
        fileconfig = newfile[ind+2:-4].strip()
        #print fileconfig
        #if fileconfig.find("Diff_"):
        #    pass
        if fileconfig == newconfig:
            reportfile = os.path.join(newreportdir,newfile)    
            #print reportfile
            try:
                newdoc = xml.dom.minidom.parse(reportfile)
            except IOError:
                logger.warning("attempted to parse directory in "+newreportdir)
            newfiles[str(newfile)] = newdoc
            filelist += [newfile]
    #go through old fir        
    for oldfile in os.listdir(oldreportdir):
        reportfile = os.path.join(oldreportdir,oldfile)
        ind = oldfile.find("__")
        fileconfig = oldfile[ind+2:-4].strip()
        #print fileconfig
        if fileconfig == oldconfig:
            try:
                olddoc = xml.dom.minidom.parse(reportfile)
            except IOError:
                logger.warning("attempted to parse directory in "+oldreportdir)     
            oldfiles[str(oldfile)] = olddoc      
    print newfiles
    print oldfiles
    #go through dictionaries and compare files and then compare tests
    comparetestfiles(newfiles, oldfiles)
    

def main(cmd_args):
    """
    cmd_args is a list contining the directories holiding the xml files to compare
    """
    start(cmd_args) 

