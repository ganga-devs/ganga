import os, commands
from AthenaCommon.AthenaCommonFlags import athenaCommonFlags

rc, out = commands.getstatusoutput('tar -zxf subcoll.tar.gz')
print out                         

for a in os.listdir('.'):
    print ":::    " + a

# load the tag info
import pickle
tag_info = pickle.load( open("taginfo.pkl"))
append_list = []
print "Loaded tag_info: "
print tag_info
stream_ref = ''

# find which .dat file to inflate
for ln in open('PoolFileCatalog.xml').readlines():
    if ln.find('<File ID="') == -1:
        continue

    guid = ln[ ln.find('"')+1: ln.rfind('"')]
    print "Found GUID " + guid
    infile = ''
    for tf in tag_info:
        for r in tag_info[tf]['refs']:
            if r[2] == guid:
                if r[1].find('AOD') != -1:
                    stream_ref = "StreamAOD_ref"
                infile = tf
                break
        if infile != '':
            break

    if infile == '':
        print "NO MATCHING FILE"
        continue

    # inflate the dat file
    print "Found file " + infile

    if infile in append_list:
        print "Already inflated. continuing..."
        continue
    
    filename = infile

    cmd = "export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ; ./CollInflateEventInfo.exe " + filename
    rc, out = commands.getstatusoutput(cmd)

    print out
    
    rc, out = commands.getstatusoutput("mv outColl.root %s" % filename+".root")
    
    append_list.append(filename)


for a in os.listdir('.'):
    print ":::    " + a

# finally, append all the TAG files together
print append_list
cmd = "CollAppend -src " 
for sub_f in append_list:
    cmd += sub_f + " RootCollection "

outname = append_list[0][ : append_list[0].find('.subcoll') ]
cmd += " -dst " + outname + ".newcoll RootCollection"
            
rc, out = commands.getstatusoutput(cmd)
print cmd
print out

print "OVERWRITING INPUT FILES"

cmd = "CollSplitByGUID.exe -splitref "+stream_ref+" -src PFN:"+outname+".newcoll.root RootCollection" 
rc, out = commands.getstatusoutput(cmd)
print cmd
if rc:
    print out

i = 0
filesInCurDir = os.listdir('.') 
for tmpName in filesInCurDir:
    match = re.search('^sub_collection_(\d+)\.root',tmpName)
    if  match != None:
        cmd = "mv %s %s" % (tmpName, athenaCommonFlags.FilesInput()[i]) 
        rc, out = commands.getstatusoutput(cmd)
        print cmd
        print out

        i += 1
        
                  



## try:
##     from EventSelectorAthenaPool.EventSelectorAthenaPoolConf import EventSelectorAthenaPool
##     orig_ESAP__getattribute2 =  EventSelectorAthenaPool.__getattribute__

##     def _dummy2(self,attr):
##         if attr == 'InputCollections':
##             return [outname + ".newcoll.root"]
##         else:
##             return orig_ESAP__getattribute2(self,attr)

##     EventSelectorAthenaPool.__getattribute__ = _dummy
##     print 'Overwrite InputCollections'
##     print EventSelectorAthenaPool.InputCollections
## except:
##     try:
##         EventSelectorAthenaPool.__getattribute__ = orig_ESAP__getattribute2
##     except:
##         pass
      
## try:
##     import AthenaCommon.AthenaCommonFlags

##     def _dummyFilesInput2(*argv):
##         return [outname + ".newcoll.root"]

##     AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput2
## except:
##     pass

## try:
##     import AthenaCommon.AthenaCommonFlags

##     def _dummyGet_Value2(*argv):
##         return [outname + ".newcoll.root"]

##     for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
##         import re
##         if re.search('^(Pool|BS).*Input$',tmpAttr) != None:
##             try:
##                 getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value2
##             except:
##                 pass
## except:
##     pass

