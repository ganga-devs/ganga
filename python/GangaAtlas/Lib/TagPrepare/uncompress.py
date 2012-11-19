import os, commands, sys, re
from AthenaCommon.AthenaCommonFlags import athenaCommonFlags

rc, out = commands.getstatusoutput('tar -zxf subcoll.tar.gz')
print out

#rc, out = commands.getstatusoutput('cp ../PoolFileCatalog.xml .')
#print out                         

print "------     ADdding to PFC"
if sys.argv[0].find('uncompress.py') != -1:
#if len(sys.argv) > 1:
    
    for ln in open('input_tag.txt').readlines():
        ln = ln.strip()
        rc, out = commands.getstatusoutput("pool_insertFileToCatalog %s" % ln)
        print out

            
rc, out = commands.getstatusoutput('more PoolFileCatalog.xml')
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

os.system('chmod +x CollInflateEventInfo.exe')

if sys.argv[0].find('uncompress.py') != -1:
#if len(sys.argv) > 1:
    # not running with Athena - do the appropriates
    for f in sys.argv[1:]:

        # inflate first
        print "inflating " + f
        cmd = "export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ; ./CollInflateEventInfo.exe " + f
        rc, out = commands.getstatusoutput(cmd)
        
        print out
        
        rc, out = commands.getstatusoutput("mv outColl.root %s" % f+".root")
        
        append_list.append(f)
        
        # now update the PFC
        for ref in tag_info[f]['refs']:
            dataset = ref[1]
            fname = ref[0]

            
else:
    
    # find which .dat file to inflate
    all_guids = []
    for ln in open('PoolFileCatalog.xml').readlines():
        if ln.find('<File ID="') == -1:
            continue

        guid = ln[ ln.find('"')+1: ln.rfind('"')]
        print "Found GUID " + guid
        all_guids.append(guid)

    for guid in all_guids:
        infile = ''

        for tf in tag_info:
            tf_guids = []
            for r in tag_info[tf]['refs']:
                tf_guids.append(r[2])

            if not guid in tf_guids:
                continue
            
            good = True
            for r in tf_guids:
                if not r in all_guids:
                    good = False

            if good:
                if tag_info[tf]['refs'][0][1].find('RAW') != -1:
                    stream_ref = "StreamRAW_ref"
                elif tag_info[tf]['refs'][0][1].find('ESD') != -1:
                    stream_ref = "StreamESD_ref"
                else:
                    stream_ref = "StreamAOD_ref"
                        
                infile = tf
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

if sys.argv[0].find('uncompress.py') != -1:
    #if len(sys.argv) > 1:
    os.rename(outname + ".newcoll.root", "outColl.root")
    sys.exit(0)
    
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
        
                  


