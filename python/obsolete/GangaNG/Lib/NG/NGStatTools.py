###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NGStatTools.py,v 1.2 2009-06-24 09:09:53 bsamset Exp $
###############################################################################
#
# NGStatTools
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007


import sys,os, dircache
from types import IntType
import md5
from datetime import datetime

def isint(x):
    return type(x)==IntType

def getCommandOutput2(command):
    child = os.popen(command)
    data = child.read()
    err = child.close()
    #if err:
    #    raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data
                        

def getsize(infile,lfcdircontl):
    if infile.strip()=="":
        return 0
    f = infile.split()[4]
    filenl = f.split("/")
    filen = filenl[len(filenl)-1]
    for l in lfcdircontl:
        ls = l.split()
        if len(ls)<2:
            continue
        if ls[0]==filen:
            return ls[2]
    return -1
                            

def getDLtime(outputdir):

    infile = "%s/gmlog/errors" % (outputdir)
    if not os.path.exists(infile):
        return 0

    io = getCommandOutput2("grep 'ownloader' %s " % infile)

    ios = io.split("\n")

    try:
        dl_start = datetime.strptime("2009 "+ios[0][:15],"%Y %b %d %H:%M:%S")
        dl_end = datetime.strptime("2009 "+ios[1][:15],"%Y %b %d %H:%M:%S")
    except:
        return -1

    dl_time = dl_end - dl_start

    if dl_time.days>0:
        print "ERROR: DL time of more than 24h???"
        return -1

    return dl_time.seconds

def getULtime(outputdir):

    infile = "%s/gmlog/errors" % (outputdir)
    if not os.path.exists(infile):
        return 0

    io = getCommandOutput2("grep 'ploader' %s " % infile)

    ios = io.split("\n")

    try:
        ul_start = datetime.strptime("2009 "+ios[0][:15],"%Y %b %d %H:%M:%S")
        ul_end = datetime.strptime("2009 "+ios[1][:15],"%Y %b %d %H:%M:%S")
    except:
        return -1

    ul_time = ul_end - ul_start

    if ul_time.days>0:
        print "ERROR: DL time of more than 24h???"
        return -1

    return ul_time.seconds

def getinputsize(outputdir):

    infile = "%s/gmlog/errors" % (outputdir)
    if not os.path.exists(infile):
        return 0

    files = getCommandOutput2("grep 'Downloaded' %s | grep lfc" % infile)

    filesl = files.split("\n")

    if len(filesl[0].split())<4:
        return 0

    lfcdirl = filesl[0].split()[4].split("/")
    lfcdir = ""
    for i in range(len(lfcdirl)-1):
        lfcdir +=lfcdirl[i]+"/"

    # print lfcdir
    lfcdirmd5 = md5.md5(lfcdir).hexdigest()
    if os.path.exists(lfcdirmd5):
        lfcdircont = open(lfcdirmd5)
        lfcdircontl = []
        for l in lfcdircont.readlines():
            lfcdircontl.append(l.strip())
    else:
        lfcdircont = getCommandOutput2("ngls -l %s " % lfcdir)
        lfcdircontl = lfcdircont.split("\n")
        of = open(lfcdirmd5,"w")
        of.write(lfcdircont)
        of.close()

    totalsize = 0
    
    for infile in filesl:
        if infile.strip()=="":
            continue
        f = infile.split()[4]
        size = getsize(infile,lfcdircontl)
        totalsize = totalsize + int(size)

    return totalsize


def getnfiles(outputdir,protocol = 'lfc'):
    
    infile = "%s/gmlog/errors" % (outputdir)
    if not os.path.exists(infile):
        return 0
    files = getCommandOutput2("grep 'Downloaded' %s | grep %s" % (infile,protocol))
    filesl = files.split("\n")

    return len(filesl)-1

def getnfiles2(outputdir,protocol = 'lfc'):
    
    infile = "%s/gmlog/description" % (outputdir)
    if not os.path.exists(infile):
        return 0
    f = open(infile)
    l = f.readline()
    ls = l.split("%s://" % protocol)
    ninfiles = len(ls)-1
    return ninfiles


def getdiag(outputdir,diag):
    """
    diag = WallTime, KernelTime, UserTime, CPUUsage, nodename, frontend_middleware, frontend_subject
    """
    infile = "%s/gmlog/diag" % (outputdir)
    if not os.path.exists(infile):
        return None    
    f = open(infile)
    for l in f:
        if l.startswith(diag):
            ls = l.split("=")
            return ls[len(ls)-1].strip()
    f.close()
    return None

def getoutputsize(outputdir):
    infile = "%s/OutputFiles.xml" % (outputdir)
    if not os.path.exists(infile):
        return 0    
    f = open(infile)
    for l in f.readlines():
        l = l.strip()
        if l.startswith("<size>"):
            return l[6:-7]
    f.close()
    return 0

def getstatus(outputdir):
    if os.path.exists("%s/gmlog/failed" % (outputdir)):
        return "FAILED"
    infile = "%s/gmlog/status" % (outputdir)
    if not os.path.exists(infile):
        return "FAILED"    
    f = open(infile)
    status = f.readline().strip()
    f.close()
    return status

def getfailreason(outputdir):
    infile = "%s/gmlog/failed" % (outputdir)
    if not os.path.exists(infile):
        return "NONE"
    f = open(infile)
    fail = f.readline().strip()
    f.close()
    return fail
                
######################################################################################

def testNGStatTools(outputdir):

    print "%30s %30s" % ( "getstatus", getstatus(outputdir))
    print "%30s %30s" % ( "getfailreason", getfailreason(outputdir))
    print "%30s %30s" % ( "getnfiles lfc",getnfiles(outputdir))
    print "%30s %30s" % ( "getnfiles2 lfc",getnfiles2(outputdir))
    print "%30s %30s" % ( "getnfiles srm",getnfiles(outputdir,'srm'))
    print "%30s %30s" % ( "getnfiles2 srm",getnfiles2(outputdir,'srm'))
    print "%30s %30s" % ( "getDLtime",getDLtime(outputdir))
    print "%30s %30s" % ( "getULtime",getULtime(outputdir))
    #print "%30s %30s" % ( "getinputsize",getinputsize(outputdir))
    print "%30s %30s" % ( "getoutputsize",getoutputsize(outputdir))
    print "%30s %30s" % ( "getdiag('WallTime')",getdiag(outputdir,"WallTime"))
    print "%30s %30s" % ( "getdiag('KernelTime')",getdiag(outputdir,"KernelTime"))
    print "%30s %30s" % ( "getdiag('UserTime')",getdiag(outputdir,"UserTime"))
    print "%30s %30s" % ( "getdiag('CPUUsage')",getdiag(outputdir,"CPUUsage"))
    print "%30s %30s" % ( "getdiag('nodename')",getdiag(outputdir,"nodename"))
        

def gatherstats(jobnum = "all"):
    
    d = dircache.listdir(".")
    
    of = open("HCStats_%s.log" % jobnum,"w")
    
    for job in d:
        if len(job)>4:
            continue

        if jobnum!="all" and jobnum!=job:
            continue
        
        s = "%10s %10s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s %20s" % ("job","subjob","status","infiles","insize","outsize","walltime","kerneltime","usertime","cpuusage","insize/outsize","DLtime","ULtime", "nodename")
        print s
        of.write(s+"\n")
        
        sd = dircache.listdir(job)
        for subjob in sd:
            if subjob.strip()=='input':
                continue
            if subjob.strip()=='output':
                continue
            
            
            if not os.path.exists("%s/%s/output/gmlog" % (str(job),str(subjob))):
                continue

            outputdir = "%s/%s/output/" % (str(job),str(subjob))
            
            #print "%s %s" % (job, subjob)
            
            status = getstatus(outputdir)
            ninfiles = getnfiles2(outputdir)
            insize = getinputsize(outputdir)
            outsize = getoutputsize(outputdir)
            walltime = getdiag(outputdir,"WallTime")
            kerneltime = getdiag(outputdir,"KernelTime")
            usertime = getdiag(outputdir,"UserTime")
            cpuusage = getdiag(outputdir,"CPUUsage")
            node = getdiag(outputdir,"nodename")
            #ce = getdiag(outputdir,"frontend_subject")[5:-1]
            dltime = getDLtime(outputdir)
            ultime = getULtime(outputdir)
            
            ioratio = 0.0
            if float(insize)>0:
                ioratio = float(outsize)/float(insize)
                
            s = "%10s %10s %15s %15s %15s %15s %15s %15s %15s %15s        %.6f %15s %15s %20s" % (job,subjob,status,ninfiles,insize,outsize,walltime,kerneltime,usertime,cpuusage,ioratio,dltime,ultime,node)
            print s
            of.write(s+"\n")
            
    of.close()
                
def stats2ntuple(jobnum = "all"):
        
    import ROOT
    
    f = open("HCStats_%s.log" % jobnum)
    
    nt = ROOT.TNtuple("fHCNT","HC NT","job:subjob:status:ninfiles:insize:outsize:walltime:kerneltime:usertime:cpuusage:dltime:ultime")
    
    for l in f.readlines():
        ls = l.split()
        if ls[0].strip()=='job':
            continue
        
        for i in range(len(ls)):
            ls[i] = str(ls[i])
            ls[i] = ls[i].strip()
            if ls[i] == "None":
                ls[i] = "-1"
            if ls[i].endswith("s"):
                ls[i] = ls[i][:-1]
            if ls[i].endswith("%"):
                ls[i] = ls[i][:-1]
        if ls[2]=='FINISHED':
            ls[2] = "1"
        else:
            ls[2] = "0"
                            
        for i in range(13):
            ls[i] = float(ls[i])
                                
        nt.Fill(ls[0],ls[1],ls[2],ls[3],ls[4],ls[5],ls[6],ls[7],ls[8],ls[9],ls[11],ls[12]);
                                
    f.close()
                                
    of = ROOT.TFile("HCSummary_%s.root" % jobnum,"RECREATE")
    of.cd()
    nt.Write()
    of.Close()
    
                                                                                                                                                                            

# $Log: not supported by cvs2svn $
# Revision 1.1  2009/02/17 10:52:22  bsamset
# Added NGStatTools
#
