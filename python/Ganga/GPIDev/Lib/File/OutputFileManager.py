"""
Checks if the output files of a given job(we are interested in the backend) 
should be postprocessed on the WN, depending on job.backend_output_postprocess dictionary
"""
def outputFilePostProcessingOnWN(job, outputFileClassName):
    backendClassName = job.backend.__class__.__name__

    if job.backend_output_postprocess.has_key(backendClassName):
        if job.backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if job.backend_output_postprocess[backendClassName][outputFileClassName] == 'WN' or job.backend_output_postprocess[backendClassName][outputFileClassName] == 'WNclient':
                return True
        
    return False



"""
This should be used from Local and Batch backend, where there is code on the WN for 
sending the output(optionally compressed before that) to the outputsandbox
"""
def getWNCodeForOutputSandbox(job, files):
        
    patternsToSandbox = []
    patternsToZip = []  

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:  
            if outputFilePostProcessingOnWN(job, 'OutputSandboxFile'):    
                patternsToSandbox.append(outputFile.name)

                if outputFile.compressed:
                    patternsToZip.append(outputFile.name)               
                

    insertScript = """\n
from Ganga.Utility.files import recursive_copy
import glob

f_to_copy = ###FILES###

for patternToSandbox in ###PATTERNSTOSANDBOX###:
    for currentFile in glob.glob(patternToSandbox):
        f_to_copy.append(currentFile)

filesToZip = []

for patternToZip in ###PATTERNSTOZIP###:
    for currentFile in glob.glob(patternToZip):
        os.system("gzip %s" % currentFile)
        filesToZip.append(currentFile)
            
final_list_to_copy = []

for f in f_to_copy:
    if f in filesToZip:
        final_list_to_copy.append('%s.gz' % f)  
    else:       
        final_list_to_copy.append(f)            

for fn in final_list_to_copy:
    try:
        recursive_copy(fn,sharedoutputpath)
    except Exception,x:
        print 'ERROR: (job'+###JOBID###+')',x
""" 
    insertScript = insertScript.replace('###FILES###', str(files))
    insertScript = insertScript.replace('###PATTERNSTOSANDBOX###', str(patternsToSandbox))
    insertScript = insertScript.replace('###PATTERNSTOZIP###', str(patternsToZip))

    return insertScript 

def getWNCodeForOutputLCGUpload(job):

    lcgCommands = []

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:      
            if outputFilePostProcessingOnWN(job, 'LCGStorageElementFile'):
                lcgCommands.append('lcgse %s %s %s\n' % (outputFile.name , outputFile.lfc_host,  outputFile.getUploadCmd()))
                
                
        
    insertScript = """\n
postprocesslocations = file(os.path.join(os.getcwd(), '__postprocesslocations__'), 'w')         

#system command executor with subprocess
def execSyscmdSubprocessAndReturnOutput(cmd):

    exitcode = -999
    mystdout = ''
    mystderr = ''

    try:
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (mystdout, mystderr) = child.communicate()
        exitcode = child.returncode
    finally:
        pass

    return (exitcode, mystdout, mystderr)
        
def uploadToSE(lcgseItem):
        
    import re

    lcgseItems = lcgseItem.split(' ')

    filenameWildChar = lcgseItems[1]
    lfc_host = lcgseItems[2]

    cmd = lcgseItem[lcgseItem.find('lcg-cr'):]

    os.environ['LFC_HOST'] = lfc_host
        
    guidResults = []

    import glob 
    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
        cmd = lcgseItem[lcgseItem.find('lcg-cr'):]
        cmd = cmd.replace('filename', currentFile)
        cmd = cmd + ' file:%s' % currentFile
        printInfo(cmd)  
        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutput(cmd)
        if exitcode == 0:
            printInfo('result from cmd %s is %s' % (cmd,str(mystdout)))
            match = re.search('(guid:\S+)',mystdout)
            if match:
                guidResults.append(mystdout)
        else:
            printError('cmd %s failed' % cmd, mystderr)   

    return guidResults    

for lcgseItem in ###LCGCOMMANDS###:
    guids = uploadToSE(lcgseItem)
    for guid in guids:
        postprocesslocations.write('%s->%s\\n' % (lcgseItem, guid))           

  
postprocesslocations.close()

"""
    insertScript = insertScript.replace('###LCGCOMMANDS###', str(lcgCommands))

    return insertScript
    
        

        
