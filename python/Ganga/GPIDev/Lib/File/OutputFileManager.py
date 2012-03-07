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

def getWNCodeForOutputLCGUpload(job, indent):

    lcgCommands = []
    massStorageCommands = []

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:      
            if outputFile.__class__.__name__ == 'LCGStorageElementFile' and outputFilePostProcessingOnWN(job, 'LCGStorageElementFile'):
                lcgCommands.append('lcgse %s %s %s' % (outputFile.name , outputFile.lfc_host,  outputFile.getUploadCmd()))
            elif outputFile.__class__.__name__ == 'MassStorageFile' outputFilePostProcessingOnWN(job, 'MassStorageFile'):  
                from Ganga.Utility.Config import getConfig      
                massStorageConfig = getConfig('MassStorageOutput')  
                massStorageCommands.append('massstorage %s %s %s %s %s' % (outputFile.name , massStorageConfig['mkdir_cmd'],  massStorageConfig['cp_cmd'], massStorageConfig['ls_cmd'], massStorageConfig['path'])) 
                
        
    insertScript = """\n
###INDENT###postprocesslocations = file(os.path.join(os.getcwd(), '__postprocesslocations__'), 'w')         

###INDENT####system command executor with subprocess
###INDENT###def execSyscmdSubprocessAndReturnOutput(cmd):

###INDENT###    exitcode = -999
###INDENT###    mystdout = ''
###INDENT###    mystderr = ''

###INDENT###    try:
###INDENT###        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
###INDENT###        (mystdout, mystderr) = child.communicate()
###INDENT###        exitcode = child.returncode
###INDENT###    finally:
###INDENT###        pass

###INDENT###    return (exitcode, mystdout, mystderr)
        
###INDENT###def uploadToSE(lcgseItem):
        
###INDENT###    import re

###INDENT###    lcgseItems = lcgseItem.split(' ')

###INDENT###    filenameWildChar = lcgseItems[1]
###INDENT###    lfc_host = lcgseItems[2]

###INDENT###    cmd = lcgseItem[lcgseItem.find('lcg-cr'):]

###INDENT###    os.environ['LFC_HOST'] = lfc_host
        
###INDENT###    guidResults = []

###INDENT###    import glob 
###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
###INDENT###        cmd = lcgseItem[lcgseItem.find('lcg-cr'):]
###INDENT###        cmd = cmd.replace('filename', currentFile)
###INDENT###        cmd = cmd + ' file:%s' % currentFile
###INDENT###        printInfo(cmd)  
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutput(cmd)
###INDENT###        if exitcode == 0:
###INDENT###            printInfo('result from cmd %s is %s' % (cmd,str(mystdout)))
###INDENT###            match = re.search('(guid:\S+)',mystdout)
###INDENT###            if match:
###INDENT###                guidResults.append(mystdout)
###INDENT###        else:
###INDENT###            printError('cmd %s failed' % cmd + os.linesep + mystderr)   

###INDENT###    return guidResults    

###INDENT###for lcgseItem in ###LCGCOMMANDS###:
###INDENT###    guids = uploadToSE(lcgseItem)
###INDENT###    for guid in guids:
###INDENT###        postprocesslocations.write('%s->%s\\n' % (lcgseItem, guid))           

###INDENT###for massStorageLine in ###MASSSTORAGECOMMANDS###:
###INDENT###    massStorageList = massStorageLine.split(' ')

###INDENT###    filenameWildChar = massStorageList[1]
###INDENT###    cm_mkdir = massStorageList[2]
###INDENT###    cm_cp = massStorageList[3]
###INDENT###    cm_ls = massStorageList[4]
###INDENT###    path = massStorageList[5]

###INDENT###    pathToDirName = os.path.dirname(path)
###INDENT###    dirName = os.path.basename(path)

###INDENT###    (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutput('nsls %s' % pathToDirName)
###INDENT###    if exitcode != 0:
###INDENT###        printError('Error while executing nsls %s command, be aware that Castor commands can be executed only ###INDENT###from lxplus, also check if the folder name is correct and existing' % pathToDirName + os.linesep + mystderr)
###INDENT###        continue

###INDENT###    directoryExists = False 
###INDENT###    for directory in mystdout.split('\\n'):
###INDENT###        if directory.strip() == dirName:
###INDENT###            directoryExists = True
###INDENT###            break

###INDENT###    if not directoryExists:
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutput('%s %s' % (cm_mkdir, path))
###INDENT###        if exitcode != 0:
###INDENT###            printError('Error while executing %s %s command, check if the ganga user has rights for creating ###INDENT###directories in this folder' % (cm_mkdir, path) + os.linesep + mystderr)
###INDENT###            continue
            
###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(),filenameWildChar)):
###INDENT###        currentFileBaseName = os.path.basename(currentFile)
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutput('%s %s %s' % (cm_cp, currentFile, os.path.join(path, currentFileBaseName)))
###INDENT###        if exitcode != 0:
###INDENT###            printError('Error while executing %s %s %s command, check if the ganga user has rights for uploading ###INDENT###files to this mass storage folder' % (cm_cp, currentFile, os.path.join(path, currentFileBaseName)) + os.linesep ###INDENT### + mystderr)
###INDENT###        else:
###INDENT###            postprocesslocations.write('massstorage %s %s\\n' % (filenameWildChar, os.path.join(path, currentFile)))
###INDENT###            #remove file from output dir
###INDENT###            os.system('rm %s' % currentFile)


  
###INDENT###postprocesslocations.close()

"""
    insertScript = insertScript.replace('###LCGCOMMANDS###', str(lcgCommands))
    insertScript = insertScript.replace('###MASSSTORAGECOMMANDS###', str(massStorageCommands))
    insertScript = insertScript.replace('###INDENT###', indent)

    return insertScript
    
        

        
