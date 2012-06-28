"""
Checks if the output files of a given job(we are interested in the backend) 
should be postprocessed on the WN, depending on job.backend_output_postprocess dictionary
"""
def outputFilePostProcessingOnWN(job, outputFileClassName):
    backendClassName = job.backend.__class__.__name__

    if job.backend_output_postprocess.has_key(backendClassName):
        if job.backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if job.backend_output_postprocess[backendClassName][outputFileClassName] == 'WN':
                return True
        
    return False


"""
Checks if the output files of a given job(we are interested in the backend) 
should be postprocessed on the client, depending on job.backend_output_postprocess dictionary
"""
def outputFilePostProcessingOnClient(job, outputFileClassName):
    backendClassName = job.backend.__class__.__name__

    if job.backend_output_postprocess.has_key(backendClassName):
        if job.backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if job.backend_output_postprocess[backendClassName][outputFileClassName] == 'client':
                return True
        
    return False

"""
Intented for grid backends where we have to set the outputsandbox patterns for the output file types that have to be processed on the client
"""
def getOutputSandboxPatterns(job):

    outputPatterns = ['__postprocesslocations__']       

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:   

            outputFileClassName = outputFile.__class__.__name__

            if outputFilePostProcessingOnClient(job, outputFileClassName) or outputFileClassName == 'OutputSandboxFile': 
                if outputFile.name not in outputPatterns:
                    if outputFile.compressed:
                        outputPatterns.append('%s.gz' % outputFile.name)
                    else:       
                        outputPatterns.append(outputFile.name)
                
    return outputPatterns

"""
This should be used from Local and Batch backend, where there is code on the WN for 
sending the output(optionally compressed before that) to the outputsandbox
"""
def getWNCodeForOutputSandbox(job, files):
        
    patternsToSandbox = []
    patternsToZip = []  

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles: 
            
            outputFileClassName = outputFile.__class__.__name__
        
            if outputFileClassName == 'OutputSandboxFile' or (outputFileClassName != 'OutputSandboxFile' and outputFilePostProcessingOnClient(job, outputFileClassName)):     
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

def getWNCodeForOutputPostprocessing(job, indent):

    #dict containing the list of outputfiles that need to be processed on the WN for every file type    
    outputFilesProcessedOnWN = {}

    #lcgCommands = []
    #massStorageCommands = []
    patternsToZip = []  

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:  

            outputfileClassName = outputFile.__class__.__name__
            backendClassName = job.backend.__class__.__name__


            if outputFile.compressed:   
                if outputfileClassName == 'OutputSandboxFile' and backendClassName not in ['Localhost', 'LSF']:
                    patternsToZip.append(outputFile.name)  
                elif outputfileClassName != 'OutputSandboxFile' and outputFilePostProcessingOnWN(job, outputfileClassName):
                    patternsToZip.append(outputFile.name)  
                elif outputfileClassName != 'OutputSandboxFile' and outputFilePostProcessingOnClient(job, outputfileClassName) and backendClassName not in ['Localhost', 'LSF']:
                    patternsToZip.append(outputFile.name)  
    
            if outputfileClassName not in outputFilesProcessedOnWN.keys():
                outputFilesProcessedOnWN[outputfileClassName] = []

            if outputFilePostProcessingOnWN(job, outputfileClassName):
                outputFilesProcessedOnWN[outputfileClassName].append(outputFile)

            """
            if outputfileClassName == 'LCGStorageElementFile' and outputFilePostProcessingOnWN(job, outputfileClassName):
                lcgCommands.append('lcgse %s %s %s' % (outputFile.name , outputFile.lfc_host,  outputFile.getUploadCmd()))
            elif outputfileClassName == 'MassStorageFile' and outputFilePostProcessingOnWN(job, outputfileClassName):  
                from Ganga.Utility.Config import getConfig      
                massStorageConfig = getConfig('Output')['MassStorageFile']['uploadOptions']  
                massStorageCommands.append('massstorage %s %s %s %s %s' % (outputFile.name , massStorageConfig['mkdir_cmd'],  massStorageConfig['cp_cmd'], massStorageConfig['ls_cmd'], massStorageConfig['path'])) 

            """


    insertScript = """\n
###INDENT###import glob
###INDENT###for patternToZip in ###PATTERNSTOZIP###:
###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(),patternToZip)):
###INDENT###        os.system("gzip %s" % currentFile)

###INDENT###postprocesslocations = file(os.path.join(os.getcwd(), '__postprocesslocations__'), 'w')  
"""
    insertScript = insertScript.replace('###PATTERNSTOZIP###', str(patternsToZip))
    insertScript = insertScript.replace('###INDENT###', indent)

    #ivan, make it more general (without check for the file type name)  
    for outputFileName in outputFilesProcessedOnWN.keys():          

        if len(outputFilesProcessedOnWN[outputFileName]) > 0:

            if outputFileName == 'LCGStorageElementFile':
                from LCGStorageElementFile import LCGStorageElementFile
                insertScript += LCGStorageElementFile.getWNInjectedScript(outputFilesProcessedOnWN[outputFileName], indent, patternsToZip, 'postprocesslocations')
            elif outputFileName == 'MassStorageFile':
                from MassStorageFile import MassStorageFile 
                insertScript += MassStorageFile.getWNInjectedScript(outputFilesProcessedOnWN[outputFileName], indent, patternsToZip, 'postprocesslocations')        
            else:
                pass

    return insertScript
    
        

        
