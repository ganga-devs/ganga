def outputFileProcessedOnWN(job, outputFile):
    backendClassName = job.backend.__class__.__name__
    outputFileClassName = outputFile.__class__.__name__ 

    if job.backend_output_postprocess.has_key(backendClassName):
        if job.backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if job.backend_output_postprocess[backendClassName][outputFileClassName] == 'WN' or job.backend_output_postprocess[backendClassName][outputFileClassName] == 'WNclient':
                return True
        
    return False



#for now this is used from Local and Batch backend, where there is code on the WN for 
#compressing (optional) and sending the output to the outputsandbox
def getWNCodeForOutputSandbox(job, files):
        
    patternsToSandbox = []
    patternsToZip = []  

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:      
            if outputFile.__class__.__name__ == 'OutputSandboxFile' and job.backend.__class__.__name__ in ['Localhost', 'LSF']:
                patternsToSandbox.append(outputFile.name)

                if outputFile.compressed:
                    patternsToZip.append(outputFile.name)               
                

    insertScript = """\n
from Ganga.Utility.files import recursive_copy

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


def getWNCodeForOutputPostprocessing(job):

    if len(job.outputfiles) > 0:
        for outputFile in job.outputfiles:      
            if outputFileProcessedOnWN(job, outputFile):
                outputFile.getWNCode()
        
    returnString = """\n
def postprocess1(outfile, errfile):
    pass"""
    return returnString
    
        

        
