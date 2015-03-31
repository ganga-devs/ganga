from Ganga.Utility.Config import getConfig   
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList

import re
import os
import glob 
import tempfile  
import copy
"""
Checks if the output files of a given job(we are interested in the backend) 
should be postprocessed on the WN, depending on job.backend_output_postprocess dictionary
"""
def outputFilePostProcessingOnWN(job, outputFileClassName):
    backendClassName = job.backend.__class__.__name__

    backend_output_postprocess = job.getBackendOutputPostprocessDict()
    if backend_output_postprocess.has_key(backendClassName):
        if backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if backend_output_postprocess[backendClassName][outputFileClassName] == 'WN':
                return True
        
    return False


"""
Checks if the output files of a given job(we are interested in the backend) 
should be postprocessed on the client, depending on job.backend_output_postprocess dictionary
"""
def outputFilePostProcessingOnClient(job, outputFileClassName):
    backendClassName = job.backend.__class__.__name__

    backend_output_postprocess = job.getBackendOutputPostprocessDict()
    if backend_output_postprocess.has_key(backendClassName):
        if backend_output_postprocess[backendClassName].has_key(outputFileClassName):
            if backend_output_postprocess[backendClassName][outputFileClassName] == 'client':
                return True
        
    return False

"""
Intented for grid backends where we have to set the outputsandbox patterns for the output file types that have to be processed on the client
"""
def getOutputSandboxPatterns(job):

    outputPatterns = []

    if len(job.outputfiles) > 0:

        outputPatterns.append(getConfig('Output')['PostProcessLocationsFileName'])

        for outputFile in job.outputfiles:   

            outputFileClassName = outputFile.__class__.__name__

            if outputFilePostProcessingOnClient(job, outputFileClassName) or outputFileClassName == 'LocalFile': 
                if outputFile.namePattern not in outputPatterns:
                    if outputFile.compressed:
                        outputPatterns.append('%s.gz' % outputFile.namePattern)
                    else:       
                        outputPatterns.append(outputFile.namePattern)
                
    return outputPatterns

"""
we have to set the inputsandbox patterns for the input files that will be copied from the client, also write the commands for downloading input files from the WN
"""
def getInputFilesPatterns(job):

    tmpDir = tempfile.mkdtemp()

    inputPatterns = []
    
    # if GangaDataset is used, check if they want the inputfiles transferred
    inputfiles_list = copy.deepcopy( job.inputfiles )
    if not job.subjobs and job.inputdata and job.inputdata._name == "GangaDataset" and job.inputdata.treat_as_inputfiles:
        inputfiles_list += job.inputdata.files
        
    for inputFile in inputfiles_list:   

        inputFileClassName = inputFile.__class__.__name__

        if inputFileClassName == 'LocalFile':
            for currentFile in glob.glob(os.path.join(inputFile.localDir, inputFile.namePattern)):
                if currentFile not in inputPatterns:
                    inputPatterns.append(currentFile)

        elif outputFilePostProcessingOnClient(job, inputFileClassName): 

            #download in temp dir
            inputFile.localDir = tmpDir
            inputFile.get()

            for currentFile in glob.glob(os.path.join(inputFile.localDir, inputFile.namePattern)):
                if currentFile not in inputPatterns:
                    inputPatterns.append(currentFile)            
                
    return inputPatterns, tmpDir


"""
This should be used from only from Interactive backend
"""
def getOutputSandboxPatternsForInteractive(job):

    patternsToSandbox = [getConfig('Output')['PostProcessLocationsFileName']]
    patternsToZip = []  

    for outputFile in job.outputfiles:

        outputFileClassName = outputFile.__class__.__name__

        if outputFileClassName == 'LocalFile' or (outputFileClassName != 'LocalFile' and outputFilePostProcessingOnClient(job, outputFileClassName)):     
            if outputFile.compressed:
                patternsToSandbox.append('%s.gz' % outputFile.namePattern)
                patternsToZip.append(outputFile.namePattern)
            else:       
                patternsToSandbox.append(outputFile.namePattern)

    return (patternsToSandbox, patternsToZip)


"""
This should be used from Local and Batch backend, where there is code on the WN for 
sending the output(optionally compressed before that) to the outputsandbox
"""
def getWNCodeForOutputSandbox(job, files, jobid):
        
    patternsToSandbox = []
    patternsToZip = []  

    if len(job.outputfiles) > 0:

        files.append(getConfig('Output')['PostProcessLocationsFileName'])

        for outputFile in job.outputfiles: 
            
            outputFileClassName = outputFile.__class__.__name__
        
            if outputFileClassName == 'LocalFile' or (outputFileClassName != 'LocalFile' and outputFilePostProcessingOnClient(job, outputFileClassName)):     
                patternsToSandbox.append(outputFile.namePattern)

                if outputFile.compressed:
                    patternsToZip.append(outputFile.namePattern)               

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
    insertScript = insertScript.replace('###JOBID###', jobid)

    return insertScript 


def getWNCodeForDownloadingInputFiles(job, indent):
    """
    Generate the code to be run on the WN to download input files
    """
    if len(job.inputfiles) == 0 and (not job.inputdata or job.inputdata._name != "GangaDataset" or not job.inputdata.treat_as_inputfiles):
        return ""

    insertScript = """\n
"""
    
    # first, go over any LocalFiles in GangaDatasets to be transferred
    # The LocalFiles in inputfiles have already been dealt with
    if job.inputdata and job.inputdata._name == "GangaDataset" and job.inputdata.treat_as_inputfiles:
        for inputFile in job.inputdata.files:
            inputfileClassName = inputFile.__class__.__name__
            
            if inputfileClassName == "LocalFile":

                # special case for LocalFile
                if job.backend.__class__.__name__ in ['Localhost', 'Batch', 'LSF', 'Condor', 'PBS']:
                    # create symlink
                    insertScript += """
###INDENT#### create symbolic links for LocalFiles
###INDENT###for f in ###FILELIST###:
###INDENT###   os.symlink(f, os.path.basename(f)) 
"""
                    insertScript = insertScript.replace('###FILELIST###', "%s" % inputFile.getFilenameList())


    # if GangaDataset is used, check if they want the inputfiles transferred
    inputfiles_list = copy.deepcopy( job.inputfiles )
    if job.inputdata and job.inputdata._name == "GangaDataset" and job.inputdata.treat_as_inputfiles:
        inputfiles_list += job.inputdata.files

    for inputFile in inputfiles_list:  

        inputfileClassName = inputFile.__class__.__name__

        if outputFilePostProcessingOnWN(job, inputfileClassName):
            inputFile.processWildcardMatches()
            if inputFile.subfiles:
                for subfile in inputFile.subfiles:
                    insertScript += subfile.getWNScriptDownloadCommand(indent)
            else:
                insertScript += inputFile.getWNScriptDownloadCommand(indent)

    insertScript = insertScript.replace('###INDENT###', indent)

    return insertScript

def getWNCodeForOutputPostprocessing(job, indent):

    #dict containing the list of outputfiles that need to be processed on the WN for every file type    
    outputFilesProcessedOnWN = {}
    patternsToZip = []  

    if len(job.outputfiles) == 0:
        return ""
    else:
        for outputFile in job.outputfiles:  

            outputfileClassName = outputFile.__class__.__name__
            backendClassName = job.backend.__class__.__name__

            if outputFile.compressed:   
                if outputfileClassName == 'LocalFile' and backendClassName not in ['Localhost', 'LSF', 'Interactive']:
                    patternsToZip.append(outputFile.namePattern)  
                elif outputfileClassName != 'LocalFile' and outputFilePostProcessingOnWN(job, outputfileClassName):
                    patternsToZip.append(outputFile.namePattern)  
                elif outputfileClassName != 'LocalFile' and outputFilePostProcessingOnClient(job, outputfileClassName) and backendClassName not in ['Localhost', 'LSF', 'Interactive']:
                    patternsToZip.append(outputFile.namePattern)  
    
            if outputfileClassName not in outputFilesProcessedOnWN.keys():
                outputFilesProcessedOnWN[outputfileClassName] = []

            if outputFilePostProcessingOnWN(job, outputfileClassName):
                outputFilesProcessedOnWN[outputfileClassName].append(outputFile)

    insertScript = """\n
###INDENT###import os, glob
###INDENT###for patternToZip in ###PATTERNSTOZIP###:
###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(),patternToZip)):
###INDENT###        os.system("gzip %s" % currentFile)

###INDENT###postprocesslocations = file(os.path.join(os.getcwd(), '###POSTPROCESSLOCATIONSFILENAME###'), 'w')  
"""

    insertScript = insertScript.replace('###PATTERNSTOZIP###', str(patternsToZip))
    insertScript = insertScript.replace('###POSTPROCESSLOCATIONSFILENAME###', getConfig('Output')['PostProcessLocationsFileName'])

    for outputFileName in outputFilesProcessedOnWN.keys():          

        if len(outputFilesProcessedOnWN[outputFileName]) > 0:

            insertScript += outputFilesProcessedOnWN[outputFileName][0].getWNInjectedScript(outputFilesProcessedOnWN[outputFileName], indent, patternsToZip, 'postprocesslocations')

    insertScript += """\n
###INDENT###postprocesslocations.close()
"""
    insertScript = insertScript.replace('###INDENT###', indent)

    return insertScript

wildcardregex = re.compile('[*?\[\]]')
def iexpandWildCards(filelist):
    for f in filelist:
        if wildcardregex.search(f.namePattern):
            f.processWildcardMatches()
            for subfile in f.subfiles:
                yield subfile
        else:
            yield f

def expandWildCards(filelist):
    """
    
    """
    l = GangaList()
    l.extend(iexpandWildCards(filelist))
    return l

        
def getWNCodeForInputdataListCreation(job, indent):
    """generate the code to create ths inputdata list on the worker node"""
    insertScript = """\n
###INDENT###open("__GangaInputData.txt__", "w").write( "\\n".join( ###FILELIST### ) )
"""

    insertScript = insertScript.replace('###INDENT###', indent)

    if job.inputdata and hasattr(job.inputdata, "getFilenameList"):
        insertScript = insertScript.replace('###FILELIST###', "%s" % job.inputdata.getFilenameList() )
    else:
        insertScript = insertScript.replace('###FILELIST###', "[]")

    return insertScript
        
