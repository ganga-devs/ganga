import os
import glob
import tempfile
import copy

from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName

from Ganga.Utility.logging import getLogger
logger = getLogger()

def outputFilePostProcessingOnWN(job, outputFileClassName):
    """
    Checks if the output files of a given job(we are interested in the backend) 
    should be postprocessed on the WN, depending on job.backend_output_postprocess dictionary
    """
    return outputFilePostProcessingTestForWhen(job, outputFileClassName, 'WN')


def outputFilePostProcessingOnClient(job, outputFileClassName):
    """
    Checks if the output files of a given job(we are interested in the backend) 
    should be postprocessed on the client, depending on job.backend_output_postprocess dictionary
    """
    return outputFilePostProcessingTestForWhen(job, outputFileClassName, 'client')


def outputFilePostProxessingOnSubmit(job, outputFileClassName):
    """
    Checks if the output files of a given job(we are interested in the backend)
    should have been postprocessed on job submission, depending on job.backend_output_postprocess dictionary
    """
    return outputFilePostProcessingTestForWhen(job, outputFileClassName, 'submit')


def outputFilePostProcessingTestForWhen(job, outputFileClassName, when):
    """
    Checks if the output files of a given job(we are interested in the backend)
    should be postprocessed 'when', depending on job.backend_output_postprocess dictionary
    """
    backendClassName = getName(job.backend)

    backend_output_postprocess = stripProxy(job).getBackendOutputPostprocessDict()
    if backendClassName in backend_output_postprocess:
        if outputFileClassName in backend_output_postprocess[backendClassName]:
            if backend_output_postprocess[backendClassName][outputFileClassName] == when:
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

            outputFileClassName = getName(outputFile)

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
    inputfiles_list = copy.deepcopy(job.inputfiles)
    from Ganga.GPIDev.Lib.Dataset.GangaDataset import GangaDataset
    if not job.subjobs and job.inputdata and isType(job.inputdata, GangaDataset) and job.inputdata.treat_as_inputfiles:
        inputfiles_list += job.inputdata.files

    for inputFile in inputfiles_list:

        inputFileClassName = getName(inputFile)

        if inputFileClassName == 'LocalFile':
            for currentFile in glob.glob(os.path.join(inputFile.localDir, inputFile.namePattern)):
                if currentFile not in inputPatterns:
                    inputPatterns.append(currentFile)

        elif outputFilePostProcessingOnClient(job, inputFileClassName):

            # download in temp dir
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

        outputFileClassName = getName(outputFile)

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

            outputFileClassName = getName(outputFile)

            if outputFileClassName == 'LocalFile' or (outputFileClassName != 'LocalFile' and outputFilePostProcessingOnClient(job, outputFileClassName)):
                patternsToSandbox.append(outputFile.namePattern)

    insertScript = """\n
from files import recursive_copy
import glob
import os

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
        if not os.path.exists(sharedoutputpath):
            os.makedirs(sharedoutputpath)
        if os.path.exists(fn):
            recursive_copy(fn, sharedoutputpath)
    except Exception as x:
        print('ERROR: (job'+###JOBID###+')',x)
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

    from Ganga.GPIDev.Lib.Dataset.GangaDataset import GangaDataset
    if job.inputfiles is None or len(job.inputfiles) == 0 and\
            (not job.inputdata or ((not isType(job.inputdata, GangaDataset)) or\
                not job.inputdata.treat_as_inputfiles )):
        return ""

    insertScript = """\n
"""

    # first, go over any LocalFiles in GangaDatasets to be transferred
    # The LocalFiles in inputfiles have already been dealt with
    if job.inputdata and isType(job.inputdata, GangaDataset) and job.inputdata.treat_as_inputfiles:
        for inputFile in job.inputdata.files:
            inputfileClassName = getName(inputFile)

            if inputfileClassName == "LocalFile":

                # special case for LocalFile
                if getName(job.backend) in ['Localhost', 'Batch', 'LSF', 'Condor', 'PBS']:
                    # create symlink
                    shortScript = """
# create symbolic links for LocalFiles
for f in ###FILELIST###:
   os.symlink(f, os.path.basename(f)) 
"""
                    from Ganga.GPIDev.Lib.File import FileUtils
                    shortScript = FileUtils.indentScript(shortScript, '###INDENT####')

                    insertScript += shortScript

                    insertScript = insertScript.replace('###FILELIST###', "%s" % inputFile.getFilenameList())

    # if GangaDataset is used, check if they want the inputfiles transferred
    inputfiles_list = job.inputfiles
    if job.inputdata and isType(job.inputdata, GangaDataset) and job.inputdata.treat_as_inputfiles:
        inputfiles_list += job.inputdata.files

    for inputFile in inputfiles_list:

        inputfileClassName = getName(inputFile)

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

    # dict containing the list of outputfiles that need to be processed on the
    # WN for every file type
    outputFilesProcessedOnWN = {}
    patternsToZip = []

    if len(job.outputfiles) == 0:
        return ""
    else:
        for outputFile in job.outputfiles:

            outputfileClassName = getName(outputFile)
            backendClassName = getName(job.backend)

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

    if not patternsToZip:
        if not any(outputFilesProcessedOnWN.values()):
            return ""

    logger.debug("Process: '%s' on WN" % str(outputFilePostProcessingOnWN))

    shortScript = """\n
import os, glob
for patternToZip in ###PATTERNSTOZIP###:
    for currentFile in glob.glob(os.path.join(os.getcwd(),patternToZip)):
        if os.path.isfile(currentFile):
            os.system("gzip %s" % currentFile)

postprocesslocations = open(os.path.join(os.getcwd(), '###POSTPROCESSLOCATIONSFILENAME###'), 'a+')  
"""

    from Ganga.GPIDev.Lib.File import FileUtils
    shortScript = FileUtils.indentScript(shortScript, '###INDENT###')

    insertScript = shortScript

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

import re

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
        insertScript = insertScript.replace(
            '###FILELIST###', "%s" % job.inputdata.getFilenameList())
    else:
        insertScript = insertScript.replace('###FILELIST###', "[]")

    return insertScript

