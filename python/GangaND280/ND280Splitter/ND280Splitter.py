################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 11/01/2014
################################################################################
"""@package ND280Splitter
This module contains various splitters for ND280 jobs.
"""

import inspect

from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from Ganga.GPIDev.Schema import *

# First define the functions that can be used here or in the transforms
def splitCSVFile(csvfile, nbevents):
    subsets = []
    allLines = []
    incsvfile = csvfile
    csvfilebuf = open(incsvfile, 'rb')
    for line in csvfilebuf:
        line = line.rstrip('\r\n')
        row = line.split(",")
        if not len(row) == 3:
            print "Ignoring badly-formatted line:", ",".join(row)
            continue
        allLines.append(line)
    csvfilebuf.close()

    if nbevents < 1:
      raise Exception('Number of nbevents not set properly.')

    subsets = []
    # Less lines than number of events per job wanted => easy
    if len(allLines) < nbevents:
      subsets.append(allLines)
    else:
      nbfulljobs = len(allLines) / nbevents
      for nb in range(nbfulljobs):
        Low = nb * nbevents
        High = (nb+1) * nbevents
        subsets.append(allLines[Low:High])

      if len(allLines) % nbevents:
        # If the number of lines is not divisible by nbevents
        # then the last subjob which has less
        subsets.append(allLines[High:])

    return subsets


def splitNbInputFile(infiles, nbfiles):
    subsets = []
    # For nbfiles 0 or less, just put all the input files in one subset.
    if nbfiles < 1:
        subsets = [infiles]
    # Less files than number of jobs wanted => easy.
    elif len(infiles) < nbfiles:
      for f in infiles:
        subsets.append([f])
    else:
      nbfulljobs = len(infiles) / nbfiles
      for nb in range(nbfulljobs):
        Low = nb*nbfiles
        High = (nb+1)*nbfiles
        subsets.append(infiles[Low:High])

      if len(infiles) % nbfiles:
        # If the number of input files is not divisible by nbfiles
        # then the last subjob which has less
        subsets.append(infiles[High:])

    return subsets


class ND280SplitNbJobs(ISplitter):

    """
    Split an ND280 job into a given number of subjobs.
    The splitting is done such that all subjobs have
    the same number of input except for the last
    subjob which as the same number or less.

    For example, to split a job into N subjobs:

      S = ND280SplitNbJobs()
      S.nbjobs = 10
    or equivalent
      S = ND280SplitNbJobs(nbjobs=10)
      ... ...
      j = Job(splitter=S)
      j.submit()

    """    
    _name = "ND280SplitNbJobs"
    _schema = Schema(Version(1,0), {
        'nbjobs' : SimpleItem(defvalue=-1,doc='The number of subjobs'),
        } )

    def split(self,job):
        
        subjobs = []

        filenames = job.inputdata.get_dataset_filenames()
      
        logger.info('Creating %d subjobs ...',self.nbjobs)

        if self.nbjobs < 1:
          raise Exception('Number of nbjobs not set properly.')

        subsets = []
        # Less files than number of jobs wanted => easy
        if len(filenames) < self.nbjobs:
          for f in filenames:
            subsets.append([f])
        else:
          isPerfectSplit = (len(filenames) % self.nbjobs) == 0
          if isPerfectSplit:
            # If the number of input files is divisible by nbjobs
            # then all subjobs have the same number of input files
            nbfulljobs = self.nbjobs
          else:
            # Otherwise all subjobs have the same number of input files
            # except the last subjob which has less
            nbfulljobs = self.nbjobs - 1

          persub = len(filenames) / nbfulljobs
          for nb in range(nbfulljobs):
            Low = nb*persub
            High = (nb+1)*persub
            subsets.append(filenames[Low:High])

          if not isPerfectSplit:
            subsets.append(filenames[High:])

        for sub in subsets:

            j = addProxy(self.createSubjob(job))

            j.inputdata.set_dataset_filenames(sub)

            subjobs.append(stripProxy(j))

        return subjobs



class ND280SplitNbInputFiles(ISplitter):

    """
    Split job into a number of subjobs such that each
    have the same given number of input files or less.

    For example, to split into subjobs with N input files
    each:

      S = ND280SplitNbInputFiles()
      S.nbfiles = 10
    or equivalent
      S = ND280SplitNbInputFiles(nbfiles=10)
      ... ...
      j = Job(splitter=S)
      j.submit()

    """    
    _name = "ND280SplitNbInputFiles"
    _schema = Schema(Version(1,0), {
        'nbfiles' : SimpleItem(defvalue=-1,doc='The number of input files for each subjobs'),
        } )

    def split(self,job):
        
        subjobs = []

        filenames = job.inputdata.get_dataset_filenames()
      
        if self.nbfiles < 1:
          raise Exception('Number of nbfiles not set properly.')

        subsets = splitNbInputFile(filenames, self.nbfiles)

        logger.info('Creating %d subjobs ...',len(subjobs))

        for sub in subsets:

            j = addProxy(self.createSubjob(job))

            j.inputdata.set_dataset_filenames(sub)

            subjobs.append(stripProxy(j))

        return subjobs




class ND280SplitCSVByNbEvt(ISplitter):

    """
    Split job into a number of subjobs such that each
    subjob has a CSV file with N events.

    For example, to split into subjobs with N events
    each:

      S = ND280SplitCSVByNbEvt()
      S.nbevents = 10
    or equivalent
      S = ND280SplitCSVByNbEvt(nbevents=10)
      ... ...
      j = Job(splitter=S)
      j.submit()

    """    
    _name = "ND280SplitCSVByNbEvt"
    _schema = Schema(Version(1,0), {
        'nbevents' : SimpleItem(defvalue=-1,doc='The number of events for each subjobs'),
        } )

    def split(self,job):
        import os
        
        subjobs = []

        subsets = splitCSVFile(job.application.csvfile, self.nbevents)

        # Less files than number of jobs wanted => easy
        logger.info('Creating %d subjobs ...',len(allLines))

        # Base for the naming of each subjob's CSV file
        tmpname = os.path.basename(incsvfile)
        if len(tmpname.split('.')) > 1:
          patterncsv = '.'.join(tmpname.split('.')[0:-1])+"_sub%d."+ tmpname.split('.')[-1]
        else:
          patterncsv = tmpname+"_sub%d"

        # Base for the naming of each subjob's output file
        tmpname = os.path.basename(job.application.outputfile)
        if len(tmpname.split('.')) > 1:
          patternout = '.'.join(tmpname.split('.')[0:-1])+"_sub%d."+ tmpname.split('.')[-1]
        else:
          patternout = tmpname+"_sub%d"

        for s,sub in enumerate(subsets):
            j = addProxy(self.createSubjob(job))

            j.inputdata = job.inputdata

            subLines = '\n'.join(sub)

            from Ganga.GPIDev.Lib.File import FileBuffer
            thiscsv = patterncsv % s
            # Save in the main job's inputdir now, then the file will be moved to
            # the inputdir of each subjobs.
            job.getInputWorkspace().writefile(FileBuffer(thiscsv,subLines),executable=0)
            j.application.csvfile = os.path.join(job.inputdir,thiscsv)
            j.application.outputfile = patternout % s

            # Prepare the output filenames which must be unique

            subjobs.append(stripProxy(j))


        return subjobs



class ND280SplitOneInputFile(ISplitter):

    """
    Split job into a number of subjobs such that each
    subjob has one input file.

    For example, to split into subjobs with N input files
    each:

      S = ND280SplitOneInputFile()
      ... ...
      j = Job(splitter=S)
      j.submit()

    """    
    _name = "ND280SplitOneInputFile"
    _schema = Schema(Version(1,0), {
        } )

    def split(self,job):
        
        subjobs = []

        filenames = job.inputdata.get_dataset_filenames()
      
        subsets = []
        # Less files than number of jobs wanted => easy
        logger.info('Creating %d subjobs ...',len(filenames))
        for nb in range(len(filenames)):
            j = addProxy(self.createSubjob(job))

            j.inputdata.set_dataset_filenames([filenames[nb]])

            subjobs.append(stripProxy(j))

        return subjobs
