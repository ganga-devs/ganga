###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMC.py,v 1.23 2009-04-30 08:42:59 ebke Exp $
###############################################################################
# AthenaMC Job Handler
#


import os, re, string, commands
import random

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Core import FileWorkspace
from Ganga.Core.exceptions import ApplicationConfigurationError

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import matchFile, expandList



class AthenaMC(IApplication):
    """The main Athena MC Job Handler for JobTransformations"""

    _schema = Schema(Version(2,0), {
        'random_seed'        : SimpleItem(defvalue='1',doc='Random Seed for MC Generator',typelist=["str"]),
        'evgen_job_option'         : SimpleItem(defvalue='',doc='JobOption filename, or path is modified locally',typelist=["str"]),
        'production_name'    : SimpleItem(defvalue='',doc='Name of the MC production',typelist=["str"]),
        'process_name'       : SimpleItem(defvalue='',doc='Name of the generated physics process. Now replaced by production_name',typelist=["str"]),
        'run_number'         : SimpleItem(defvalue='',doc='Run number. Now replaced by production_name',typelist=["str"]),
        'number_events_job'  : SimpleItem(defvalue=1,doc='Number of events per job',typelist=["int"]),
        'atlas_release'      : SimpleItem(defvalue='',doc='ATLAS Software Release',typelist=["str"]),
        'transform_archive'  : SimpleItem(defvalue='',doc='Name or Web location of a modified ATLAS transform archive.',typelist=["str"]),
        'se_name'            : SimpleItem(defvalue='none',doc='Name of prefered SE or DQ2 site (from TierOfAtlas.py) for output',typelist=["str"]),
        'mode'               : SimpleItem(defvalue='',doc='Step in the generation chain (evgen, simul (is simul+digit), recon, template). template is to use any transformation not coverd by any of the three previous steps.'),
        'transform_script'     : SimpleItem(defvalue='',doc='File name of the transformation script to use',typelist=["str"]),
        
#        'input_firstfile'           : SimpleItem(defvalue=1,doc='simul,recon: lowest partition number to be processed from input dataset'),
#        'number_inputfiles'  : SimpleItem(defvalue=1,sequence=0,doc='Number of inputfiles to process. With input_firstfile, defines a subset of inputfiles to be processed (subset of size number_inputfiles starting with partition number= input_firstfile)'),
#        'output_firstfile'   : SimpleItem(defvalue=1,doc='offset for output file partition numbers. First job will generate the partition number output_firstfile, second will generate output_firstfile+1, and so on...'),
        'firstevent'         : SimpleItem(defvalue=1,doc='evgen: sets first event number to be generated (in first job. The first event number in second job will be firstevent+number_events_job and so on...). simul, recon: decides how many events to be skipped in input files (= skip +1). This is propagated to all subjobs.',typelist=["int"]),
        'extraArgs'          : SimpleItem(defvalue='',doc='Extra arguments for the transformation, fixed value (experts only)',typelist=["str"]),
        'extraIncArgs'       : SimpleItem(defvalue='',doc='Extra integer arguments for the transformation, with value increasing with the subjob number. Please set like this: extraIncArgs="arg1=val1_0 arg2=val2_0" with valX_0 the value taken by the argument at the first subjob. On the second subjob, the arguments will have the value valX_0 + 1 and so on...  (experts only)',typelist=["str"]),
        'geometryTag'        : SimpleItem(defvalue='ATLAS-DC3-05',doc='Geometry tag for simulation and reconstruction',typelist=["str"]),        
        'partition_number'  : SimpleItem(defvalue=None,doc='output partition number',typelist=['type(None)',"int"]),
        'triggerConfig' : SimpleItem(defvalue='NONE',doc='recon, 12.0.5 and beyond: trigger configuration',typelist=["str"]),
        'version' : SimpleItem(defvalue='',doc='version tag to insert in the output dataset and file names',typelist=["str"]),
        'verbosity' : SimpleItem(defvalue='ERROR',doc='Verbosity of transformation for log files',typelist=["str"]),
        'siteroot' : SimpleItem(defvalue='',doc='location of experiment software area for non-grid backends.',typelist=["str"]),
        'cmtsite' : SimpleItem(defvalue='',doc='flag to use kit or cern AFS installation. Set to CERN for the latter, leave unset otherwise.',typelist=["str"]),
        'dryrun' : SimpleItem(defvalue=False,doc='flag to not do stagein/stageout, for testing.',typelist=["bool"]),
        })
    
    _category = 'applications'
    _name = 'AthenaMC'
    _exportmethods = ['prepare', 'postprocess']
    _GUIPrefs= [ { 'attribute' : 'mode', 'widget' : 'String_Choice', 'choices' : ['evgen','simul','recon','template']}, { 'attribute' : 'verbosity', 'widget' : 'String_Choice', 'choices' : ['ALL','VERBOSE','DEBUG','INFO','WARNING','ERROR','FATAL']}]

    dbrelease=""

    def postprocess(self):
       """Determine outputdata and outputsandbox locations of finished jobs
       and fill output variable"""
       from Ganga.GPIDev.Lib.Job import Job
       job = self._getParent()
       if job.outputdata:
          job.outputdata.fill()
              
    def prepare(self):
       """Prepare each job/subjob from the user area"""
       # will call backend related method in RTHandler.py
       # can call configure(), then master_configure()
       
    def getPartitionList(self):
        """ Calculates the list of partitions that should be processed by this application. 
            If no splitter is present, the list has always length one.
            Returns the tuple (list of partitions, boolean 'open'), where 'open' is True if the last 
            entry in the list is the beginning of an open range. """
        try:
           job = self.getJobObject()
        except AssertionError:
           job = None # Unassociated application - returns open range

        # Treat evgen, partition only specified by partition_number
        if self.mode == "evgen":
           if self.partition_number == None:
              firstpartition = 1
           else:
              firstpartition = self.partition_number
        else:
        # If we are in simul/recon mode the partition can be specified using firstevent XOR partition_number
           if self.partition_number == None:
              firstpartition = 1 + (self.firstevent-1)/self.number_events_job
           elif self.firstevent == 1:
              firstpartition = self.partition_number
           else:
              raise ApplicationConfigurationError(None,"Except for evgen jobs, app.firstevent is an alternative to app.partition_number. You can not specify both at the same time!")

        if job and not job.splitter:
           return ([firstpartition], False)
        elif not job:
           return ([firstpartition], True)
        else:
           # First, use the splitter variables
           if (not job.splitter.output_partitions) and (not job.splitter.input_partitions):
              ## process numsubjobs or all partitions
              if job.splitter.numsubjobs:
                 return (range(firstpartition, firstpartition+job.splitter.numsubjobs), False)
              else:
                 return ([firstpartition], True) # open range starting at firstpartition
           elif (not job.splitter.output_partitions) ^ (not job.splitter.input_partitions): # ^ is XOR
              if job.splitter.output_partitions:
                 return expandList(job.splitter.output_partitions)
              else:
                 inputs = expandList(job.splitter.input_partitions)
                 return (self.getPartitionsForInputs(inputs[0], job.inputdata), inputs[1]) # propagate open range
           else:
              raise ApplicationConfigurationError(None,"Either splitter.output_partitions or splitter.input_partitions can be specified, but not both!")

    def getInputPartitionInfo(self, ids):
        """ Returns the tuple (jobs_per_input, inputs_per_job, skip_files, skip_jobs). The variables are:
            jobs_per_input: Number of jobs it takes to process a full input file.
            inputs_per_job: Number of input files a job processes
            skip_files: How many input files are skipped
            skip_jobs: How many jobs are skipped in numbering, i.e. in simulation if the first 1000 events 
                 of one large input file are skipped, partition number one would start at event number 1001."""
        if not ids:
            raise ApplicationConfigurationError(None,"No input dataset specified!")
        # This strange division rounds correctly, for example -((-10)/10) == 1,
        jobs_per_input = -((-ids.number_events_file)/self.number_events_job)
        inputs_per_job = -((-self.number_events_job)/ids.number_events_file)
        skip_files = ids.skip_files + ids.skip_events/ids.number_events_file
        skip_jobs = (ids.skip_events % ids.number_events_file) / self.number_events_job
        return (jobs_per_input, inputs_per_job, skip_files, skip_jobs)
  
    def getPartitionsForInputs(self,inputs,ids):
        """ Returns a list of partition numbers that process the given input files"""
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        partitions = []
        # loop over the actual input partition numbers
        for inputpart in [input - 1 - skip_files for input in inputs]:
            firstjob = jobs_per_input*inputpart/inputs_per_job + 1 - skip_jobs
            partitions.extend([i for i in range(firstjob, firstjob + jobs_per_input) if i > 0])
        partitions = dict([(i,1)for i in partitions]).keys() # make unique
        partitions.sort()
        return partitions

    def getInputsForPartitions(self,partitions,ids):
        """ Returns a list of input files that are needed by the given jobs (given as partition numbers)"""
        if not ids:
            return []
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        inputs = []
        # loop over the partition numbers and collect inputs
        for p in partitions:
            firstinput = inputs_per_job * (p - 1 + skip_jobs)/jobs_per_input + skip_files + 1
            inputs.extend(range(firstinput, firstinput+inputs_per_job))
        inputs = dict([(i,1)for i in inputs]).keys() # make unique
        inputs.sort()
        return inputs

    def getFirstEvent(self, partition,ids):
        """ For a given partition, return the first event in the first input file that has to be processed. 
            Returns a tuple (firstevent, numevents) where numevents is the adjusted number of events to be processed.
            ids is an input dataset that is used"""
        if not ids:
            return (self.firstevent + (partition - 1) * self.number_events_job, self.number_events_job)
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        if partition == 1:
            skip = (ids.skip_events % ids.number_events_file) % self.number_events_job
        else:
            skip = 0
        return (1 + ((partition - 1 + skip_jobs) % jobs_per_input) * self.number_events_job + skip, self.number_events_job - skip)



    def getEvgenArgs(self):
        """prepare args vector for evgen mode"""
        args=[]
        if not self.transform_script:
            self.transform_script="csc_evgen_trf.py"

        args =  [ self.atlas_rel,
                  self.se_name,
                  self.outputfiles["LOG"],
                  self.transform_script,
                  "runNumber=%s" % str(self.runNumber),
                  "firstEvent=%s" % str(self.firstevt),
                  "maxEvents=%s" % str(self.Nevents_job),
                  "randomSeed=%s" % str(self.random_seed),
                  "jobConfig=%s" % self.evgen_job_option_filename,
                  "outputEvgenFile=%s" % self.outputfiles["EVNT"]
                  ]

        if "HIST" in self.outputfiles:
            args.append("histogramFile=%s" % self.outputfiles["HIST"]) # validation histos on request only for csc_evgen_trf.py
        if "NTUP" in self.outputfiles:
            args.append("ntupleFile=%s" % self.outputfiles["NTUP"])
        if self.infileString:
            args.append("inputGeneratorFile=%s" % self.infileString)


        return args
    
    def getSimulArgs(self):
        """prepare args vector for simul-digit mode"""
        args=[]
        skip=str(self.firstevt-1)
        
        if not self.transform_script:
            self.transform_script="csc_simul_trf.py"
            
        args = [ self.atlas_rel,
                 self.se_name,
                 self.outputfiles["LOG"],
                 self.transform_script,
                 "inputEvgenFile=%s" % self.infileString, # already quoted by construction
                 "outputHitsFile=%s" % self.outputfiles["HITS"],
                 "outputRDOFile=%s" % self.outputfiles["RDO"],
                 "maxEvents=%s" % str(self.Nevents_job),
                 "skipEvents=%s" % str(skip),
                 "randomSeed=%s" % str(self.random_seed),
                 "geometryVersion=%s" % self.geometryTag
                 ]
        if self.atlas_rel >="12.0.5" :
            args.append("triggerConfig=%s" % self.triggerConfig)
        if self.atlas_rel >="13" and self.extraArgs.find("digiSeedOffset")<0:
            random.seed(int(self.random_seed))
            self.extraArgs += ' digiSeedOffset1=%s digiSeedOffset2=%s ' % (random.randint(1,2**15),random.randint(1,2**15))
        
        return args

        
    def getReconArgs(self):
        """prepare args vector for recon mode"""
        args=[]
        skip=str(self.firstevt-1)
            
        if not self.transform_script:
            self.transform_script="csc_reco_trf.py"

        args = [ self.atlas_rel,
                 self.se_name,
                 self.outputfiles["LOG"],
                 self.transform_script,
                 "inputRDOFile=%s" % self.infileString,
                 "maxEvents=%s" % str(self.Nevents_job),
                 "skipEvents=%s" % str(skip),
                 "geometryVersion=%s" % self.geometryTag
                 ]
        if "ESD" in self.outputfiles and self.outputfiles["ESD"].upper() != "NONE":
            args.append("outputESDFile=%s" % self.outputfiles["ESD"])
        if "AOD" in self.outputfiles and self.outputfiles["AOD"].upper() != "NONE":
            args.append("outputAODFile=%s" % self.outputfiles["AOD"])

        if self.atlas_rel >="12.0.5" :
            if "NTUP" in self.outputfiles and self.outputfiles["NTUP"].upper() != "NONE":
                args.append("ntupleFile=%s" %  self.outputfiles["NTUP"])
            args.append("triggerConfig=%s" % self.triggerConfig)

        return args

    def getTemplateArgs(self):
        """prepare args vector for template mode"""
        try:
            assert self.transform_script
        except AssertionError:
            raise ApplicationConfigurationError(None,"template mode requires the name of the transformation you want to use")
        
        logger.debug("Using the new template mode. Please use application.extraArgs for the transformation parameters")

        try:
            assert "LOG" in self.outputfiles
        except AssertionError:
            raise ApplicationConfigurationError(None,"template mode requires a logfile, set by job.application.outputdata.logfile")

        args =  [ self.atlas_rel,
                 self.se_name,
                 self.outputfiles["LOG"],
                 self.transform_script
                 ]
        if "EVNT" in self.outputfiles and self.outputfiles["EVNT"].upper() != "NONE":
            args.append("outputEvgenFile=%s" % self.outputfiles["EVNT"]) 
        if "HIST" in self.outputfiles and self.outputfiles["HIST"].upper() != "NONE":
            args.append("histogramFile=%s" % self.outputfiles["HIST"]) 
        if "HITS" in self.outputfiles and self.outputfiles["HITS"].upper() != "NONE":
            args.append("outputHitsFile=%s" % self.outputfiles["HITS"]) 
        if "RDO" in self.outputfiles and self.outputfiles["RDO"].upper() != "NONE":
            args.append("outputRDOFile=%s" % self.outputfiles["RDO"]) 
        if "ESD" in self.outputfiles and self.outputfiles["ESD"].upper() != "NONE":
            args.append("outputESDFile=%s" % self.outputfiles["ESD"])
        if "AOD" in self.outputfiles and self.outputfiles["AOD"].upper() != "NONE":
            args.append("outputAODFile=%s" % self.outputfiles["AOD"])
        if "NTUP" in self.outputfiles and self.outputfiles["NTUP"].upper() != "NONE":
            args.append("ntupleFile=%s" % self.outputfiles["NTUP"])
        return args
 
    def configure(self,masterappconfig):
        
        # getting configuration for individual subjobs
        self.inputfiles=self.turls.keys()
        partition = self.getPartitionList()[0][0] # This function either throws an exception or returns at least one element
        job = self._getParent() # Returns job or subjob object
        (self.firstevt, self.Nevents_job) = self.getFirstEvent(partition, job.inputdata)
        logger.debug("partition %i, first event is %i, processing %i events" % (partition,self.firstevt, self.Nevents_job))
        
        inputnumbers = self.getInputsForPartitions([partition], job._getRoot().inputdata) # getInputsForPartitions get the subset of inputfiles needed by partition i. So far so good. 
        if inputnumbers:
            matchrange = (job._getRoot().inputdata.numbersToMatcharray(inputnumbers), False)
        else:
            matchrange = ([],False)
        logger.debug("partition %i using input partitions: %s as files: %s" % (partition, inputnumbers, matchrange[0]))

        self.inputfiles = [fn for fn in self.turls.keys() if matchFile(matchrange, fn)]
        self.inputfiles.sort()
        
        # Strict matching must be discarded if inputdata.redefine_partitions is not used.

        if (job._getRoot().inputdata and job._getRoot().inputdata.redefine_partitions == ""):
            self.inputfiles=[]
            inlfns=self.turls.keys()
            inlfns.sort()
            for i in inputnumbers:
                try:
                    assert len(inlfns)>= i
                except:
                    raise ApplicationConfigurationError(None,"Not enough input files, got %i expected %i" % (len(inlfns),inputnumbers))

                self.inputfiles.append(inlfns[i-1])

        if not self.dryrun and len(self.inputfiles) < len(inputnumbers):
            if len(self.inputfiles) > 0:
               missing = []
               for fn in matchrange[0]:
                   found = False
                   for infile in self.inputfiles:
                       if fn in infile: 
                           found = True
                           break
                   if not found:
                       missing.append(fn)
               logger.warning("Not all input files for partition %i found! Missing files: %s" % (partition, missing))
            else:
               raise ApplicationConfigurationError(None,"No input files for partition %i found ! Files expected: %s" % (partition, matchrange[0]))
           
        for infile in self.inputfiles:
            self.dsetmap[infile]=self.lfcs.keys()[0]
            self.sitemap[infile]=string.join(self.sites," ") # only for signal input datasets
        self.infileString=",".join(self.inputfiles)
        # adding cavern/minbias/dbrelease to the mapping
        self.cavernfiles=self.cavern_turls.keys()
        for infile in  self.cavernfiles:
            self.dsetmap[infile]=self.cavern_lfcs.keys()[0]
            #            sitemap[infile]=string.join(self.cavern_sites," ")
            self.sitemap[infile]=self.cavern_sites[0]
        self.mbfiles=self.minbias_turls.keys()
        for infile in self.mbfiles:
            self.dsetmap[infile]=self.minbias_lfcs.keys()[0]
            #           sitemap[infile]=string.join(self.minbias_sites," ")
            self.sitemap[infile]=self.minbias_sites[0]
        self.dbfiles=self.dbturls.keys()

        for infile in self.dbfiles:
            self.dsetmap[infile]=self.dblfcs.keys()[0]
            #            sitemap[infile]=string.join(self.dbsites," ")
            self.sitemap[infile]=self.dbsites[0]
        random.shuffle(self.cavernfiles)
        if job.inputdata and len(self.cavernfiles) >0 and job.inputdata.n_cavern_files_job:
            imax=job.inputdata.n_cavern_files_job
            try:
                assert len(self.cavernfiles)>= imax
            except:
                raise ApplicationConfigurationError(None,"Not enough cavern input files to sustend a single job (expected %d got %d). Aborting" %(imax,len(self.cavernfiles)))
            self.cavernfiles=self.cavernfiles[:imax]

        random.shuffle(self.mbfiles)
        if job.inputdata and len(self.mbfiles) >0 and job.inputdata.n_minbias_files_job:
            imax=job.inputdata.n_minbias_files_job
            try:
                assert len(self.mbfiles)>= imax
            except:
                raise ApplicationConfigurationError(None,"Not enough minbias input files to sustend a single job (expected %d got %d). Aborting" %(imax,len(self.mbfiles)))
            self.mbfiles=self.mbfiles[:imax]

# now doing output files....
        outpartition = partition + job._getRoot().outputdata.output_firstfile - 1
        for filetype in self.fileprefixes.keys():
            if filetype=="LOG":
                self.outputfiles["LOG"]=self.fileprefixes["LOG"]+"._%5.5d.job.log" % outpartition 
            elif  filetype=="HIST":
                self.outputfiles["HIST"]=self.fileprefixes["HIST"]+"._%5.5d.hist.root" % outpartition
            elif  filetype=="NTUP":
                self.outputfiles["NTUP"]=self.fileprefixes["NTUP"]+"._%5.5d.root" % outpartition
            else:
                self.outputfiles[filetype]=self.fileprefixes[filetype]+"._%5.5d.pool.root" % outpartition
            # add the final lfn to the expected output list
            if self.outputfiles[filetype].upper() != "NONE":
                logger.debug("adding %s to list of expected output" % self.outputfiles[filetype])
                job.outputdata.expected_output.append(self.outputfiles[filetype])

        # map to subjobs.
        self.subjobsOutfiles[job.id]={}
        for type in self.outputfiles.keys():
            self.subjobsOutfiles[job.id][type]=self.outputfiles[type]

        for type in self.outputfiles.keys():
            if self.outputpaths[type][-1]!="/":
                self.outputpaths[type]=self.outputpaths[type]+"/"
        expected_datasets=""
        for filetype in self.outputpaths.keys():
            dataset=string.replace(self.outputpaths[filetype],"/",".")
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
        if not job.outputdata.output_dataset or string.find(job.outputdata.output_dataset,",") > 0 :
            # if not job.outputdata.output_dataset:
            job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.
        # Fill arg list and output data vars depending on the prod mode
        if not self.se_name:
            self.se_name='none'
        if self.mode=='evgen':
            self.args=self.getEvgenArgs()
        elif self.mode=='simul':
            self.args=self.getSimulArgs()
        elif self.mode=='recon':
            self.args=self.getReconArgs()
        elif self.mode=='template':
            self.args=self.getTemplateArgs()

        if self.extraArgs:    
            #            args.append(self.extraArgs)
            #        need to scan for $entries...
            arglist=string.split(self.extraArgs)
            NewArgstring=""
            for arg in arglist:
                key,val=string.split(arg,"=")
                if key=="DBRelease" and self.dbrelease:
                    continue # this key must be deleted as a new value must be formed (see next block)
                imin=string.find(val,"$")
                imin2=string.find(val,"$out")
                newval=""
                if imin>-1:
                    if string.find(val[imin+1:],"J")>-1:
                        nval=val.replace("$J",str(partition))
                        try:
                            newval=eval(nval)
                            assert newval
                        except AssertionError:
                            raise ApplicationConfigurationError(None,"error while parsing arguments: %s %d %d" % (val, imin, imin2))
                    
                    if string.find(val[imin+1:],"inputfile")>-1:
                        newval=self.infileString
                    if string.find(val[imin+1:],"cavern")>-1:
                        newval=self.cavernfile
                    if string.find(val[imin+1:],"minbias")>-1:
                        newval=self.minbiasfile
                    if string.find(val[imin+1:],"first")>-1:
                        newval=str(self.firstevt)
                    if string.find(val[imin+1:],"skip")>-1:
                        skip=str(self.firstevt-1)
                        newval=str(skip)
                    if string.find(val[imin+1:],"number_events_job")>-1:
                        newval=str(self.number_events_job)
                    #if imin2 > -1:
                     #   print self.outputfiles.keys()
                    if imin2 > -1 and val[imin2+4:] in self.outputfiles:
                        newval=self.outputfiles[ val[imin2+4:]]
                    try:
                        assert newval
                    except AssertionError:
                        raise ApplicationConfigurationError(None,"Error while parsing arguments: %s %d %d" % (val, imin, imin2))
                        
                    newarg="%s=%s" % (key,newval)
                else:
                    newarg=arg
                NewArgstring=NewArgstring+newarg+" "
            if self.dbrelease:
                dbfile="DBRelease-%s.tar.gz" % self.dbrelease
                NewArgstring=NewArgstring+"DBRelease=%s " % dbfile

            self.args.append(NewArgstring)
               
        if self.extraIncArgs:
            # incremental arguments: need to add the subjob number.
            arglist=string.split(self.extraIncArgs)
            NewArgstring=""
            for arg in arglist:
                key,val=string.split(arg,"=")
                ival=partition
                if not val.isdigit():
                    logger.warning("Non digit value entered for extraIncArgs: %s. Using %i as default value" % (str(val),ival))
                else:
                    ival+=string.atoi(val)
                newarg="%s=%i" %(key,ival)
                NewArgstring=NewArgstring+newarg+" "
            self.args.append(NewArgstring)

        try:
            assert len(self.args)>0
        except AssertionError:
            raise ApplicationConfigurationError(None,"Transformation with no arguments. Please check your inputs!")


        return (None,None)

    
    def master_configure(self):
       """Prepare the master job """
       self.prod_release,self.atlas_rel="",""
       self.turls,self.cavern_turls,self.minbias_turls,self.dbturls={},{},{},{}
       self.lfcs,self.cavern_lfcs,self.minbias_lfcs,self.dblfcs={},{},{},{}
       self.sites,self.cavern_sites,self.minbias_sites,self.dbsites=[],[],[],[]
       self.outputpaths,self.fileprefixes,self.outputfiles={},{},{}
       self.dsetmap,self.sitemap={},{}
       self.inputfiles,self.cavernfiles,self.mbfiles,self.dbfiles=[],[],[],[]
       self.infileString=""
       self.args=[]
       self.runNumber=""
       self.subjobsOutfiles={}
       self.evgen_job_option_filename=""
       
       job = self._getRoot()
       # basic checks
       try:
          assert self.mode in [ 'evgen', 'simul' , 'recon' , 'template']
       except AssertionError:
          logger.error('Variable application.mode: must be evgen, simul or recon or template')
          raise

       try:
           assert self.atlas_release
           assert self.number_events_job
       except AssertionError:
           logger.error('Please provide a start value for parameter atlas_release, number_events_job')
           raise
       
       if self.mode == "evgen":
          try:
             assert self.evgen_job_option
          except AssertionError:
             logger.error('Please provide a start value for parameter evgen_job_option needed for any evgen transformation')
          if os.path.exists(self.evgen_job_option):
              # need to strip the path away.
              self.evgen_job_option_filename = self.evgen_job_option.split("/")[-1]
          else:
              self.evgen_job_option_filename = self.evgen_job_option

          jobfields=self.evgen_job_option_filename.split(".")
          try:
              assert len(jobfields)==4 and jobfields[1].isdigit() and len(jobfields[1])==6

          except:
              raise ApplicationConfigurationError(None,"Badly formatted job option name %s. Transformation expects to find something named $project.$runNumber.$body.py, where $runNumber is a 6-digit number and $body does not contain any dot (.)" % self.evgen_job_option_filename )
          self.runNumber=self.run_number
          if not self.run_number:
              self.runNumber=str(jobfields[1])
          if not self.production_name:
              self.production_name=str(jobfields[1])+"."+str(jobfields[2])
              
       if self.mode in [ 'simul' , 'recon' ]:
           try:
               assert self.geometryTag
           except AssertionError:
               logger.error('Variable application.geometryTag: In step simul or recon with AtlasProduction python transforms, please provide detector geometry version tag')
               raise

       if string.count(self.atlas_release,".")==3:
           self.prod_release=self.atlas_release
           imax=string.rfind(self.atlas_release,".")
           self.atlas_rel=self.atlas_release[:imax]
       else:
           self.atlas_rel=self.atlas_release
       try:
           assert self.prod_release or self.transform_archive
       except:
           raise ApplicationConfigurationError(None,"A reference to the Production archive to be used must be set, either through the declaration of the archive itself in application.transform_archive or by putting the 4-digit production cache release number in application.atlas_release. Neither are set. Aborting.")
       
       if job.splitter:
           try:
               assert job.splitter._name=="AthenaMCSplitterJob"
           except AssertionError:
               raise ApplicationConfigurationError(None,'If you want to use a job splitter with the AthenaMC application, you have to use AthenaMCSplitterJob')
           
       # checking inputdata
       # handling of dbrelease

       if self.extraArgs:    
           arglist=string.split(self.extraArgs)
           for arg in arglist:
               key,val=string.split(arg,"=")
               digval=string.replace(val,".","0")
               if key=="DBRelease" and digval.isdigit():
                   self.dbrelease=val
                   if not job.inputdata:
                       job.inputdata=AthenaMCInputDatasets()
                   break
               
       if self.mode !="evgen" and self.mode !="template" and not self.dryrun:
           try:
               assert job.inputdata
           except :
               raise ApplicationConfigurationError(None,"job.inputdata must be used and set to 'AthenaMCInputDatasets'")
       if job.inputdata:
           try:
               assert job.inputdata._name == 'AthenaMCInputDatasets'
           except :
               raise ApplicationConfigurationError(None,"job.inputdata must be set to 'AthenaMCInputDatasets'")
           job.inputdata.get_dataset(self, job.backend._name)
##           print self.turls,self.lfcs,self.sites
##           print self.cavern_turls,self.cavern_lfcs,self.cavern_sites
##           print self.minbias_turls,self.minbias_lfcs,self.minbias_sites
##           print self.dbrelease,self.dbturls,self.dblfcs,self.dbsites
##           raise ApplicationConfigurationError(None,"debug")
       # checking output data
       try:
           assert (job.outputdata and job.outputdata._name=="AthenaMCOutputDatasets")
       except AssertionError:
           logger.error('AthenaMC now requires to set outputdata to AthenaMCOutputDatasets')
           raise
       # doing output data now
       self.fileprefixes,self.outputpaths=job.outputdata.prep_data(self)
       expected_datasets=""
       for filetype in self.outputpaths.keys():
           dataset=string.replace(self.outputpaths[filetype],"/",".")
           if dataset[0]==".": dataset=dataset[1:]
           if dataset[-1]==".": dataset=dataset[:-1]
           expected_datasets+=dataset+","

       if not job.outputdata.output_dataset or string.find(job.outputdata.output_dataset,",") > 0 : #update only if output_dataset is not used to force the output dataset names.
           job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.

           
       # This try block must be at the very end.
       try:
           assert self.production_name
       except:
           raise ApplicationConfigurationError("application.production_name was not set and could not be deduced from other input fields. Aborting")
         
       return (0,None)

from Ganga.GPIDev.Adapters.ISplitter import ISplitter

class AthenaMCSplitterJob(ISplitter):
    """AthenaMC handler job splitting"""
    
    _name = "AthenaMCSplitterJob"
    _schema = Schema(Version(1,0), {
       'numsubjobs': SimpleItem(defvalue=0,sequence=0, doc='Limit the number of subjobs. If this is left at 0, all partitions will be processed.'),
       'input_partitions' : SimpleItem(defvalue="",doc='List of input file numbers to be processed, either as a string in the format "1,3,5-10,15-" or as a list of integers. Alternative to output_partitions',typelist=["str","list"]),
       'output_partitions' : SimpleItem(defvalue="",doc='List of partition numbers to be processed, either as a string in the format "1,3,5-10,15-" or as a list of integers. Alternative to input_partitions',typelist=["str","list"]),
       'random_seeds' : SimpleItem(defvalue=[],doc='List of random seeds to use for the subjobs. Only used if it is a list',typelist=["str","list"])
        } )


    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []

        partitionList = job.application.getPartitionList()
        partitions = partitionList[0]
        openrange = partitionList[1]
        if self.numsubjobs:
            partitions = partitions[:self.numsubjobs]
            if openrange and len(partitions) < self.numsubjobs:
                missing = self.numsubjobs - len(partitions)
                partitions.extend(range(partitions[-1]+1, partitions[-1]+1+missing))
            openrange = False
        elif openrange:

            inputnumbers = job.application.getInputsForPartitions(partitions, job.inputdata)
            if inputnumbers:
                matchrange = (job._getRoot().inputdata.numbersToMatcharray(inputnumbers), openrange)
            else:
                matchrange = ([],False)
            infiles = [fn for fn in job.application.turls.keys() if matchFile(matchrange, fn)]
            innumbers = job._getRoot().inputdata.filesToNumbers(infiles)
            partitions = partitions[:-1] # the partition start of the open range beginning is not mandatory 
            partitions.extend(job.application.getPartitionsForInputs(innumbers, job.inputdata))
            partitions = dict([(i,1)for i in partitions]).keys() # make unique
            logger.warning("Number of subjobs %i determined from input dataset!" % len(partitions))

        try:
            assert partitions
        except AssertionError:
            logger.error('Partition to process could not be determined! Check if inputdata.skip_files or inputdata.skip_events do not skip your specified input partition!')
            raise
        
        i = 0
        for p in partitions:
            rndtemp = int(job.application.random_seed)+p
            if len(self.random_seeds) > i:
               rndtemp = self.random_seeds[i]
            i+=1
            j = Job()
            j.application = job.application
            j.application.random_seed = "%s" % rndtemp
            j.backend=job.backend
            j.inputdata=job.inputdata
            j.outputdata=job.outputdata
            j.inputsandbox=job.inputsandbox
            j.outputsandbox=job.outputsandbox
            j.application.partition_number=p
            subjobs.append(j)
        return subjobs

config = makeConfig('AthenaMC', 'AthenaMC configuration options')
logger = getLogger()

# some default values

# $Log: not supported by cvs2svn $
# Revision 1.22  2009/04/29 13:03:53  fbrochu
# Removed spurrious warnings
#
# Revision 1.21  2009/04/27 08:36:35  ebke
# Fixed dbrelease not available (necessary for tasks.overview())
#
# Revision 1.20  2009/04/23 15:25:11  fbrochu
# removed __init__ function , moved initialization of private members to master_configure()
#
# Revision 1.19  2009/04/23 09:54:12  ebke
# Added __init__ function to avoid global scope of lists
#
# Revision 1.18  2009/04/22 14:38:08  fbrochu
# bug fix for evgen job option file handling and missing transforms flags in panda
#
# Revision 1.17  2009/04/20 15:33:57  fbrochu
# Redistribution of code between AthenaMC.py and the RTHandlers. The application handles the preparation of input/output data through AthenaMCDatasets functions, while the RTHandlers just deal with backend specific issues and formatting, reading all the data they need from AthenaMC members.
#
# Revision 1.16  2009/04/03 12:48:30  fbrochu
# Adding new Run Time Handler for the Panda backend. Simplified AthenaMC interface: process_name is now inactive, and production_name can be used to fill in for run_number as well. Finally, an attempt to extract process_name for input job options and/or dataset has been implemented, reducing the total number of mandatory fields. Native backend member to select a given storage can be used to replace se_name as well. Arguments in extraIncArgs can be put in extraArgs as well, using the dedicated key word . Added length check on output dataset names.
#
# Revision 1.15  2009/03/10 16:02:49  fbrochu
# Adding support for AtlasTier0 production caches (to be tested), sort input sites by decreasing dataset shares and allow aggregation of output data files to the site specified in app.se_name by subscribing the frozen datasets at the completion of the master job
#
# Revision 1.14  2009/02/11 17:26:10  ebke
# Replaced many Exceptions with ApplicationConfigurationErrors
# Suggested at the Developer Days
#
# Revision 1.13  2009/02/09 15:00:42  fbrochu
# *** empty log message ***
#
# Revision 1.12  2009/02/09 14:33:49  fbrochu
# Migration to use of dq2-get for input data and strict job to data policy. Added migration classes in AthenaMCDatasets.py to allow display of old jobs. Bug fix for collection of adler32 checksum numbers of output files and improved debug output from stage-out script.
#
# Revision 1.11  2009/02/06 13:14:06  ebke
# *** empty log message ***
#
# Revision 1.10  2009/02/04 14:05:49  fbrochu
# Roll back creation of output containers and job subdatasets, added protection against cross-cloud input data replication, support for adler32 and use of lcg tools built-in timeout mecanism on top of existing timeout mecanism
#
# Revision 1.9  2009/01/16 14:09:22  ebke
# Added dryrun functionality to AthenaMC
#
# Revision 1.8  2009/01/15 09:50:42  ebke
# Fix for firstevent in AthenaMC
#
# Revision 1.7  2009/01/14 17:28:09  ebke
# * partition_number=None made evgen fail, fixed
# * getPartitionList now works without job object, then returns an open range starting at the first partition
#
# Revision 1.6  2008/12/12 10:17:42  elmsheus
# Changes for improved data handling and more splitting capabilities, goes along with new Tasks version from Johannes Ebke
#
# Revision 1.5  2008/10/21 06:43:09  elmsheus
# Change defvalue of partition_number
#
# Revision 1.4  2008/10/15 13:44:05  fbrochu
# Bug fix for inputdata.inputfiles type, improving timeout evaluation for stage-out, allowing support for stage-in files from SRMV2 space tokens, extending splitter to allow partial resubmission of master jobs (all failed subjobs at once instead of one by one.)
#
# Revision 1.3  2008/09/02 13:32:44  fbrochu
# Gave a type and a default value to app.number_events_job
#
# Revision 1.2  2008/07/30 13:23:55  fbrochu
# AthenaMC.py: added type checks to members. AthenaMCDatasets: bug fix for treatment of DQ2 datasets available only for Panda. AthenaMCLCGRTGandler.py and wrapper.sh: extension of the glite WMS workaround to input data
#
# Revision 1.1  2008/07/17 16:41:19  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.19.2.5  2008/04/22 11:23:22  fbrochu
# bug fix for template mode: taking into account first event/ skip parameters in list of transformation parameters
#
# Revision 1.19.2.4  2008/04/21 15:55:56  fbrochu
# Added new production mode, template, to handle any transformation not covered by the three existing modes. template arguments must be entered using application.extraArgs and extraIncArgs. See internal doc of application.mode for more details. Simplified wrapper.sh to allow support for input sandbox files (input weight files, custom job option fragments). Extended application.atlas_release to support 4-digits production cache releases. Using a 4-digit number in application.atlas_release will force the job to be sent to a site where the requested production cache is already deployed, meaning that application.transform_archive is not needed in this particular case. 3-digit numbers are still supported.
#
# Revision 1.19.2.3  2008/04/16 13:51:19  fbrochu
# cleaning up job scripts wrapper.sh and setup-release.sh to reduce spurrious error messages in stderr. Added new production mode, template, to cover all transformations that cannot be run with the three existing modes. template relies solely on the use of application.extraArgs and application.extraIncArgs to fill the transformation parameters. Input and Output data must be declared in dedicated inputdata and outputdata members, and then the relevant members must be declared in application.extraArgs like this: outputEvgenFile= if the file was declared in outputdata.outrootfile.
#
# Revision 1.19.2.2  2008/02/08 16:53:11  fbrochu
# Added support to PBS backend, new application members siteroot and cmtsite for ATLAS setup for Local and PBS job submissions, bug fixes for input dataset preparation and staging , handling of missing output files in fill()
#
# Revision 1.19.2.1  2008/01/07 13:37:10  elmsheus
# Update to new config schema
#
# Revision 1.19  2007/11/27 11:52:27  fbrochu
# bug fix for output_dataset broken string in AthenaMCDatasets.py and support for extra integer transformation variables incrementing with the subjob number (extraIncArgs)
#
# Revision 1.18  2007/07/30 09:26:04  elmsheus
# Move 4-4-0-dev-branch to head
#
# Revision 1.17.2.2  2007/07/30 09:09:57  fbrochu
# warning suppression
#
# Revision 1.17.2.1  2007/06/19 14:46:50  fbrochu
# initial export for 4.4.0 developments
#
# Revision 1.17  2007/05/29 11:27:54  fbrochu
# cosmetic changes: removal of extra coma at the end of application scheme in AthenaMC.py, downgrading two warnings to info and debug level
#
# Revision 1.16  2007/05/29 10:37:01  fbrochu
# Support for remote transformation archives (from web area repositories only, including official area), embedded all grid transactions of AthenaMCLCGRTHandler.py in gridShell calls and added verbosity member to AthenaMC application to control transformation verbosity
#
# Revision 1.15  2007/05/08 16:19:42  elmsheus
# Several patch provided by Johannes Ebke
#
# Revision 1.14  2007/05/04 08:14:44  fbrochu
# Corrected logic bug in AthenaMCLCGRTHandler.py, preventing processing of user-named datasets. Changed default value of version to the null string
#
# Revision 1.13  2007/05/01 09:42:09  fbrochu
# fixed typos in AthenaMC.py (new member version) and AthenaMCLCGRTHandler.py (support to Condor Backend). Revisited verbosity level of most warnings
#
# Revision 1.12  2007/04/24 15:34:58  fbrochu
# Added support to Nordugrid, Cronus and Condor backends. Revisited file naming conventions, added new application member: version
#
# Revision 1.11  2007/03/15 09:59:36  fbrochu
# corrected username bug in AthenaMC, added automatic registration of input castor files for LCG jobs
#
# Revision 1.10  2007/03/07 09:33:47  elmsheus
# Rename fill to postprocess
#
# Revision 1.9  2007/03/07 08:19:01  elmsheus
# Add fill method in application
#
# Revision 1.8  2007/02/23 13:25:11  fbrochu
# Port 4.2.X bug fixes, implement timestamp in output files, add support for LSF backend and local CASTOR files, redesigned I/O file management
#
# Revision 1.7  2006/11/24 13:32:38  elmsheus
# Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
# Add Frederics changes and improvement for AthenaMC
#
# Revision 1.6  2006/09/26 11:17:09  elmsheus
# Frederic: Merge of AthenaMC and AthenaMCpyJT, DQ2 updates
#
# Revision 1.5  2006/09/05 10:25:49  elmsheus
# Some bugfixes from Frederic
#
# Revision 1.2  2006/05/25 14:24:29  elmsheus
# Fix numsubjobs defvalue, introduce offset_job and T_CONTEXT
#
# Revision 1.1  2006/05/19 09:26:22  elmsheus
# initial import of AthenaMC JobTransform wrapper
#
