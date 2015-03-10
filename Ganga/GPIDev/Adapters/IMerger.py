################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IMerger.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor, MultiPostProcessor
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import commands
import os
import string

from posixpath import curdir, sep, pardir, join, abspath, commonprefix

def relpath(path, start=curdir):
    """Return a relative version of a path"""
    if not path:
        raise ValueError("no path specified")
    start_list = abspath(start).split(sep)
    path_list = abspath(path).split(sep)
    # Work out how much of the filepath is shared by start and path.
    i = len(commonprefix([start_list, path_list]))
    rel_list = [pardir] * (len(start_list)-i) + path_list[i:]
    if not rel_list:
        return curdir
    return join(*rel_list)

#set the mergers config up
config = makeConfig('Mergers','parameters for mergers')
config.addOption('associate',"{'log':'TextMerger','root':'RootMerger',"
                 "'text':'TextMerger','txt':'TextMerger'}",'Dictionary of file associations')
gangadir = getConfig('Configuration')['gangadir']
config.addOption('merge_output_dir', gangadir+'/merge_results',"location of the merger's outputdir")
config.addOption('std_merge','TextMerger','Standard (default) merger')

def getDefaultMergeDir():
    """Gets the default location of the mergers outputdir from the config"""
    
    outputdir = gangadir + "/merge_results"
    try:
        config = getConfig('Mergers')
        outputdir = config['merge_output_dir']
    except ConfigError:
        pass
    return os.path.expanduser(outputdir)

class IMerger(IPostProcessor):
    """
    Contains the interface for all mergers, all mergers should inherit from this object.
    """

    # set outputdir for auto merge policy flag
    # the default behaviour (True) is that outputdir is set by runAutoMerge() function in Merger.py module
    # however if this flag is set to False then merge() will be called for auto merge with sum_outputdir set to None
    # thus it is up to the subclass to decide where the output goes in case of auto merge
    set_outputdir_for_automerge = True


    _category = 'postprocessor'
    _exportmethods = ['merge']
    _name = 'IMerger'
    _hidden = 1
    _schema = Schema(Version(1,0), {
        'files' : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc='A list of files to merge.'),
        'ignorefailed' : SimpleItem(defvalue = False, doc='Jobs that are in the failed or killed states will be excluded from the merge when this flag is set to True.'),
        'overwrite' : SimpleItem(defvalue = False, doc='The default behaviour for this Merger object. Will overwrite output files.'),
        } )
    order = 1
    def execute(self,job,newstatus):
        """
        Execute
        """
        if (len(job.subjobs) != 0):
            try: return self.merge(job.subjobs, job.outputdir)
            except PostProcessException, e:
                logger.error(str(e))
                return self.failure
        else:
            return True

        
    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):

        if ignorefailed == None:
            ignorefailed = self.ignorefailed
            
        if overwrite == None:
            overwrite = self.overwrite

        from Ganga.GPIDev.Lib.Job import Job

        if not outputdir:
            outputdir = getDefaultMergeDir()
        else:
            if isinstance(outputdir,GPIProxyObject) and isinstance(outputdir._impl,Job):
                #use info from job
                outputdir = outputdir.outputdir
            else:
                outputdir = os.path.expanduser(outputdir)

        files = {}


        if isinstance(jobs,GPIProxyObject) and isinstance(jobs._impl,Job):
            if outputdir is None:
                outputdir = jobs.outputdir
            return self.merge(jobs.subjobs,outputdir = outputdir, ignorefailed = ignorefailed, overwrite = overwrite)

        if not len(jobs):
            logger.warning('The jobslice given was empty. The merge will not continue.')
            return self.success

        for j in jobs:
            #first check that the job is ok
            if j.status != 'completed':
                #check if we can keep going
                if j.status == 'failed' or j.status == 'killed':
                    if ignorefailed:
                        logger.warning('Job %s has status %s and is being ignored.', j.fqid, j.status)
                        continue
                    else:
                        raise PostProcessException('Job %s has status %s and so the merge can not continue. '\
                                     'This can be overridden with the ignorefailed flag.'%(j.fqid,j.status))
                else:
                    raise PostProcessException("Job %s is in an unsupported status %s and so the merge can not continue. '\
                    'Supported statuses are 'completed', 'failed' or 'killed' (if the ignorefailed flag is set)."%(j.fqid,j.status))

            if len(j.subjobs):
                sub_result = self.merge(j.subjobs,outputdir = j.outputdir, ignorefailed = ignorefailed, overwrite = overwrite)
                if (sub_result == self.failure) and not ignorefailed:
                    raise PostProcessException('The merge of Job %s failed and so the merge can not continue. '\
                                 'This can be overridden with the ignorefailed flag.'%j.fqid)
            
            import glob 
            for f in self.files:

                for matchedFile in glob.glob(os.path.join(j.outputdir,f)):
                    relMatchedFile = ''
                    try:
                        relMatchedFile = os.path.relpath(matchedFile,j.outputdir)
                    except:
                        relMatchedFile = relpath(matchedFile,j.outputdir)
                    if relMatchedFile in files:
                        files[relMatchedFile].append(matchedFile)
                    else:
                        files[relMatchedFile] = [matchedFile]    

                if not len(glob.glob(os.path.join(j.outputdir,f))):
                    if ignorefailed:
                        logger.warning('The file pattern %s in Job %s was not found. The file will be ignored.',str(f),j.fqid)
                        continue
                    else:
                        raise PostProcessException('The file pattern %s in Job %s was not found and so the merge can not continue. '\
                                     'This can be overridden with the ignorefailed flag.'%(str(f), j.fqid))
                #files[f].extend(matchedFiles)

        for k in files.keys():
            # make sure we are not going to over write anything
            outputfile = os.path.join(outputdir,k)
            if os.path.exists(outputfile) and not overwrite:
                raise PostProcessException('The merge process can not continue as it will result in over writing. '\
                             'Either move the file %s or set the overwrite flag to True.'%str(outputfile))

            #make the directory if it does not exist
            if not os.path.exists(outputdir):
                os.mkdir(outputdir)

            #recreate structure from output sandbox
            outputfile_dirname = os.path.dirname(outputfile)
            if outputfile_dirname != outputdir:
                if not os.path.exists(outputfile_dirname):
                    os.mkdir(outputfile_dirname)

            #check that we are merging some files
            if not files[k]:
                logger.warning('Attempting to merge with no files. Request will be ignored.')
                continue

            #check outputfile != inputfile
            for f in files[k]:
                if f == outputfile:
                    raise PostProcessException('Output file %s equals input file %s. The merge will fail.'%(outputfile, f))
            #merge the lists of files with a merge tool into outputfile
            msg = None
            try:
                self.mergefiles(files[k],outputfile)

                #create a log file of the merge
                #we only get to here if the merge_tool ran ok
                log_file = '%s.merge_summary' % outputfile
                log = file(log_file,'w')
                try:
                    log.write('# -- List of files merged -- #\n')
                    for f in files[k]:
                        log.write('%s\n' % f)
                    log.write('# -- End of list -- #\n')
                finally:
                    log.close()
                
            except PostProcessException, e:
                msg = str(e)

                #store the error msg
                log_file = '%s.merge_summary' % outputfile
                log = file(log_file,'w')
                try:
                    log.write('# -- Error in Merge -- #\n')
                    log.write('\t%s\n' % msg)
                finally:
                    log.close()
                raise e

        return self.success

    
