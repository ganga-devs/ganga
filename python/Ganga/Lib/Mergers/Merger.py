################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Merger.py,v 1.5 2009-03-18 10:46:01 wreece Exp $
################################################################################

from Ganga.GPIDev.Adapters.IMerger import MergerError, IMerger
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version

from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import commands
import os
import string

logger = getLogger()

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

def getMergerObject(file_ext):
    """Returns an instance of the correct merger tool, or None if there is not one"""
    from Ganga.Utility.Plugin.GangaPlugin import PluginManagerError
    result = None
    try:
        config = getConfig('Mergers')

        if file_ext == 'std_merge':
            result = allPlugins.find('mergers',config[file_ext])()
        else:
            #load the dictionary of file assocaitions
            file_types = eval(config['associate'])
            result = allPlugins.find('mergers',file_types[file_ext])()
    except ConfigError:
        pass
    except KeyError:
        pass
    except PluginManagerError:
        pass
    except SyntaxError:
        pass
    except TypeError:#TypeError as we may not be able to call ()
        pass #just return None
    return result

def runAutoMerge(job, new_status):
    """Method to run the merge command."""
    
    result = False

    #we only run on master jobs (which have no parent)
    if job._getParent() != None:
        return result
    
    allowed_states = ['completed','failed','killed']
    if not new_status in allowed_states:
        return result
    
    try:
        if job.merger:
            #we run if master is in a failed state if ignorefailed flag is set
            if new_status == allowed_states[0] or job.merger.ignorefailed:

                # leave the output directory to the implementation (fix for http://savannah.cern.ch/bugs/?76445)
                sum_outputdir = None
                if job.merger.set_outputdir_for_automerge:
                    sum_outputdir = job.outputdir

                result = job.merger.merge(job.subjobs, sum_outputdir)

    except Exception:
        log_user_exception()
        raise
    
    return result



class IMergeTool(GangaObject):
    """This is an interface class for the a stateless merge tool. Concrete merge tools should inherit from it"""
    _category = 'merge_tools'
    _hidden = 1
    _name = 'IMergeTool'
    _schema =  Schema(Version(1,0), {})
    
    def mergefiles(self, file_list, output_file):
        """
        file_list: A list of fully qualified file names that should be merged together.
        output_file: The name of the file to write the merge results to.

        If the merge fails for any reason, then a MergerError should be thrown.
        """
        raise NotImplementedError

class AbstractMerger(IMerger):
    """
    The idea behind this class is to put all of the checking and user interaction in this class, and then use a very simple
    stateless merge_tool to actually do the relevant merge. The Abstract label is perhaps misleading, but I intend all Merger
    objects to inherit from this.
    """
    _schema = Schema(Version(1,0), {
        'files' : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc='A list of files to merge.'),
        'merge_tool' : ComponentItem('merge_tools', defvalue = None, doc='The merge tool to use.', hidden = 1),
        'ignorefailed' : SimpleItem(defvalue = False, doc='Jobs that are in the failed or killed states will be excluded from the merge when this flag is set to True.'),
        'overwrite' : SimpleItem(defvalue = False, doc='The default behaviour for this Merger object. Will overwrite output files.')
        } )
    _category = 'mergers'
    _name = 'AbstractMerger'
    _hidden = 1
    
    _GUIPrefs = [{'attribute' : 'ignorefailed', 'widget' : 'Bool'},
                 {'attribute' : 'overwrite', 'widget' : 'Bool'}]

    success = True
    failure = False

    def __init__(self, merge_tool):
        super(AbstractMerger,self).__init__()
        self.merge_tool = merge_tool

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        """
        Method to merge the output of jobs.
        
        jobs may be a single job instance or a sequence of Jobs
        outputdir is the name of the directry to put the merge results in. It will be created if needed.
        ignorefailed and overwrite have the same meaning as in the schema, but override the schema values.

        returns whether the merge was successful or not as a boolean
        """
        
        if ignorefailed == None:
            ignorefailed = self.ignorefailed

        if overwrite == None:
            overwrite = self.overwrite

        # special case the passing of a Job object.
        from Ganga.GPIDev.Lib.Job import Job
        if isinstance(jobs,GPIProxyObject) and isinstance(jobs._impl,Job):
            if outputdir is None:
                outputdir = jobs.outputdir
            return self.merge(jobs.subjobs,outputdir = outputdir, ignorefailed = ignorefailed, overwrite = overwrite)

        if len(jobs) == 0:
            logger.warning('The jobslice given was empty. The merge will not continue.')
            return self.success

        if not outputdir:
            outputdir = getDefaultMergeDir()
        else:
            if isinstance(outputdir,GPIProxyObject) and isinstance(outputdir._impl,Job):
                #use info from job
                outputdir = outputdir.outputdir
            else:
                outputdir = os.path.expanduser(outputdir)

        files = {}
        for f in self.files:
            files[f] = []
        for j in jobs:
            #first check that the job is ok
            if j.status != 'completed':
                #check if we can keep going
                if j.status == 'failed' or j.status == 'killed':
                    if ignorefailed:
                        logger.warning('Job %s has status %s and is being ignored.', j.fqid, j.status)
                        continue
                    else:
                        logger.error('Job %s has status %s and so the merge can not continue. '\
                                     'This can be overridden with the ignorefailed flag.', j.fqid, j.status)
                        return self.failure
                else:
                    logger.error("Job %s is in an unsupported status %s and so the merge can not continue. '\
                    'Supported statuses are 'completed', 'failed' or 'killed' (if the ignorefailed flag is set).", j.fqid, j.status)
                    return self.failure

            #run the merge recursively
            if not j.merger and len(j.subjobs):
                sub_result = self.merge(j.subjobs,outputdir = j.outputdir, ignorefailed = ignorefailed, overwrite = overwrite)
                if (sub_result == self.failure) and not ignorefailed:
                    logger.error('The merge of Job %s failed and so the merge can not continue. '\
                                 'This can be overridden with the ignorefailed flag.', j.fqid)
                    return self.failure
            
                
            for f in files.keys():
                p = os.path.join(j.outputdir,f)
                if not os.path.exists(p):
                    if ignorefailed:
                        logger.warning('The file %s in Job %s was not found. The file will be ignored.',str(f),j.fqid)
                        continue
                    else:
                        logger.error('The file %s in Job %s was not found and so the merge can not continue. '\
                                     'This can be overridden with the ignorefailed flag.', str(f), j.fqid)
                        return self.failure
                files[f].append(p)

        for k in files.keys():
            # make sure we are not going to over write anything
            outputfile = os.path.join(outputdir,k)

            if os.path.exists(outputfile) and not overwrite:
                logger.error('The merge process can not continue as it will result in over writing. '\
                             'Either move the file %s or set the overwrite flag to True.', str(outputfile))
                return self.failure

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
                    logger.error('Output file %s equals input file %s. The merge will fail.',
                                 outputfile, f)
                    return self.failure
                            
            #merge the lists of files with a merge tool into outputfile
            msg = None
            try:
                self.merge_tool.mergefiles(files[k],outputfile)

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
                
            except MergerError, e:
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
                
    
class _TextMergeTool(IMergeTool):
    """Very simple merge tool to cat together text files. Adds a few simple headers."""
    _category = 'merge_tools'
    _hidden = 1
    _name = '_TextMergeTool'
    _schema = IMergeTool._schema.inherit_copy()
    _schema.datadict['compress'] = SimpleItem(defvalue = False, doc='Output should be compressed with gzip.')

    def mergefiles(self, file_list, output_file):

        import time

        if self.compress or output_file.lower().endswith('.gz'):
            #use gzip
            import gzip
            if not output_file.lower().endswith('.gz'):
                output_file += '.gz'
            out_file = gzip.GzipFile(output_file,'w')
        else:
            out_file = file(output_file,'w')

        out_file.write('# Ganga TextMergeTool - %s #\n' % time.asctime())
        for f in file_list:

            if not f.lower().endswith('.gz'):
                in_file = file(f)
            else:
                import gzip
                in_file = gzip.GzipFile(f)

            out_file.write('# Start of file %s #\n' % str(f))
            out_file.write(in_file.read())
            out_file.write('\n')

            in_file.close()

        out_file.write('# Ganga Merge Ended Successfully #\n')
        out_file.flush()
        out_file.close()

class _RootMergeTool(IMergeTool):
    """Wrapper around hadd that merges root files."""

    _category = 'merge_tools'
    _hidden = 1
    _name = '_RootMergeTool'
    _schema = IMergeTool._schema.inherit_copy()
    _schema.datadict['args'] = SimpleItem(defvalue = None, doc='Arguments to be passed to hadd.',\
                                          typelist=['str','type(None)'])

    def mergefiles(self, file_list, output_file):

        from Ganga.Utility.root import getrootprefix, checkrootprefix
        rc, rootprefix =  getrootprefix()

        if rc != 0:
            raise MergerError('ROOT has not been properly configured. Check your .gangarc file.')

        if checkrootprefix():
            raise MergerError('Can not run ROOT correctly. Check your .gangarc file.')

        #we always force as the overwrite is handled by our parent
        default_arguments = '-f'
        merge_cmd = rootprefix + 'hadd '
        if self.args: #pass any args on
            merge_cmd += ' %s ' % self.args
      
        #don't add a -f unless needed  
        if not default_arguments in merge_cmd:
            merge_cmd += ' %s ' % default_arguments
        

        #add the list of files, output file first
        arg_list = [output_file]
        arg_list.extend(file_list)
        merge_cmd += string.join(arg_list,' ')

        rc, out = commands.getstatusoutput(merge_cmd)
        if rc:
            logger.error(out)
            raise MergerError('The ROOT merge failed to complete. The command used was %s.' % merge_cmd)


            
class TextMerger(AbstractMerger):
    """Merger class for text

    TextMerger will append specified text files in the order that they are
    encountered in the list of Jobs. Each file will be separated by a header
    giving some very basic information about the individual files.

    Usage:

    tm = TextMerger()
    tm.files = ['job.log','results.txt']
    tm.overwrite = True #False by default
    tm.ignorefailed = True #False by default

    # will produce the specified files
    j = Job() 
    j.outputsandbox = ['job.log','results.txt']
    j.splitter = SomeSplitter()
    j.merger = tm
    j.submit()

    The merge object will be used to merge the output of
    each subjob into j.outputdir. This will be run when
    the job completes. If the ignorefailed flag has been set
    then the merge will also be run as the job enters the
    killed or failed states.
    
    The above merger object can also be used independently
    to merge a list of jobs or the subjobs of an single job.

    #tm defined above
    tm.merge(j, outputdir = '~/merge_dir')
    tm.merge([.. list of jobs ...], '~/merge_dir', ignorefailed = True, overwrite = False)

    If ignorefailed or overwrite are set then they override the values set on the
    merge object.

    If outputdir is not specified, the default location specfied
    in the [Mergers] section of the .gangarc file will be used.

    For large text files it may be desirable to compress the merge
    result using gzip. This can be done by setting the compress
    flag on the TextMerger object. In this case, the merged file
    will have a '.gz' appended to its filename.

    A summary of all the files merged will be created for each entry in files.
    This will be created when the merge of those files completes
    successfully. The name of this is the same as the output file, with the
    '.merge_summary' extension appended and will be placed in the same directory
    as the merge results.

    """
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'TextMerger'
    _schema = AbstractMerger._schema.inherit_copy()
    _schema.datadict['compress'] = SimpleItem(defvalue = False, doc='Output should be compressed with gzip.')
        

    def __init__(self):
        super(TextMerger,self).__init__(_TextMergeTool())

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        self.merge_tool.compress = self.compress
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(TextMerger,self).merge(jobs, outputdir, ignorefailed, overwrite)

class RootMerger(AbstractMerger):
    """Merger class for ROOT files

    RootMerger will use the version of ROOT configured in the .gangarc file to
    add together histograms and trees using the 'hadd' command provided by ROOT.
    Further details of the hadd command can be found in the ROOT documentation.

    Usage:

    rm = RootMerger()
    rm.files = ['hist.root','trees.root']
    rm.overwrite = True #False by default
    rm.ignorefailed = True #False by default
    rm.args = '-f2' #pass arguments to hadd

    # will produce the specified files
    j = Job() 
    j.outputsandbox = ['hist.root','trees.root']
    j.splitter = SomeSplitter()
    j.merger = rm
    j.submit()

    The merge object will be used to merge the output of
    each subjob into j.outputdir. This will be run when
    the job completes. If the ignorefailed flag has been set
    then the merge will also be run as the job enters the
    killed or failed states.

    The above merger object can also be used independently
    to merge a list of jobs or the subjobs of an single job.

    #rm defined above
    rm.merge(j, outputdir = '~/merge_dir')
    rm.merge([.. list of jobs ...], '~/merge_dir', ignorefailed = True, overwrite = False)

    If ignorefailed or overwrite are set then they override the
    values set on the merge object.

    A summary of all the files merged will be created for each entry in files.
    This will be created when the merge of those files completes
    successfully. The name of this is the same as the output file, with the
    '.merge_summary' extension appended and will be placed in the same directory
    as the merge results.

    If outputdir is not specified, the default location specfied
    in the [Mergers] section of the .gangarc file will be used.

    """
    
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'RootMerger'
    _schema = AbstractMerger._schema.inherit_copy()
    _schema.datadict['args'] = SimpleItem(defvalue = None, doc='Arguments to be passed to hadd.',\
                                          typelist=['str','type(None)'])

    def __init__(self):
        super(RootMerger,self).__init__(_RootMergeTool())

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        self.merge_tool.args = self.args
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(RootMerger,self).merge(jobs, outputdir, ignorefailed, overwrite)


class MultipleMerger(IMerger):
    """Merger class when merges of different file types are needed.

    Here is a typical usage example:

    # job produces both Root and Text files
    j = Job()

    tm = TextMerger()
    tm.files = ['job.log','stdout']
    tm.overwrite = True

    rm = RootMerger()
    rm.files = ['histo.root','tree.root']
    rm.ignorefailed = True

    mm = MultipleMerger()
    mm.addMerger(tm)
    mm.addMerger(rm)

    j.merger = mm
    # All files will be merged on completion
    j.submit()

    MultipleMerger objects can also be used on
    individual Jobs or lists of Jobs.

    #mm defined above
    mm.merge([..list of Jobs ...], outputdir = '~/merge_results', ignorefailed = False, overwrite = True)

    The ignorefailed and overwrite flags are
    propagated to the individual Merger objects.

    If outputdir is not specified, the default location
    specfied in the [Mergers] section of the .gangarc
    file will be used.

    It is permissible to nest MultipleMerger objects
    inside one another if extra hierarchy is desired.

    """

    _category = 'mergers'
    _exportmethods = ['addMerger','merge']
    _name = 'MultipleMerger'
    _schema = Schema(Version(1,0), {
        'merger_objects' : ComponentItem('mergers', defvalue = [], doc = 'A list of Merge objects to run', sequence = 1)
        })

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        #run the merger objects one at a time
        merge_results = []
        for m in self.merger_objects:
            #stop infinite recursion
            if m is self:
                continue
            #run the merge
            merge_results.append(m.merge(jobs, outputdir, ignorefailed = ignorefailed, overwrite = overwrite))
        #if one fails then we all fail
        return not False in merge_results

    def addMerger(self, merger_object):
        """Adds a merger object to the list of merges to be done."""
        self.merger_objects.append(merger_object)

def findFilesToMerge(jobs):
    """Look at a list of jobs and find a set of files present in each job that can be merged together"""
    
    result = []
    
    file_map = {}
    jobs_len = len(jobs)
    for j in jobs:
        for file_name in j.outputsandbox:
            file_map[file_name] = file_map.setdefault(file_name,0) + 1
    
    for file_name, count in file_map.iteritems():
        if count == jobs_len: result.append(file_name)
        else:
            logger.warning('The file %s was not found in all jobs to be merged and so will be ignored.', file_name)
    logger.info('No files specified, so using %s.', str(result))
        
    return result
    

class SmartMerger(IMerger):
    """Allows the different types of merge to be run according to file extension in an automatic way.

    SmartMerger accepts a list of files which it will delegate to individual Merger objects based on
    the file extension of the file. The mapping between file extensions and Merger objects can
    be defined in the [Mergers] section of the .gangarc file. Extensions are treated in a case
    insensitive way. If a file extension is not recognized than the file will be ignored if the
    ignorefailed flag is set, or the merge will fail.

    Example:

    sm = SmartMerger()
    sm.files = ['stderr','histo.root','job.log','summary.txt','trees.root','stdout']
    sm.merge([... list of jobs ...], outputdir = '~/merge_dir')#also accepts a single Job

    If outputdir is not specified, the default location specfied in the [Mergers]
    section of the .gangarc file will be used.
    
    If files is not specified, then it will be taken from the list of jobs given to
    the merge method. Only files which appear in all jobs will be merged.

    SmartMergers can also be attached to Job objects in the same way as other Merger
    objects.

    #sm defined above
    j = Job()
    j.splitter = SomeSplitter()
    j.merger = sm
    j.submit() 
    
    """
    
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'SmartMerger'
    _schema = Schema(Version(1,0), {
        'files' : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc='A list of files to merge.'),
        'ignorefailed' : SimpleItem(defvalue = False, doc='Jobs that are in the failed or killed states will be excluded from the merge when this flag is set to True.'),
        'overwrite' : SimpleItem(defvalue = False, doc='The default behaviour for this Merger object. Will overwrite output files.')
        } )
        
    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):

        #following same as in AbstractMerger
        if ignorefailed == None:
            ignorefailed = self.ignorefailed
            
        if overwrite == None:
            overwrite = self.overwrite

        # special case the passing of a Job object.
        from Ganga.GPIDev.Lib.Job import Job
        if isinstance(jobs,Job):
            if outputdir is None:
                outputdir = jobs.outputdir
            return self.merge(jobs.subjobs,outputdir = outputdir, ignorefailed = ignorefailed, overwrite = overwrite)

        if len(jobs) == 0:
            logger.warning('The jobslice given was empty. The merge will not continue.')
            return AbstractMerger.success
        
        #make a guess of what to merge if nothing is specified
        if not self.files:
            self.files = findFilesToMerge(jobs)

        type_map = {}
        for f in self.files:

            if not getMergerObject(f):

                #find the file extension and check
                file_ext = os.path.splitext(f)[1].lstrip('.')

                # default to txt
                if not file_ext:
                    if f in ['stdout','stderr']:
                        file_ext = 'std_merge'
                    elif ignorefailed:
                        logger.warning('File extension not found for file %s and so the file will be ignored. '\
                                       'Check the name of the file.',f)
                        continue
                    else:
                        logger.warning('File extension not found for file %s and so the merge will fail. '\
                                       'Check the name of the file or set the ignorefailed flag.',f)
                        return AbstractMerger.failure

                file_ext = file_ext.lower()#treat as lowercase

            else:
                #allow per file config
                file_ext = f
            
            #store the file association
            type_map.setdefault(file_ext, []).append(f)

        #we are going to use the MultipleMerger objects to do all this
        multi_merge = MultipleMerger()
        for ext in type_map.keys():

            merge_object = getMergerObject(ext) # returns an instance
            if merge_object == None:
                logger.error('Extension %s not recognized and so the merge will fail. '\
                            'Check the [Mergers] section of your .gangarc file.', ext)
                return AbstractMerger.failure

            #we have a merge object, so lets go...
            merge_object.files = type_map[ext]
            merge_object.ignorefailed = ignorefailed
            merge_object.overwrite = overwrite
            #add to multimerge
            multi_merge.addMerger(merge_object)

        return multi_merge.merge(jobs, outputdir = outputdir, ignorefailed = ignorefailed, overwrite = overwrite)
    
class _CustomMergeTool(IMergeTool):
    """Allows arbitrary python modules to be used to merge"""

    _category = 'merge_tools'
    _hidden = 1
    _name = '_CustomMergeTool'
    _schema = IMergeTool._schema.inherit_copy()
    _schema.datadict['module'] = FileItem(defvalue = None, doc='Path to a python module to perform the merge.')

    def mergefiles(self, file_list, output_file):

        import os
        if not os.path.exists(self.module.name):
            raise MergerError("The module '&s' does not exist and so merging will fail.",self.module.name)
        
        try:
            
            module_contents = ''
            result = False
            
            try:
                module_file = file(self.module.name)
                module_contents = module_file.read()
            finally:
                module_file.close()
            
            if module_contents:
                module_contents += """
_xxxResult = mergefiles(%s , '%s') 
                """ % (file_list, output_file)
            
                import tempfile
                out_file = tempfile.mktemp('.py')
                try:
                    out = file(out_file,'w')
                    out.write(module_contents)
                finally:
                    out.close()
            
                ns = {}
                execfile(out_file,ns)
                os.unlink(out_file)
                
                result = ns.get('_xxxResult',1)
                
            if result != 0:
                raise MergerError('The merge module returned False or did not complete properly')
            
            
        except Exception,e:
            raise MergerError("Merge failed: ('%s')" % str(e))
            
        

class CustomMerger(AbstractMerger):
    """User tool for writing custom merging tools with Python
    
    Allows a script to be supplied that performs the merge of some custom file type.
    The script must be a python file which defines the following function:
    
    def mergefiles(file_list, output_file):
    
        #perform the merge
        if not success:
            return -1
        else:
            return 0
            
    This module will be imported and used by the CustomMerger. The file_list is a
    list of paths to the files to be merged. output_file is a string path for
    the output of the merge. This file must exist by the end of the merge or the
    merge will fail. If the merge cannot proceed, then the function should return a 
    non-zero integer.
    
    Clearly this tool is provided for advanced ganga usage only, and should be used with
    this in mind.
    
    """
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'CustomMerger'
    _schema = AbstractMerger._schema.inherit_copy()
    _schema.datadict['module'] = FileItem(defvalue = None, doc='Path to a python module to perform the merge.')
        

    def __init__(self):
        super(CustomMerger,self).__init__(_CustomMergeTool())

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        if self.module is None or not self.module:
            logger.error('No custom module specified. The merge will end now')
            return AbstractMerger.success
        self.merge_tool.module = self.module
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(CustomMerger,self).merge(jobs, outputdir, ignorefailed, overwrite)
    


#configure the plugins
allPlugins.add(_CustomMergeTool,'merge_tools','_CustomMergeTool') 
allPlugins.add(_TextMergeTool,'merge_tools','_TextMergeTool')
allPlugins.add(_RootMergeTool,'merge_tools','_RootMergeTool')        
#we need a default, but don't care much what it is
allPlugins.setDefault('merge_tools','_TextMergeTool')


