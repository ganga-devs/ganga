##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Merger.py,v 1.5 2009-03-18 10:46:01 wreece Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IMerger import IMerger
from Ganga.GPIDev.Schema import FileItem, SimpleItem
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.Utility.Config import ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger
import commands
import os
import string
import copy

logger = getLogger()


def getMergerObject(file_ext):
    """Returns an instance of the correct merger tool, or None if there is not one"""
    from Ganga.Utility.Plugin.GangaPlugin import PluginManagerError
    result = None
    try:
        config = getConfig('Mergers')
        if file_ext == 'std_merge':
            result = allPlugins.find('postprocessor', config[file_ext])()
        else:
            # load the dictionary of file assocaitions # Why was there _ever_ an eval statement here? rcurrie
            file_types = config['associate']
            associate_merger = file_types[file_ext]
            result = allPlugins.find('postprocessor', associate_merger)()
    except ConfigError, err:
        logger.debug("ConfError %s" % str(err))
    except KeyError, err:
        logger.debug("KeyError %s" % str(err))
    except PluginManagerError, err:
        logger.debug("PluginError %s" % str(err))
    except SyntaxError, err:
        logger.debug("SyntaxError %s" % str(err))
    except TypeError, err:  # TypeError as we may not be able to call ()
        logger.debug("TypeError %s" % str(err))
    return result


class TextMerger(IMerger):

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
    _category = 'postprocessor'
    _name = 'TextMerger'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['compress'] = SimpleItem(
        defvalue=False, doc='Output should be compressed with gzip.')

    def mergefiles(self, file_list, output_file):

        import time

        if self.compress or output_file.lower().endswith('.gz'):
            # use gzip
            import gzip
            if not output_file.lower().endswith('.gz'):
                output_file += '.gz'
            out_file = gzip.GzipFile(output_file, 'w')
        else:
            out_file = open(output_file, 'w')

        out_file.write('# Ganga TextMergeTool - %s #\n' % time.asctime())
        for f in file_list:

            if not f.lower().endswith('.gz'):
                in_file = open(f)
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


class RootMerger(IMerger):

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

    _category = 'postprocessor'
    _name = 'RootMerger'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['args'] = SimpleItem(defvalue=None, doc='Arguments to be passed to hadd.',
                                          typelist=[str, None])

    def mergefiles(self, file_list, output_file):

        from Ganga.Utility.root import getrootprefix, checkrootprefix
        rc, rootprefix = getrootprefix()

        if rc != 0:
            raise PostProcessException(
                'ROOT has not been properly configured. Check your .gangarc file.')

        if checkrootprefix():
            raise PostProcessException(
                'Can not run ROOT correctly. Check your .gangarc file.')

        # we always force as the overwrite is handled by our parent
        default_arguments = '-f'
        merge_cmd = rootprefix + 'hadd '
        if self.args:  # pass any args on
            merge_cmd += ' %s ' % self.args

        # don't add a -f unless needed
        if not default_arguments in merge_cmd:
            merge_cmd += ' %s ' % default_arguments

        # add the list of files, output file first
        arg_list = [output_file]
        arg_list.extend(file_list)
        merge_cmd += string.join(arg_list, ' ')

        rc, out = commands.getstatusoutput(merge_cmd)

        log_file = '%s.hadd_output' % output_file
        with open(log_file, 'w') as log:
            log.write('# -- Hadd output -- #\n')
            log.write('%s\n' % out)

        if rc:
            logger.error(out)
            raise PostProcessException(
                'The ROOT merge failed to complete. The command used was %s.' % merge_cmd)


class CustomMerger(IMerger):

    """User tool for writing custom merging tools with Python

    Allows a script to be supplied that performs the merge of some custom file type.
    The script must be a python file which defines the following function:

    def merge(file_list, output_file):

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
    _category = 'postprocessor'
    _name = 'CustomMerger'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['module'] = FileItem(
        defvalue=None, doc='Path to a python module to perform the merge.')

    def mergefiles(self, file_list, output_file):

        import os
        if isinstance(self.module, IGangaFile):
            module_name = os.path.join(self.module.localDir, self.module.namePattern)
        elif isinstance(self.module, File):
            module_name = self.module.name
        else:
            module_name = self.module
        if not os.path.exists(module_name):
            raise PostProcessException("The module '&s' does not exist and so merging will fail.", module_name)
        result = False
        try:
            ns = {'file_list': copy.copy(file_list),
                  'output_file': copy.copy(output_file)}
            execfile(module_name, ns)
            exec('_result = mergefiles(file_list, output_file)', ns)
            result = ns.get('_result', result)
        except Exception as e:
            raise PostProcessException('There was a problem executing the custom merge: %s. Merge will fail.' % e)
        if result is not True:
            raise PostProcessException('The custom merge did not return True, merge will fail.')
        return self.success


def findFilesToMerge(jobs):
    """Look at a list of jobs and find a set of files present in each job that can be merged together"""

    result = []

    file_map = {}
    jobs_len = len(jobs)
    for j in jobs:
        if j.outputsandbox != []:
            for file_name in j.outputsandbox:
                file_map[file_name] = file_map.setdefault(file_name, 0) + 1
        elif j.outputfiles != []:
            for file_name in j.outputfiles:
                if isType(file_name, LocalFile):
                    file_map[file_name.namePattern] = file_map.setdefault(file_name.namePattern, 0) + 1

    for file_name, count in file_map.iteritems():
        if count == jobs_len:
            result.append(file_name)
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

    Mergers can also be attached to Job objects in the same way as other Merger
    objects.

    #sm defined above
    j = Job()
    j.splitter = SomeSplitter()
    j.merger = sm
    j.submit() 

    """

    _category = 'postprocessor'
    _exportmethods = ['merge']
    _name = 'SmartMerger'
    _schema = IMerger._schema.inherit_copy()

    def merge(self, jobs, outputdir=None, ignorefailed=None, overwrite=None):

        if ignorefailed is None:
            ignorefailed = self.ignorefailed

        if overwrite is None:
            overwrite = self.overwrite

        # make a guess of what to merge if nothing is specified
        if not self.files:
            self.files = findFilesToMerge(jobs)

        type_map = {}
        for f in self.files:

            if not getMergerObject(f):

                # find the file extension and check
                file_ext = os.path.splitext(f)[1].lstrip('.')

                # default to txt
                if not file_ext:
                    if f in ['stdout', 'stderr']:
                        file_ext = 'std_merge'
                    elif ignorefailed:
                        logger.warning('File extension not found for file %s and so the file will be ignored. '
                                       'Check the name of the file.', f)
                        continue
                    else:
                        logger.warning('File extension not found for file %s and so the merge will fail. '
                                       'Check the name of the file or set the ignorefailed flag.', f)
                        return self.failure

                file_ext = file_ext.lower()  # treat as lowercase

            else:
                # allow per file config
                file_ext = f

            # store the file association
            type_map.setdefault(file_ext, []).append(f)

        merge_results = []
        for ext in type_map.keys():
            merge_object = getMergerObject(ext)  # returns an instance
            if merge_object is None:
                logger.error('Extension %s not recognized and so the merge will fail. '
                             'Check the [Mergers] section of your .gangarc file.', ext)
                return self.failure
            else:
                logger.debug('Extension %s matched and using appropriate object: %s' % (str(ext), str(merge_object)))
            merge_object.files = type_map[ext]
            merge_result = merge_object.merge(jobs, outputdir, ignorefailed, overwrite)
            merge_results.append(merge_result)

        return not False in merge_results

