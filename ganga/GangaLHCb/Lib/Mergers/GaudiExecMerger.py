##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Merger.py,v 1.5 2009-03-18 10:46:01 wreece Exp $
##########################################################################

from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException
from GangaCore.GPIDev.Adapters.IMerger import IMerger
from GangaCore.GPIDev.Schema import FileItem, SimpleItem
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.GPIDev.Lib.File.File import File
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.Utility.Config import ConfigError, getConfig
from GangaCore.Utility.Plugin import allPlugins
from GangaCore.Utility.logging import getLogger
import tarfile
import subprocess
import tempfile
import os
import shutil
import string
import copy

logger = getLogger()

class GaudiExecMerger(IMerger):

    """Merger class for GaudiExec jobs

    GaudiExec merger will use the version of hadd in your GaudiExec application
    to merge your files for maximum compatibility.

    Usage:

    gm = GaudiExecMerger()
    gm.files = ['hist.root','trees.root']
    gm.overwrite = True #False by default
    gm.ignorefailed = True #False by default
    gm.args = '-f2' #pass arguments to hadd

    # will produce the specified files
    j = Job() 
    j.outputfiles = ['hist.root','trees.root']
    j.splitter = SomeSplitter()
    j.postprocessors = gm
    j.submit()

    The merge object will be used to merge the output of
    each subjob into j.outputdir. This will be run when
    the job completes. If the ignorefailed flag has been set
    then the merge will also be run as the job enters the
    killed or failed states.

    The above merger object can also be used independently
    to merge a list of jobs or the subjobs of an single job.

    #gm defined above
    gm.merge(j, outputdir = '~/merge_dir')
    gm.merge([.. list of jobs ...], '~/merge_dir', ignorefailed = True, overwrite = False)

    If ignorefailed or overwrite are set then they override the
    values set on the merge object.

    A summary of all the files merged will be created for each entry in files.
    This will be created when the merge of those files completes
    successfully. The name of this is the same as the output file, with the
    '.merge_summary' extension appended and will be placed in the same directory
    as the merge results.

    If outputdir is not specified, the default location specfied
    in the [Mergers] section of the .gangarc file will be used.

    The way this works is to copy the compressed cmake input sandbox from the sharedir to your tmp area,
    untar it and execute ./run hadd in order to make use of the correct environment.

    """

    _category = 'postprocessor'
    _name = 'GaudiExecMerger'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['args'] = SimpleItem(defvalue=None, doc='Arguments to be passed to hadd.',
                                          typelist=[str, None])

    def merge(self, jobs, outputdir=None, ignorefailed=None, overwrite=None):

        if ignorefailed is None:
            ignorefailed = self.ignorefailed

        if overwrite is None:
            overwrite = self.overwrite

        from GangaCore.GPIDev.Lib.Job import Job

        if not outputdir:
            outputdir = getDefaultMergeDir()
        else:
            if isType(outputdir, Job):
                # use info from job
                outputdir = outputdir.outputdir
            else:
                outputdir = os.path.expanduser(outputdir)

        files = {}

        if isType(jobs, Job):
            if outputdir is None:
                outputdir = jobs.outputdir
            return self.merge(jobs.subjobs, outputdir=outputdir, ignorefailed=ignorefailed, overwrite=overwrite)

        if not len(jobs):
            logger.warning('The jobslice given was empty. The merge will not continue.')
            return self.success
        masterjob = jobs[0].master

        for j in jobs:
            # first check that the job is ok
            if j.status != 'completed':
                # check if we can keep going
                if j.status == 'failed' or j.status == 'killed':
                    if ignorefailed:
                        logger.warning('Job %s has status %s and is being ignored.', j.fqid, j.status)
                        continue
                    else:
                        raise PostProcessException('Job %s has status %s and so the merge can not continue. '
                                                   'This can be overridden with the ignorefailed flag.' % (j.fqid, j.status))
                else:
                    raise PostProcessException("Job %s is in an unsupported status %s and so the merge can not continue. '\
                    'Supported statuses are 'completed', 'failed' or 'killed' (if the ignorefailed flag is set)." % (j.fqid, j.status))

            if len(j.subjobs):
                sub_result = self.merge(
                    j.subjobs, outputdir=j.outputdir, ignorefailed=ignorefailed, overwrite=overwrite)
                if (sub_result == self.failure) and not ignorefailed:
                    raise PostProcessException('The merge of Job %s failed and so the merge can not continue. '
                                               'This can be overridden with the ignorefailed flag.' % j.fqid)

            import glob
            for f in self.files:

                for matchedFile in glob.glob(os.path.join(j.outputdir, f)):
                    relMatchedFile = ''
                    try:
                        relMatchedFile = os.path.relpath(
                            matchedFile, j.outputdir)
                    except Exception as err:
                        logger.debug("Err: %s" % err)
                        GangaCore.Utility.logging.log_unknown_exception()
                        relMatchedFile = relpath(matchedFile, j.outputdir)
                    if relMatchedFile in files:
                        files[relMatchedFile].append(matchedFile)
                    else:
                        files[relMatchedFile] = [matchedFile]

                if not len(glob.glob(os.path.join(j.outputdir, f))):
                    if ignorefailed:
                        logger.warning('The file pattern %s in Job %s was not found. The file will be ignored.', f, j.fqid)
                        continue
                    else:
                        raise PostProcessException('The file pattern %s in Job %s was not found and so the merge can not continue. '
                                                   'This can be overridden with the ignorefailed flag.' % (f, j.fqid))
                # files[f].extend(matchedFiles)

        for k in files.keys():
            # make sure we are not going to over write anything
            outputfile = os.path.join(outputdir, k)
            if os.path.exists(outputfile) and not overwrite:
                raise PostProcessException('The merge process can not continue as it will result in over writing. '
                                           'Either move the file %s or set the overwrite flag to True.' % outputfile)

            # make the directory if it does not exist
            if not os.path.exists(outputdir):
                os.makedirs(outputdir)

            # recreate structure from output sandbox
            outputfile_dirname = os.path.dirname(outputfile)
            if outputfile_dirname != outputdir:
                if not os.path.exists(outputfile_dirname):
                    os.mkdir(outputfile_dirname)

            # check that we are merging some files
            if not files[k]:
                logger.warning('Attempting to merge with no files. Request will be ignored.')
                continue

            # check outputfile != inputfile
            for f in files[k]:
                if f == outputfile:
                    raise PostProcessException(
                        'Output file %s equals input file %s. The merge will fail.' % (outputfile, f))
            # merge the lists of files with a merge tool into outputfile
            msg = None
            try:
                self.mergefiles(masterjob, files[k], outputfile)

                # create a log file of the merge
                # we only get to here if the merge_tool ran ok
                log_file = '%s.merge_summary' % outputfile
                with open(log_file, 'w') as log:
                    log.write('# -- List of files merged -- #\n')
                    for f in files[k]:
                        log.write('%s\n' % f)
                    log.write('# -- End of list -- #\n')

            except PostProcessException as e:
                msg = str(e)

                # store the error msg
                log_file = '%s.merge_summary' % outputfile
                with open(log_file, 'w') as log:
                    log.write('# -- Error in Merge -- #\n')
                    log.write('\t%s\n' % msg)
                raise e

        return self.success


    def mergefiles(self, masterjob, file_list, output_file):

        #First grab the job object etc.
        j = masterjob
        sharedir = j.application.is_prepared.path()
        tmp_dir = tempfile.gettempdir()
        #Extract the run script
        tar = tarfile.open(os.path.join(sharedir, "cmake-input-sandbox.tgz"), "r:gz")
        tar.extractall(tmp_dir)
        #Run the hadd command from the application environment.
        merge_cmd = os.path.join(tmp_dir, 'run')
        default_arguments = '-f'
        merge_cmd += ' hadd '
        if self.args:  # pass any args on
            merge_cmd += ' %s ' % self.args

        # don't add a -f unless needed
        if not default_arguments in merge_cmd:
            merge_cmd += ' %s ' % default_arguments

        # add the list of files, output file first
        arg_list = [output_file]
        arg_list.extend(file_list)
        merge_cmd += string.join(arg_list, ' ')

        rc, out = subprocess.getstatusoutput(merge_cmd)

        try:
            #Clean up - first make a list of everything in the tarfile
            tarlist = tar.getnames()
            #Pop the run script and find the common prefix of the application folder
            tarlist.pop(tarlist.index('run'))
            folderToRemove = os.path.commonprefix(tarlist)
            #Now remove the files
            os.remove(os.path.join(tmp_dir, 'run'))
            shutil.rmtree(os.path.join(tmp_dir, folderToRemove))
        except OSError:
            logger.error('Failed to remove temporary files from merging at %s' % tmp_dir)


        log_file = '%s.hadd_output' % output_file
        with open(log_file, 'w') as log:
            log.write('# -- Hadd output -- #\n')
            log.write('%s\n' % out)

        if rc:
            logger.error(out)
            raise PostProcessException(
                'The ROOT merge failed to complete. The command used was %s.' % merge_cmd)
