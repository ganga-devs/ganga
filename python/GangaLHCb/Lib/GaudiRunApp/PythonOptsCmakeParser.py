import os.path
import tempfile
from Ganga.GPIDev.Lib.File import LocalFile
import Ganga.Utility.logging
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.files import expandfilename
logger = Ganga.Utility.logging.getLogger()

## Due to a bug in Gaudi at some point we need this equivalence here: see #204
DataObjectDescriptorCollection = str

class PythonOptsCmakeParser(object):

    """ Parses job options file(s) w/ gaudirun.py to extract user's files
    Uses gaudirun.py to parse the job options file to allow for easy extraction
    of inputdata, outputdata and output files."""

    def __init__(self, optsfiles, app):
        self.optsfiles = optsfiles
        self.app = app
        self.opts_dict, self.opts_pkl_str = self._get_opts_dict_and_pkl_string()

    def _get_opts_dict_and_pkl_string(self):
        '''Parse the options using gaudirun.py and create a dictionary of the
        configuration and pickle the options. The app handler will make a copy
        of the .pkl file for each job.'''

        logger.info("Started parsing input Data file")

        tmp_pkl = tempfile.NamedTemporaryFile(suffix='.pkl')
        tmp_py = tempfile.NamedTemporaryFile(suffix='.py')
        py_opts = tempfile.NamedTemporaryFile(suffix='.py')
        py_opts.write(self._join_opts_files())
        py_opts.flush()

        gaudirun = 'gaudirun.py -n -v -o %s %s' % (tmp_py.name, py_opts.name)
        opts_str = ''
        err_msg = ''
        options = {}

        rc, stdout, m = self.app.exec_cmd(gaudirun)

        if stdout.find('Gaudi.py') >= 0:
            msg = 'The version of gaudirun.py required for your application is not supported.'
            raise ValueError(None, msg)

        elif stdout.find('no such option: -o') >= 0:
            gaudirun = 'gaudirun.py -n -v -p %s %s' % (tmp_pkl.name, py_opts.name)
            rc, stdout, m = self.app.exec_cmd(gaudirun)
            rc = 0

            if stdout and rc == 0:
                opts_str = stdout
                err_msg = 'Please check %s -v %s' % (cmdbase, py_opts.name)
                err_msg += ' returns valid python syntax'

        else:
            cmd = 'gaudirun.py -n -p %s %s' % (tmp_pkl.name, py_opts.name)
            rc, stdout, m = self.app.exec_cmd(cmd)
            if rc == 0 and stdout:
                opts_str = tmp_py.read()
                err_msg = 'Please check gaudirun.py -o file.py produces a valid python file.'

        if stdout and rc == 0:
            try:
                options = eval(opts_str)
            except Exception as err:
                logger.error('Cannot eval() the options file. Exception: %s', err)
                from traceback import print_exc
                logger.error(' ', print_exc())
                raise ApplicationConfigurationError(None, stdout + '###SPLIT###' + m)
            try:
                opts_pkl_string = tmp_pkl.read()
            except IOError as err:
                logger.error('Cannot read() the temporary pickle file: %s', tmp_pkl.name)
                raise err

        if not rc == 0:
            logger.debug('Failed to run: %s', gaudirun)
            raise ApplicationConfigurationError(None, stdout + '###SPLIT###' + m)

        tmp_pkl.close()
        py_opts.close()
        tmp_py.close()

        logger.info("Finished parsing input Data file")

        return (options, opts_pkl_string)

    def get_input_data(self):
        '''Collects the user specified input data that the job will process'''
        data = []
        try:
            opts_input = self.opts_dict['EventSelector']['Input']
            data = [f for f in opts_input]
        except KeyError as err:
            logger.error('No inputdata has been defined in the options file.')
            logger.error("%s" % str(err))

        from Ganga.GPIDev.Base.Filters import allComponentFilters
        file_filter = allComponentFilters['gangafiles']

        all_files = []
        for d in data:
            p1 = d.find('DATAFILE=') + len('DATAFILE=')
            quote = d[p1]
            p2 = d.find(quote, p1 + 1)
            f = d[p1 + 1:p2]
            this_file = file_filter(f, None)
            if this_file is None:
                this_file = LocalFile(name=f)
            all_files.append(this_file)

        ds = LHCbDataset(files=all_files, fromRef=True)
        return ds

    def _join_opts_files(self):
        '''Create a single options file from all supplied options.'''
        joined_py_opts = ''
        for name in self.optsfiles:
            with open(expandfilename(name), 'r') as this_file:
                joined_py_opts += this_file.read()
        return joined_py_opts

