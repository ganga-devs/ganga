#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import sys
import imp
import tempfile
import pickle
import subprocess
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IMerger import IMerger
from Ganga.Utility.Plugin import allPlugins
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObject

xml_schema = {}
xml_summary = {}

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class GaudiXMLSummary(GangaObject):

    '''Class for handling XMLSummary objects.

    Obtaining the XML summary info requires the full path to the XML file
    and the location of the XMLSummaryBase version to use
    (store in env_var).

    One can instead simply initialize from a job object (which had a Gaudi
    application) as follows:

    gxml = GaudiXMLSummary(job,"summary.xml")
    sum = gxml.summary()

    N.b. the 2nd argument to the constructor defaults to "summary.xml".
    '''
    schema = {}
    schema['file'] = SimpleItem(defvalue='', typelist=['str'])
    schema['env_var'] = SimpleItem(defvalue='', typelist=['str'], hidden=0)
    _schema = Schema(Version(1, 1), schema)
    _category = ''
    _name = "GaudiXMLSummary"
    _exportmethods = ['create', 'summary']

    def __init__(self, job, file):
        super(GaudiXMLSummary, self).__init__()
        self.data = None
        self.create(job, file)

    def _attribute_filter__set__(self, n, v):
        if n == 'env_var':
            self.data = None
            return os.path.expanduser(v)
        else:
            return v

    def summary(self):
        '''Returns the summary object.'''
        try:
            data = self.data
            if not data:
                self._init()
        except:
            self._init()
        return self.data

    def _init(self):
        global xml_summary
        global xml_schema

        if not self.env_var:
            raise GangaException('XMLSummary env not set!')
        if not self.file:
            raise GangaException('File not specified!')
        if not os.path.exists(self.file):
            raise GangaException('%s does not exist!' % self.file)

        p = self._xmlPath()
        v = self.env_var
        if v not in xml_schema:
            if 'schema' in sys.modules:
                del sys.modules['schema']
            xml_schema[v] = imp.load_source('schema', p + '/schema.py')
            if 'summary' in sys.modules:
                del sys.modules['summary']
            xml_summary[v] = imp.load_source('summary', p + '/summary.py')
            xml_summary[v].__schema__ = xml_schema[v]

        sum = xml_summary[v].Summary(self._xmlSchema())
        sum.parse(self.file)
        self.data = sum

        if 'schema' in sys.modules:
            del sys.modules['schema']
        if 'summary' in sys.modules:
            del sys.modules['summary']
        return

    def create(self, job, file='summary.xml'):
        '''Sets up the GaudiXMLSummary object from a Ganga job.'''
        self._setEnvVar(job.application)
        self._setFile(job, file)
        self._init()

    def _setEnvVar(self, app):
        gaudi_env = app.getenv()
        self.env_var = gaudi_env['XMLSUMMARYBASEROOT']

    def _setFile(self, job, file):
        self.file = job.outputdir + file

    def _xmlPath(self):
        return self.env_var + '/python/XMLSummaryBase'

    def _xmlSchema(self):
        return self.env_var + '/xml/XMLSummary.xsd'

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class GaudiXMLSummaryMerger(IMerger):

    '''Merger for XML summary files.'''
    _category = 'postprocessors'
    _exportmethods = ['merge']
    _name = 'GaudiXMLSummaryMerger'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['env_var'] = SimpleItem(defvalue='', typelist=['str'])

    def mergefiles(self, file_list, output_file, jobs):
        from Ganga.GPIDev.Lib.Job import Job
        from Ganga.GPIDev.Base.Proxy import isType
        gaudi_env = {}
        if isType(jobs, Job):
            gaudi_env = jobs.application.getenv()
        elif len(jobs) > 0:
            gaudi_env = jobs[0].application.getenv()
        self.env_var = gaudi_env['XMLSUMMARYBASEROOT']
        # needed as exportmethods doesn't seem to cope with inheritance
        if not self.env_var:
            raise GangaException('XMLSummary env not set!')
        script_name = tempfile.mktemp('.py')
        script = open(script_name, 'w')
        dummy = GaudiXMLSummary()
        dummy.env_var = self.env_var

        # write py script
        script.write('import sys\n')
        script.write('sys.path.append("%s") \n' % dummy._xmlPath())
        script.write('import summary \n')
        script.write('sum = summary.Merge(%s,"%s") \n'
                     % (str(file_list), dummy._xmlSchema()))
        script.write('sum.write("%s") \n' % output_file)
        script.close()

        # run it
        proc = subprocess.Popen(['python', script_name])
        proc.wait()
        rc = proc.poll()
        if rc != 0:
            msg = 'Failed to merge XML summary file!'
            raise GangaException(msg)

        if not os.path.exists(output_file):
            raise GangaException('Failed to merge XML summary file!')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Add it to the list of plug-ins
# allPlugins.add(_GaudiXMLSummaryMergeTool,'merge_tools',
#               '_GaudiXMLSummaryMergeTool')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
