#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import sys
import imp
import tempfile
import pickle
import subprocess
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IMerger import MergerError
from Ganga.Lib.Mergers.Merger import AbstractMerger, IMergeTool
from Ganga.Utility.Plugin import allPlugins
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObject

xml_schema = {}

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
    schema['file'] = SimpleItem(defvalue='',typelist=['str'])
    schema['env_var'] = SimpleItem(defvalue='',typelist=['str'],hidden=0)
    _schema = Schema(Version(1,1), schema)
    _category = ''
    _name = "GaudiXMLSummary"
    _exportmethods = ['create','summary']

    def __init__(self):
        super(GaudiXMLSummary,self).__init__()
        self.data = None

    def __construct__(self, args):
        from Ganga.GPIDev.Lib.Job.Job import Job
        if (len(args) > 2 ) or (len(args) == 0) or (type(args[0]) is not Job):
            super(GaudiXMLSummary,self).__construct__(args)
        else:
            if len(args) == 1: self.create(args[0])
            else: self.create(args[0],args[1])

    def summary(self):
        '''Returns the summary object.'''
        try:
            data = self.data
            if not data: self._init()
        except:
            self._init()
        return self.data

    def _init(self):
        if not self.env_var:
            raise GangaException('XMLSummary env not set!')
        if not self.file: 
            raise GangaException('File not specified!')
        if not os.path.exists(self.file):
            raise GangaException('%s does not exist!' % self.file)
        
        script_name = tempfile.mktemp('.py')
        script = open(script_name,'w')
        pkl_file = tempfile.mktemp('.pkl')

        # write py script
        script.write('import sys, pickle \n')
        script.write('sys.path.append("%s") \n' % self._xmlPath())
        script.write('import summary \n')
        script.write('sum = summary.Summary("%s") \n' % self._xmlSchema())
        script.write('sum.parse("%s") \n' % self.file)
        script.write('f = open("%s","w") \n' % pkl_file)
        script.write('pickle.dump(sum,f) \n')
        script.write('f.close()\n')
        script.close()
        
        # run it
        proc = subprocess.Popen(['python', script_name])
        proc.wait()
        rc = proc.poll()
        if rc != 0:
            msg = 'Failed to parse XML summary file!'
            raise GangaException(msg)

        # get summary
        schema = imp.load_source('schema',self._xmlPath()+'/schema.py')
        summary = imp.load_source('summary',self._xmlPath()+'/summary.py')
        f = open(pkl_file)
        self.data = pickle.load(f)
        f.close()

    def create(self,job,file='summary.xml'):
        '''Sets up the GaudiXMLSummary object from a Ganga job.'''
        self._setEnvVar(job.application)
        self._setFile(job,file)
        self._init()
        
    def _setEnvVar(self,app):
        gaudi_env = app.getenv()
        self.env_var = gaudi_env['XMLSUMMARYBASEROOT']

    def _setFile(self,job,file):
        self.file = job.outputdir + file

    def _xmlPath(self):        
        return self.env_var + '/python/XMLSummaryBase'

    def _xmlSchema(self):
        return self.env_var + '/xml/XMLSummary.xsd'

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class _GaudiXMLSummaryMergeTool(IMergeTool):
    
    _category = 'merge_tools'
    _hidden = 1
    _name = '_GaudiXMLSummaryMergeTool'
    _schema = IMergeTool._schema.inherit_copy()
    _schema.datadict['env_var'] = SimpleItem(defvalue='',typelist=['str'])
    
    def mergefiles(self, file_list, output_file):
        if not self.env_var:
            raise GangaException('XMLSummary env not set!')
        script_name = tempfile.mktemp('.py')
        script = open(script_name,'w')
        dummy = GaudiXMLSummary()
        dummy.env_var = self.env_var        

        # write py script
        script.write('import sys\n')
        script.write('sys.path.append("%s") \n' % dummy._xmlPath())
        script.write('import summary \n')
        script.write('sum = summary.Merge(%s,"%s") \n' \
                     % (str(file_list),dummy._xmlSchema()))
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


class GaudiXMLSummaryMerger(AbstractMerger):
    '''Merger for XML summary files.'''
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'GaudiXMLSummaryMerger'
    _schema = AbstractMerger._schema.inherit_copy()

    def __init__(self):
        super(GaudiXMLSummaryMerger,self).__init__(_GaudiXMLSummaryMergeTool())

    def merge(self,jobs,outputdir=None,ignorefailed=None,overwrite=None):
        from Ganga.GPIDev.Lib.Job import Job
        gaudi_env = {}
        if isinstance(jobs,GPIProxyObject) and isinstance(jobs._impl,Job):
            gaudi_env = jobs.application.getenv()
        elif len(jobs) > 0:
            gaudi_env = jobs[0].application.getenv()
        self.merge_tool.env_var = gaudi_env['XMLSUMMARYBASEROOT']
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(GaudiXMLSummaryMerger,self).merge(jobs,outputdir,
                                                       ignorefailed,overwrite)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Add it to the list of plug-ins
allPlugins.add(_GaudiXMLSummaryMergeTool,'merge_tools',
               '_GaudiXMLSummaryMergeTool')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
