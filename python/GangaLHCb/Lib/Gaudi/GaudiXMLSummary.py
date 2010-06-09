#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import sys
import imp
import tempfile
import pickle
import subprocess
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject

xml_schema = {}

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiXMLSummary(GangaObject):
    schema = {}
    schema['file'] = SimpleItem(defvalue='',typelist=['str'])
    schema['env_var'] = SimpleItem(defvalue='',typelist=['str'],hidden=1)
    _schema = Schema(Version(1,1), schema)
    _category = ''
    _name = "GaudiXMLSummary"
    _exportmethods = ['create','summary']

    def __init__(self):
        super(GaudiXMLSummary,self).__init__()
        self.data = None

    def summary(self):
        try:
            data = self.data
            if not data: self._init()
        except:
            self._init()
        return self.data

    def _init(self):
        if not self.env_var and self.file: return
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

    def create(self,job):
        self._setEnvVar(job.application)
        self._setFile(job)
        self._init()
        
    def _setEnvVar(self,app):
        gaudi_env = app.getenv()
        self.env_var = gaudi_env['XMLSUMMARYBASEROOT']

    def _setFile(self,job):
        self.file = job.outputdir + 'summary.xml' 

    def _xmlPath(self):        
        return self.env_var + '/python/XMLSummaryBase'

    def _xmlSchema(self):
        return self.env_var + '/xml/XMLSummary.xsd'

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
