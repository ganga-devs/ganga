#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import Ganga.Utility.Config 
from RTHUtils import *
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
#from GangaLHCb.Lib.Applications.GaudiJobConfig import GaudiJobConfig
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Utility.util import unique
from Ganga.Core import TypeMismatchError
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from GangaGaudi.Lib.RTHandlers.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import sharedir_handler
import pickle
logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbGaudiRunTimeHandler(GaudiRunTimeHandler):
    """This is the application runtime handler class for Gaudi applications 
    using the local, interactive and LSF backends."""
  
    def _additional_master_prepare(self,
                                   app,
                                   appmasterconfig,
                                   inputsandbox,
                                   outputsandbox):

        job=app.getJobObject()

        share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  app.is_prepared.name,
                                  'output',
                                  'options_parser.pkl')

        outdata=[]
        if os.path.exists(share_path):
            # if not os.path.exists(share_path):
            #     raise GangaException('could not find the parser')
            f=open(share_path,'r+b')
            parser = pickle.load(f)
            f.close()
           


            outbox, outdata = parser.get_output(job)
            outputsandbox += outbox[:]

        if job.outputdata: outdata += job.outputdata.files

        # add summary.xml
        outputsandbox += ['summary.xml','__parsedxmlsummary__']

        r = StandardJobConfig(inputbox   = unique(inputsandbox ),
                              outputbox  = unique(outputsandbox) )

        r.outputdata = unique(outdata)
        
        return r          


    def _additional_prepare(self,
                            app,
                            appsubconfig,
                            appmasterconfig,
                            jobmasterconfig,
                            inputsandbox,
                            outputsandbox):
        
        job = app.getJobObject()
        

        indata = job.inputdata
        ## splitters ensure that subjobs pick up inputdata from job over that in optsfiles
        ## but need to take sare of unsplit jobs
        if not job.master:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      app.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')
            if not indata:
                if os.path.exists(share_path):
                    f=open(share_path,'r+b')
                    indata = pickle.load(f)
                    f.close()
           
            
        data_str=''
        if indata:
            data_str = indata.optionsString()
            if indata.hasLFNs():
                inputsandbox.append(FileBuffer('catalog.xml',indata.getCatalog()))           
                cat_opts='\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
                data_str += cat_opts

        if hasattr(job,'_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data.py',data_str))

        outputdata=OutputData()
        if jobmasterconfig: outputdata.files += jobmasterconfig.outputdata

        script = create_runscript(app,outputdata,job)

        return StandardJobConfig( FileBuffer('gaudiscript.py', script, executable=1),
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox) )


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
