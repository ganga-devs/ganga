#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import copy, os, pickle
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from RTHUtils import *
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique
from GangaGaudi.Lib.RTHandlers.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare
logger = getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbGaudiRunTimeHandler(GaudiRunTimeHandler):
    """This is the application runtime handler class for Gaudi applications 
    using the local, interactive and LSF backends."""
  
    def master_prepare(self,app,appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig,['inputsandbox'])

        # add summary.xml
        outputsandbox += ['summary.xml','__parsedxmlsummary__']

        return StandardJobConfig( inputbox  = unique(inputsandbox),
                                  outputbox = unique(outputsandbox) )


    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)

        job = app.getJobObject()
        
        ## Cant wait to get rid of this when people no-longer specify
        ## inputdata in options file
        #######################################################################
        new_job = copy.deepcopy(job)
        ## splitters ensure that subjobs pick up inputdata from job over that in
        ## optsfiles but need to take sare of unsplit jobs
        if not job.master:
            share_path = os.path.join(get_share_path(app),
                                      'inputdata',
                                      'options_data.pkl')

            if not job.inputdata:
                if os.path.exists(share_path):
                    f=open(share_path,'r+b')
                    new_job.inputdata = pickle.load(f)
                    f.close()
        
        ########################################################################

        ## Cant wait to get rid of this when people no-longer specify
        ## outputsandbox or outputdata in options file
        #######################################################################
        share_path = os.path.join(get_share_path(app),
                                  'output',
                                  'options_parser.pkl')

        if os.path.exists(share_path):
#        if not os.path.exists(share_path):
           # raise GangaException('could not find the parser')
           f=open(share_path,'r+b')
           parser = pickle.load(f)
           f.close()

           outbox, outdata = parser.get_output(job)
           if job.outputdata:
               new_job.outputdata.files = unique(job.outputdata.files[:] + outdata[:])
           else:
               new_job.outputdata = OutputData(files=unique(outdata[:]))
           outputsandbox  = unique(outputsandbox  + outbox[:]) 
        #######################################################################

        data = new_job.inputdata
        data_str=''
        if data:
            data_str = data.optionsString()
            if data.hasLFNs():
                inputsandbox.append(FileBuffer('catalog.xml',data.getCatalog()))           
                cat_opts='\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
                data_str += cat_opts

        if hasattr(job,'_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data.py',data_str))


        script = create_runscript(new_job.application)
        
        return StandardJobConfig( FileBuffer('gaudi-script.py', script, executable=1),
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox) )


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
