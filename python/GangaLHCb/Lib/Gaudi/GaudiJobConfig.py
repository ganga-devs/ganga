from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData

class GaudiJobConfig(StandardJobConfig):

    def __init__(self,exe='',inputbox=[],args=[],outputbox=[],env={},outputdata=[]):
        self.exe=exe
        self.inputbox=inputbox[:]
        self.args=args
        self.outputbox=outputbox[:]
        self.env=env
        self.outputdata=outputdata[:]
    
        self.__all_inputbox = []
        self.__args_strings = []
        self.__exe_string = ""
        self.__sandbox_check = {}

        self.processValues()
