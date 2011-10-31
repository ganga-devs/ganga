from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
class GaudiAppConfig:
    '''Used to pass extra info from Gaudi apps to the RT-handler.'''
    _name = "GaudiAppConfig"
    
    def __init__(self):
        self.inputsandbox = []
        self.outputsandbox = []
        self.inputdata = LHCbDataset()
        self.outputdata = OutputData()
        
