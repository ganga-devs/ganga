from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler


def addLocalTestSubmitter():
    allHandlers.add('DaVinci', 'TestSubmitter', LHCbGaudiRunTimeHandler)
