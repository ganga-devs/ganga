from GangaCore.Lib.Executable import RTHandler
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Executable', 'TestSubmitter', RTHandler)
allHandlers.add('Executable', 'DummyRemote', RTHandler)
