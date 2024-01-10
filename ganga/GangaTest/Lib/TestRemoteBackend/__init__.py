
import GangaCore.Utility.Config
from .TestRemoteBackend import DummyRemote

__all__ = ['DummyRemote']

config = GangaCore.Utility.Config.makeConfig(
    'TestDummyRemote', 'A dummy remote backend to emulate non local endpoints such as DIRAC')
config.addOption("SERVER_PORT", 5100, "Port that the dummy server runs on")
config.addOption("SERVER_DEFAULT_DELAY", 0.1, "Artificial delay in seconds")
config.addOption("ENABLE_FINALISATION", True,
                 "Whether or not to simulate job finalisation via dummy output file download.")
config.addOption("FINALISATION_DELAY", 10, "Job finalisation simulation delay in seconds.")
