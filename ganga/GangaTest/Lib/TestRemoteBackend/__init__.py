import GangaCore.Utility.Config
from .TestRemoteBackend import *

config = GangaCore.Utility.Config.makeConfig(
    'TestDummyRemote', 'A dummy remote backend to emulate non local endpoints such as DIRAC')
config.addOption("SERVER_PORT", 5100, "Port that the server runs on")
config.addOption("SERVER_DEFAULT_DELAY", 0.1, "Artificial delay in seconds")

from .server import DummyServer

server = DummyServer()
server.start()
