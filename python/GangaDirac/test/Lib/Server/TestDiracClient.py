from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.Server.DiracClient             import DiracClient, SocketAddress
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
#from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI                                     import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest#, tempfile, os

class TestDiracClient(GangaGPITestCase):
    def setUp(self):
        pass

    def test___init__(self):
        import threading
        def shutdown_server(this):
            pass
        setattr(DiracClient,'shutdown_server',shutdown_server)
        d=DiracClient()
        self.assertEqual(d._DiracClient__socket_addr,
                         SocketAddress(address = 'localhost', port = 49000))#,'not enough threads')
        
        
