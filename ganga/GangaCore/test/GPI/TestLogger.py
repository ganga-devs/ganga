import logging
from GangaCore.Utility.logging import getLogger, _formats

# Test if logger works properly
def test_logging(gpi):

    # Assert False if any error is thrown
    try:
        logger = getLogger()

        logger.info('info test')
        logger.error('error test')
        logger.warning('warning test')

        private_logger = logging.getLogger("TESTLOGGER.CHILD.GRANDCHILD")
        formatter = logging.Formatter(_formats['DEBUG'])
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        private_logger.setLevel(logging.DEBUG)
        private_logger.addHandler(console)
        private_logger.critical('hello')
        assert True
    except:
        assert False, "Should not throw any error"

def test_message_caching(gpi):

    import time

    # Assert False if any error is thrown
    try:
        l = getLogger()
        
        def f(name=''):
            for i in range(5):
                l.warning(name + str(i))
                time.sleep(1)

        f('Main:')

        from GangaCore.Core.GangaThread import GangaThread

        class MyThread(GangaThread):

            def run(self):
                f('GangaThread:')

        t = MyThread('GangaThread')
        t.start()
        assert True
    except:
        assert False, "Should not throw any error"
    
