from __future__ import print_function
from __future__ import absolute_import

import Ganga.Utility.logging as logging


def test_logging():
    logger = logging.getLogger()

    logger.info('info test')
    logger.error('error test')
    logger.warning('warning test')

    private_logger = logging._get_logging().getLogger("TESTLOGGER.CHILD.GRANDCHILD")
    formatter = logging._get_logging().Formatter(logging._formats['DEBUG'])
    console = logging._get_logging().StreamHandler()
    console.setFormatter(formatter)
    private_logger.setLevel(logging._get_logging().DEBUG)
    private_logger.addHandler(console)
    private_logger.critical('hello')
