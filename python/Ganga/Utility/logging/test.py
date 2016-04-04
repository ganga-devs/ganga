from __future__ import print_function
from __future__ import absolute_import

import logging

from Ganga.Utility.logging import getLogger, _formats


def test_logging():
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
