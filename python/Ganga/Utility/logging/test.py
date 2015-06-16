from __future__ import print_function

import test
import Ganga.Utility.logging
Ganga.Utility.logging.config['Ganga.Utility.logging'] = 'DEBUG'

logger = Ganga.Utility.logging.getLogger()

import sys
print(sys.path)

print(logger.name)

logger.info('info test')
logger.error('error test')
logger.warning('warning test')

# Ganga.Utility.logging.bootstrap()
