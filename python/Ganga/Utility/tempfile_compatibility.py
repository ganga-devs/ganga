
# tempfile - python2.3 compatibility module 

# provides simple implementation for mkdtemp

import tempfile
from tempfile import *

if not hasattr(tempfile,'mkdtemp'):
    def mkdtemp(suffix='',prefix='',dir=''):
        if prefix or dir:
            import Ganga.Utility.logging
            logger = Ganga.Utility.logging.getLogger()
            logger.warning('tempfile.mkdtemp(): prefix and dir arguments ignored in Python 2.3 compatibility mode')
            
        tempdir = mktemp(suffix)
        try:
            import os
            os.mkdir(tempdir)
            # probably we should chmod to 700
        except IOError as x:
            raise
        return tempdir
