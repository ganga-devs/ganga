"""
Ganga Runtime subsystem: initialization, configuration and creation of runtime objects.
"""
from __future__ import absolute_import

import sys

from .bootstrap import GangaProgram


def setupGanga(argv=sys.argv, interactive=True):
    # Process options given at command line and in configuration file(s)
    # Perform environment setup and bootstrap
    import Ganga.Runtime
    Ganga.Runtime._prog = GangaProgram(argv=argv)
    Ganga.Runtime._prog.parseOptions()
    Ganga.Runtime._prog.configure()
    Ganga.Runtime._prog.initEnvironment()
    Ganga.Runtime._prog.bootstrap(Ganga.Runtime._prog.interactive)
    Ganga.Runtime._prog.new_user_wizard(interactive)

