"""
Ganga Runtime subsystem: initialization, configuration and creation of runtime objects.
"""


import sys

from .bootstrap import GangaProgram


def setupGanga(argv=sys.argv, interactive=True):
    # Process options given at command line and in configuration file(s)
    # Perform environment setup and bootstrap
    import GangaCore.Runtime
    GangaCore.Runtime._prog = GangaProgram(argv=argv)
    GangaCore.Runtime._prog.parseOptions()
    GangaCore.Runtime._prog.configure()
    GangaCore.Runtime._prog.initEnvironment()
    GangaCore.Runtime._prog.bootstrap(GangaCore.Runtime._prog.interactive)
    GangaCore.Runtime._prog.new_user_wizard(interactive)

