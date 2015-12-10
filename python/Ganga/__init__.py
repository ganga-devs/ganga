def getEnvironment(config = None):
    return {}

# ------------------------------------------------
# Setup all configs for this module
from Ganga.Utility.Config import makeConfig

gpiconfig = makeConfig('GPI_Semantics',
                       'Customization of GPI behaviour. These options may affect the semantics of the Ganga GPI interface (what may result in a different behaviour of scripts and commands).')
gpiconfig.addOption('job_submit_keep_going', False,
                    'Keep on submitting as many subjobs as possible. Option to j.submit(), see Job class for details')
gpiconfig.addOption('job_submit_keep_on_fail', False,
                    'Do not revert job to new status even if submission failed. Option to j.submit(), see Job class for details')

logconfig = makeConfig("Logging", """control the messages printed by Ganga
The settings are applied hierarchically to the loggers. Ganga is the name of the top-level logger which
applies by default to all Ganga.* packages unless overriden in sub-packages.
You may define new loggers in this section.
The log level may be one of: CRITICAL ERROR WARNING INFO DEBUG
""", is_open=True)

# FIXME: Ganga WARNING should be turned into INFO level when the messages
# are reviewed in all the code
logconfig.addOption('Ganga', "INFO", "top-level logger")
logconfig.addOption('Ganga.Runtime.bootstrap', "INFO", 'FIXME')
logconfig.addOption('Ganga.GPIDev', "INFO", "logger of Ganga.GPIDev.* packages")
logconfig.addOption('Ganga.Utility.logging', "WARNING", "logger of the Ganga logging package itself (use with care!)")
logconfig.addOption('_format', "NORMAL", "format of logging messages: TERSE,NORMAL,VERBOSE,DEBUG")
logconfig.addOption('_colour', True, "enable ASCII colour formatting of messages e.g. errors in red")
logconfig.addOption('_logfile', "~/.ganga.log", "location of the logfile")
logconfig.addOption('_logfile_size', 100000,
                 "the size of the logfile (in bytes), the rotating log will never exceed this file size")  # 100 K
logconfig.addOption('_interactive_cache', True,
                 'if True then the cache used for interactive sessions, False disables caching')
logconfig.addOption('_customFormat', "", "custom formatting string for Ganga logging\n e.g. '%(name)-35s: %(levelname)-8s %(message)s'")


