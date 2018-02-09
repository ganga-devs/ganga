"""
Configuration options for the remote autonomous testing
of new releases of Ganga
"""

import GangaCore.Utility.Config

config=GangaCore.Utility.Config.makeConfig('TestRobot','Parameters for the autonomous independent testing of Ganga')

config.addOption('ReleasePath','http://cern/ch/ganga/download/','Default URL to check for Ganga releases')
config.addOption('InstallPath','','default install path for Ganga scratch testing')
config.addOption('JobDir','','dir to put job data in')
config.addOption('PluginsRequired','GangaTest','Add additional plugins if non-vanilla tests are required')
config.addOption('TestPairs',[['Ganga/old_test', 'Local', 'local'], ['GangaLHCb/old_test', 'Local', 'local']],'These are the tests to perform, the backend, and the configuration')
config.addOption('ReleaseNumber','','Blank default release number - to be set in session')
config.addOption('JobTimeOut',1800,'time out for the pre-release tests in SECONDS')
config.addOption('VersionTime','None','Time of upload of last pre-release tested')
config.addOption('VersionNumber','None','Last Version tested')
config.addOption('SleepTime','600','Time to wait between checking of new releases')
config.addOption('EmailOnStartup','False','If you require the autonomous process to email you on startup following a fail')
config.addOption('LastCheckedTime','0','Last Checked Time of the VERSIONS.txt')
config.addOption('PublishPath','~/public_html/testrobot','Directory where test results should be made available as a .tgz file')
config.addOption('Site','Imperial','Name of site where test is carried out')
