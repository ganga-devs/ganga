"""Ganga Robot is a tool for running a user-defined list of actions within the
context of a Ganga session.

The Framework package contains the driver code, see Framework.Driver, and the
action interface, see Framework.Action, which should be implemented by any
action to be executed by the driver.

The Lib package contains some implementations of the action interface, including
base abstract implementations, see Lib.Base, which provide infrastructure to
easily add custom submit/finish/extract/report actions, as well as some generic
submit/finish/extract/report actions, see Lib.Core, which are used for the
default configuration.

The scripts folder contains a robot script for launching the robot.

The exports folder contains files of exported jobs which can be specified for
submission via the CoreSubmitter_Patterns configuration option or as arguments
to the robot script.

For robot script usage:
 ganga --config-path=GangaRobot/ROBOT.INI robot help

"""

import GangaCore.Utility.Config

def _initconfig():
    config=GangaCore.Utility.Config.makeConfig('Robot','Parameters for the Robot to run repetitive tests',is_open=True)
    
    config.addOption('Driver_Run',['submit', 20, 'finish', 'extract', 'report'],
                     'List of action names and sleep periods (seconds)')
    config.addOption('Driver_Repeat',False,
                     'boolean indicating if the run should repeat indefinitely')
    
    config.addOption('Driver_Action_submit',
                     'GangaRobot.Lib.Core.CoreSubmitter.CoreSubmitter',
                     'GangaRobot.Framework.Action.IAction class names for submit action in Driver_Run')
    config.addOption('Driver_Action_finish',
                     'GangaRobot.Lib.Core.CoreFinisher.CoreFinisher',
                     'GangaRobot.Framework.Action.IAction class names for finish action in Driver_Run')
    config.addOption('Driver_Action_extract',
                     'GangaRobot.Lib.Core.CoreExtractor.CoreExtractor',
                     'GangaRobot.Framework.Action.IAction class names for extract action in Driver_Run')
    config.addOption('Driver_Action_report',
                     'GangaRobot.Lib.Core.CoreReporter.CoreReporter',
                     'GangaRobot.Framework.Action.IAction class names for report action in Driver_Run')

    config.addOption('BaseFinisher_Timeout',3600,
                     'Timeout (seconds) for waiting for jobs to finish')
    config.addOption('BaseExtractor_XmlFile',
                     '~/gangadir_robot/robot/extract/${runid}.xml',
                     'Filename for XML extract data, ${runid} is replaced by current run id')
    config.addOption('BaseReporter_TextFile',
                     '~/gangadir_robot/robot/report/${runid}.txt',
                     'Filename for TEXT report data, ${runid} is replaced by current run id')
    config.addOption('BaseReporter_HtmlFile',
                     '~/gangadir_robot/robot/report/${runid}.html',
                     'Filename for HTML report data, ${runid} is replaced by current run id')

    config.addOption('CoreSubmitter_Patterns',
                     ['GangaRobot/exports/local-echo-jobs.txt'],
                     'Exported job file patterns. Can contain Unix-style glob pathname patterns (relative patterns are evaluated against the current working directory and the Ganga python root, i.e. ganga/python/)')
    config.addOption('CoreReporter_ExtractUrl','',
                     'URL for links to extract data (if empty no link is created)')
    
    config.addOption('FileEmailer_Host','localhost:25','SMTP host and port')
    config.addOption('FileEmailer_Type','html',
                     'Email type \'html\' (i.e. html + plain) or \'text\' (i.e. plain)')
    config.addOption('FileEmailer_From','',
                     'From address for email, e.g. sender@domain.org')
    config.addOption('FileEmailer_Recipients','',
                     'Recepient list for email, e.g. recipient1@domain.org, recipient2@domain.org (if empty no email is sent)')
    config.addOption('FileEmailer_Subject','Ganga Robot: ${runid}.',
                     'Subject for email, ${runid} is replaced by current run id')
    config.addOption('FileEmailer_TextFile','',
                     'Filename for TEXT email body, ${runid} is replaced by current run id')
    config.addOption('FileEmailer_HtmlFile','',
                     'Filename for HTML email body, ${runid} is replaced by current run id')
    config.addOption('ExceptionBehaviour','Fatal','Changes behaviour of robot when exception is thrown. Options are Continue, Break, and Fatal (Default)')
    config.addOption('ThreadedSubmitter_numThreads',
                     10,
                     'Number of concurrent threads to use when using the ThreadedSubmitter')

def loadPlugins(config={}):
    from GangaCore.Utility.Config.Config import _after_bootstrap

    if not _after_bootstrap:

        _initconfig()
# SVN 
