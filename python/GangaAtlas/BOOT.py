# Example startup script for loading modules allowing Ganga submission
# of ADA jobs to DIAL backend

import os, sys
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

print >>sys.stderr
print >>sys.stderr, 'ATLAS Distributed Analysis Support is provided by the "Distributed Analysis Help" HyperNews forum. You can find the forum at '
print >>sys.stderr, '    https://hypernews.cern.ch/HyperNews/Atlas/get/distAnalysisHelp.html'
print >>sys.stderr, 'or you can send an email to hn-atlas-dist-analysis-help@cern.ch'
print >>sys.stderr

try:
    from IPython.iplib import InteractiveShell

    def magic_cmtsetup(self,args=''):
        '''Setup CMT environment.'''

        # determine the CMTHOME path
        from Ganga.Utility.Config import getConfig, ConfigError
        config = getConfig('Athena')
        cmthome = os.path.join(os.environ['HOME'], 'cmthome')
        try:
            cmthome = config['CMTHOME']
        except ConfigError:
            pass

        _do_setup(os.path.join(cmthome, 'setup.sh'), args.split())

        logger.info('CMTCONFIG = %s',os.environ['CMTCONFIG'])
        for path in os.environ['CMTPATH'].split(':'):
            logger.info('CMTPATH = %s',path)

    def magic_setup(self,args=''):
        '''Setup a CMT package'''

        import re

        _do_setup('setup.sh',args.split())

        output = os.popen('cmt -quiet show macro package').read()
        match = re.search('package=\'(.+)\'',output)
        if match:
           package = match.group(1)
           if not package == 'cmt_standalone':
               logger.info('Package %s has been configured.',package)
           else:
               logger.warning('No package directory')
        else:
           logger.error('Problem parsing cmt output')


    def magic_subjobs(self,args=''):
        '''Print status of subjobs'''

        for arg in args.split():
            try:
               jobnr = int(arg)
            except ValueError:
               logger.warning('Invalid job id %s',arg)
               continue

            logger.warning('The subjobs magic command is not supported anymore in Ganga 5')
            logger.warning('Please use instead: jobs(%s).subjobs', jobnr) 

    def magic_fixpython(self,args=''):
        '''Fix python conflict'''

        # detect the python base dir.
        _pybins = os.popen('which python').readlines()
        _pybase = sys.prefix

        if _pybins:
           _pybase = _pybins[0].split('/bin/python')[0]

        print _pybase

        _pypaths = os.environ['PYTHONPATH'].split(':')

        _new_pypaths = [] 

        # detect and remove the default python library path from PYTHONPATH
        for p in _pypaths:
            if p.find('%s/lib/python' % _pybase) >= 0:
                logger.warning('removing %s from PYTHONPATH' % p)
            else:
                _new_pypaths.append(p)

        ## reset the new python path
        os.environ['PYTHONPATH'] = ':'.join(_new_pypaths)

    InteractiveShell.magic_fixpython = magic_fixpython
    InteractiveShell.magic_cmtsetup = magic_cmtsetup
    InteractiveShell.magic_setup = magic_setup
    InteractiveShell.magic_subjobs = magic_subjobs

    del magic_cmtsetup
    del magic_setup 
    del magic_fixpython
    del InteractiveShell

except ImportError:
    pass

def starttasks():
    #from GangaAtlas.Lib.Tasks.TaskRegistry import TaskRegistry
    from Ganga.Core.GangaRepository import addRegistry
    from Ganga.Utility.Config import getConfig
    #addRegistry("tasks",TaskRegistry,getConfig("Tasks")["repositoryType"],getConfig("Tasks")["repositoryLocation"])

starttasks()
del starttasks

def _do_setup(setup,tags):

    import re

    setup=os.path.expanduser(setup)

    if not os.path.exists(setup):
       logger.warning('%s does not exist.' % setup)
       return

    if tags:
        cmd = '%s -tag=%s' % (setup,','.join(tags))
    else:
        cmd = setup

    pipe = os.popen('source %s; printenv' % cmd)
    output = pipe.read()
    rc = pipe.close()
    if rc: logger.error('non-zero return code %d from executing setup command',rc)    

    for key, value in re.findall('(\S+)=(\S+)\n',output):
        if key not in ['_','PWD','SHLVL']:
            os.environ[key] = value    
