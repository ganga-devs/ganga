# Example startup script for loading modules allowing Ganga submission
# of ADA jobs to DIAL backend

import os, sys
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

print >>sys.stderr
print >>sys.stderr, 'ATLAS User Support is provided by the Hypernews Forum Ganga User and Developers'
print >>sys.stderr, 'You find the forum at https://hypernews.cern.ch/HyperNews/Atlas/get/GANGAUserDeveloper.html'
print >>sys.stderr, 'or you can send an email to hn-atlas-GANGAUserDeveloper@cern.ch'
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

            job = jobs(jobnr)
            if not job:
               logger.warning('Job %d does not exist.',jobnr)
               continue

            print '\nJob %d - %s - Application %s - Backend %s - Subjobs %3d' % (job.id,job.status,job.application._impl._name,job.backend._impl._name,len(job.subjobs))
            print '\n#    id    status  backend status  actualCE\n'

            if job.backend._impl._name == 'Panda' and job.backend.buildjob:
               print '# build %-10s %-15s %-20s\n' % (job.status,job.backend.buildjob.status,job.backend.actualCE) 

            for subjob in job.subjobs:
                try:
                    ce = subjob.backend.actualCE
                except AttributeError:
                    ce = ''
                try:
                    be_status = subjob.backend.status
                except AttributeError:
                    be_status = ''
                print '#%6d %-10s %-15s %-20s' % (subjob.id%10000, subjob.status, be_status, ce)

    InteractiveShell.magic_cmtsetup = magic_cmtsetup
    InteractiveShell.magic_setup = magic_setup
    InteractiveShell.magic_subjobs = magic_subjobs

    del magic_cmtsetup
    del magic_setup 
    del InteractiveShell



except ImportError:
    pass

def starttasks():
    from GangaAtlas.Lib.Tasks.tasklist import TaskList
    from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
    reload(Ganga.GPIDev.Persistency)
    from Ganga.GPIDev.Persistency import load
    from Ganga.Runtime.GPIexport import exportToGPI
    from Ganga.Utility.Config import getConfig
    import os
    import os.path
    fn = os.path.join(getConfig("Configuration")["gangadir"],"tasks.dat")
    try:
       os.stat(fn)
       plists = load(fn)
       if len(plists) > 0:
          pl = plists[0]
          logger.info("Tasks read from file")
       else:
          logger.warning("No Tasks in Persistency! Creating new Task list.")
          pl = GPIProxyObjectFactory(TaskList())
    except OSError:
       logger.info("Starting for first launch - Creating new Task list.")
       pl = GPIProxyObjectFactory(TaskList())

    exportToGPI('tasks',pl,'Objects','List of all tasks')
    pl.start()

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
