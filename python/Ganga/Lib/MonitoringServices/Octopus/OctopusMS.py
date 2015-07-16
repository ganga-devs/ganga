from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
from Ganga.Lib.MonitoringServices.Octopus.Octopus import *
from urlparse import urlparse
import sys
import traceback


class OctopusMS(IMonitoringService):

    """ A simple tool to send the stdout of a job continuusly to an Octopus server so
    that the user can immediately look at it."""

    def __init__(self, job_info):
        IMonitoringService.__init__(self, job_info)
        if isinstance(job_info, type({})):
            print job_info
            self.server = self.job_info['octopus_server']
            self.port = self.job_info['octopus_port']
            self.client = Octopus(self.server, self.port)
            self.channel = self.job_info['channel']
            self.stdoutpos = 0
            self.stderrpos = 0
            self.DEBUG = False

    def start(self, *other):
        if self.DEBUG:
            print 'Start called'
        print 'Octopus channel:', self.channel
        return self.progress(other)

    def progress(self, *other):
        if self.DEBUG:
            print 'Progress called'
        if not self.client.connected:
            try:
                self.client.create(self.channel)
            except ProtocolException as e:
                print >> sys.stderr, 'Error connecting to octopus server: ', e
                return
            self.stdoutFile = open('stdout', 'r')
        try:
            self.stdoutFile.seek(0, 2)
            cLen = self.stdoutFile.tell()
            self.stdoutFile.seek(self.stdoutpos)
            if self.DEBUG:
                print 'File length:', cLen
            while cLen > self.stdoutpos:
                b = self.stdoutFile.read()
                if self.DEBUG:
                    print 'Sending >', b, '<'
                self.client.send(b)
                self.stdoutpos = self.stdoutpos + len(b)
            if self.DEBUG:
                print 'Leaving progress'
        except ProtocolException as e:
            print >> sys.stderr, 'Error sending to octopus server: ', e

    def stop(self, exitcode, *other):
        if self.DEBUG:
            print 'Octopus done!'
        self.client.close()

    def getJobInfo(self):
        import os
        try:
            s = os.environ['GANGA_OCTOPUS_SERVER']
        except KeyError:
            from Ganga.Utility.util import hostname
            s = hostname()

        try:
            p = os.environ['GANGA_OCTOPUS_PORT']
        except KeyError:
            p = 8882

        h = long(hash(self.job_info.getFQID('.')))
        h = h + sys.maxsize * (long(hash(s)) + sys.maxsize)

        return {'jobid': self.job_info.getFQID('.'), 'channel': h,
                'octopus_server': s, 'octopus_port': p}

    def getSandboxModules(self):
        print "Sending sandbox modules"
        import Ganga.Lib.MonitoringServices.Octopus.OctopusMS

        return [Ganga, Ganga.Lib, Ganga.Lib.MonitoringServices, Ganga.Lib.MonitoringServices.Octopus, Ganga.Lib.MonitoringServices.Octopus.OctopusMS, Ganga.Lib.MonitoringServices.Octopus.Octopus] + IMonitoringService.getSandboxModules(self)
