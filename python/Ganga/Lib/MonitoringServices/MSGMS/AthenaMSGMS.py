from Ganga.Lib.MonitoringServices.MSGMS import MSGMS
import cPickle as pickle

class AthenaMSGMS(MSGMS):

    def __init__(self,job_info):
        MSGMS.__init__(self, job_info)

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGMS.AthenaMSGMS
        return [
            Ganga.Lib.MonitoringServices.MSGMS.AthenaMSGMS,
            ] + MSGMS.getSandboxModules(self)


    def stop(self, exitcode, **opts):
        exit_status = None
        if exitcode == 0:
            exit_status = "finished"
        else:
            exit_status = "failed"

        message = self.getMessage()
        message['event'] = exit_status
        import os
        message['uat09.ls'] = os.listdir('.')
        message['uat09.env'] = os.environ
        try:
            f = open('stats.pickle','r')
            stats = pickle.load(f)
            f.close()
            message['uat09.Athena.stats'] = stats
        except:
            pass
        for x in ('input_files','input_guids','athena_options','output_files'):
            try:
                f = open(x,'r')
                y = ','.join([l.strip() for l in f])
                f.close()
                message['uat09.Athena.%s'%x] = y
            except:
                pass

        from Ganga.Lib.MonitoringServices.MSGMS import sendJobStatusChange
        sendJobStatusChange( message )


