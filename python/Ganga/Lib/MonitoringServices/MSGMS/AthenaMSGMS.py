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
        try:
            f = open('stats.pickle','r')
            stats = pickle.load(f)
            f.close()
            for (k,v) in stats:
                message['uat09.'+k]=v
            message['uat09.stats'] = stats
        except:
            pass

        from Ganga.Lib.MonitoringServices.MSGMS import sendJobStatusChange
        sendJobStatusChange( message )


