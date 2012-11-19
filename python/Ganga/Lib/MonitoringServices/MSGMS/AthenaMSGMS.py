from Ganga.Lib.MonitoringServices.MSGMS.MSGMS import MSGMS
import cPickle as pickle

# TODO: move this into GangaAtlas

class AthenaMSGMS(MSGMS):

    def __init__(self, job_info, config_info):
        MSGMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGMS
        return [
            Ganga.Lib.MonitoringServices.MSGMS.AthenaMSGMS,
            ] + MSGMS.getSandboxModules(self)

    def stop(self, exitcode, **opts):
        # create message as in MSGMS
        if exitcode == 0:
            event = "finished"
        else:
            event = "failed"
        message = self.getMessage(event)
        # add UAT09 properties to message
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
        for x in ('input_files','input_guids','athena_options','output_files','output_data','output_guids','output_location'):
            try:
                f = open(x,'r')
                y = ','.join([l.strip() for l in f])
                f.close()
                message['uat09.Athena.%s'%x] = y
            except:
                pass
        # send message
        self.send(message)


