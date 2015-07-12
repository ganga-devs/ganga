from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PickleStreamer.py,v 1.1 2008-07-17 16:40:56 moscicki Exp $
##########################################################################


from .utilities import serialize, gangaObjectFactory
import pickle


class PickleJobStreamer(object):

    def getStreamFromJob(self, job):
        return pickle.dumps(serialize(job))

    def getJobFromStream(self, stream):
        return gangaObjectFactory(pickle.loads(stream))
