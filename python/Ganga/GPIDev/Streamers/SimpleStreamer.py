##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SimpleStreamer.py,v 1.1 2008-07-17 16:40:56 moscicki Exp $
##########################################################################


from utilities import serialize, gangaObjectFactory

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger(modulename=1)


class SimpleJobStreamer(object):

    def _getJobFromDict(self, attrDict):
        j, migrated, errors = gangaObjectFactory(attrDict)
        if errors:
            j.status = 'incomplete'
            msg = "Job %d can not be completely recreated because of the errors %s It is created in incomplete state." % (
                j.id, str(map(str, errors)))
            logger.error(msg)
        else:
            if migrated:
                # add job to the MigrationControl list for flushing back to the
                # repository
                from MigrationControl import migrated_jobs
                if j not in migrated_jobs:
                    migrated_jobs.append(j)
        return j

    def _getDictFromJob(self, job):
        return serialize(job)

    def getStreamFromJob(self, job):
        return repr(self._getDictFromJob(job))

    def getJobFromStream(self, stream):
        return self._getJobFromDict(eval(stream))


class SimpleTreeStreamer(object):

    def getStreamFromTree(self, tree):
        return repr(serialize(tree))

    def getTreeFromStream(self, stream):
        tree, migrated, errors = gangaObjectFactory(eval(stream))
        if migrated:
            msg = "JobTree was migrated from previous version"
            logger.warning(msg)
        if errors:
            tree = None
            msg = "JobTree can not be recreated because of the errors %s." % str(
                map(str, errors))
            logger.error(msg)
        return tree
