##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Separator.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
##########################################################################


class Parser(object):

    def extractSubJobs(self, attrDict):
        sjobs = attrDict['data']['subjobs']
        if sjobs:
            attrDict['data']['subjobs'] = []
            res = map(self.extractSubJobs, sjobs)
        else:
            res = []
        return (attrDict, res)
    extractSubJobs = classmethod(extractSubJobs)

    def insertSubJobs(self, sjob_tree):
        attrDict = sjob_tree[0]
        attrDict['data']['subjobs'] = map(self.insertSubJobs, sjob_tree[1])
        return attrDict
    insertSubJobs = classmethod(insertSubJobs)
