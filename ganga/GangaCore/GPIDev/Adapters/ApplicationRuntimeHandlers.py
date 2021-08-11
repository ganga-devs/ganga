##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ApplicationRuntimeHandlers.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

""" Application adapter table is a mechanism to match application and backend handlers.
"""


class _ApplicationRuntimeHandlers(object):

    def __init__(self):
        self.handlers = {}

    def add(self, application, backend, handler):
        self.handlers.setdefault(backend, {})[application] = handler

    def get(self, application, backend):
        return self.handlers[backend][application]

    def getAllBackends(self, application=None):
        if application is None:
            return list(self.handlers.keys())
        else:
            return [b for b in self.handlers.keys() if application in self.handlers[b]]

    def getAllApplications(self, backend=None):
        if backend is None:
            apps = {}

            for a in self.handlers.values():
                apps.update(a)

            return list(apps.keys())
        else:
            return list(self.handlers[backend].keys())

allHandlers = _ApplicationRuntimeHandlers()

if __name__ == '__main__':

    a = _ApplicationRuntimeHandlers()
    a.add('a', 'X', 1)
    a.add('a', 'Y', 1)
    a.add('b', 'X', 1)
    a.add('c', 'Z', 1)

    def compare(alist, blist):
        alist.sort()
        blist.sort()
        assert(alist == blist)

    compare(a.getAllBackends(), ['X', 'Y', 'Z'])
    compare(a.getAllApplications(), ['a', 'b', 'c'])

    compare(a.getAllBackends('a'), ['X', 'Y'])
    compare(a.getAllBackends('b'), ['X'])
    compare(a.getAllBackends('c'), ['Z'])

    compare(a.getAllApplications('X'), ['a', 'b'])
    compare(a.getAllApplications('Y'), ['a'])
    compare(a.getAllApplications('Z'), ['c'])
