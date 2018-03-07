###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ArgSplitter.py,v 1.1 2008-07-17 16:40:59 moscicki Exp $
###############################################################################

import copy
from GangaCore.Core.exceptions import SplitterError
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.Utility.logging import getLogger
logger = getLogger()


class ArgSplitter(ISplitter):

    """
    Split job by changing the args attribute of the application.

    This splitter only applies to the applications which have args attribute (e.g. Executable, Root), or those
    with extraArgs (GaudiExec). If an application has both, args takes precedence.
    It is a special case of the GenericSplitter.

    This splitter allows the creation of a series of subjobs where
    the only difference between different jobs are their
    arguments. Below is an example that executes a ROOT script ~/analysis.C

    void analysis(const char* type, int events) {
      std::cout << type << "  " << events << std::endl;
    }

    with 3 different sets of arguments.

    s = ArgSplitter(args=[['AAA',1],['BBB',2],['CCC',3]])
    r = Root(version='5.10.00',script='~/analysis.C')
    j.Job(application=r, splitter=s)

    Notice how each job takes a list of arguments (in this case a list
    with a string and an integer). The splitter thus takes a list of
    lists, in this case with 3 elements so there will be 3 subjobs.

    Running the subjobs will produce the output:
    subjob 1 : AAA  1
    subjob 2 : BBB  2
    subjob 3 : CCC  3
"""
    _name = "ArgSplitter"
    _schema = Schema(Version(1, 0), {
        'args': SimpleItem(defvalue=[], typelist=[list, GangaList], sequence=1, doc='A list of lists of arguments to pass to script')
    })

    def split(self, job):

        subjobs = []

        for arg in self.args:
            j = self.createSubjob(job,['application'])
            # Add new arguments to subjob
            app = copy.deepcopy(job.application)
            if hasattr(app, 'args'):
                app.args = arg
            elif hasattr(app, 'extraArgs'):
                app.extraArgs = arg
            else:
                raise SplitterError('Application has neither args or extraArgs in its schema') 
                    
            j.application = app
            logger.debug('Arguments for split job is: ' + str(arg))
            subjobs.append(stripProxy(j))

        return subjobs


#
#
# $Log: not supported by cvs2svn $
# Revision 1.7.4.3  2008/07/03 08:36:16  wreece
# Typesystem fix for Splitters
#
# Revision 1.7.4.2  2008/03/12 12:42:38  wreece
# Updates the splitters to check for File objects in the list
#
# Revision 1.7.4.1  2008/02/08 15:09:52  amuraru
# fixed the TypeMismatchError
#
# Revision 1.7  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.6  2006/09/29 08:31:56  moscicki
# typo fix
#
# Revision 1.5  2006/09/15 14:24:24  moscicki
# fixed a typo
#
# Revision 1.4  2006/08/24 16:52:10  moscicki
# splitter.createJob()
#
# Revision 1.3  2006/08/03 08:14:30  moscicki
# SimpleItem fix
#
# Revision 1.2  2006/08/01 10:25:50  moscicki
# small schema fixes
#
# Revision 1.1  2006/06/21 11:27:29  moscicki
# splitters moved to the new location
#
#
#
