###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GenericSplitter.py,v 1.2 2008-09-09 15:11:35 moscicki Exp $
###############################################################################

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.logging import getLogger
logger = getLogger()


class GenericSplitter(ISplitter):

    """
    Split job by changing arbitrary job attribute.

    This splitter allows the creation of a series of subjobs where
    the only difference between different jobs can be defined by giving 
    the "attribute" and "values" of the splitter object.

    For example, to split a job according to the given application arguments:

      s = GenericSplitter()
      s.attribute = 'application.args'
      s.values = [["hello","1"],["hello","2"]]
      ... ...
      j = Job(splitter=s)
      j.submit()

    To split a job into two LCG jobs running on two different CEs:

      s = GenericSplitter()
      s.attribute = 'backend.CE'
      s.value = ["quanta.grid.sinica.edu.tw:2119/jobmanager-lcgpbs-atlas","lcg00125.grid.sinica.edu.tw:2119/jobmanager-lcgpbs-atlas"]
      ... ...
      j = Job(backend=LCG(),splitter=s)
      j.submit()

    to split over mulitple attributes, use the multi_args option:

      j = Job()
      j.splitter = GenericSplitter()
      j.splitter.multi_attrs = { "application.args":["hello1", "hello2"], "application.env":[{"MYENV":"test1"}, {"MYENV":"test2"}] }

    this will result in two subjobs, one with args set to 'hello1' and the MYENV set to 'test1', the other with
    args set to 'hello2' and the MYENV set to 'test2'.

    Known issues of this generic splitter:
      - it will not work if specifying different backends for the subjobs

    """
    _name = "GenericSplitter"
    _schema = Schema(Version(1, 0), {
        'attribute': SimpleItem(defvalue='', doc='The attribute on which the job is splitted'),
        'values': SimpleItem(defvalue=[], typelist=None, sequence=1, doc='A list of the values corresponding to the attribute of the subjobs'),
        'multi_attrs': SimpleItem(defvalue={}, doc='Dictionary to specify multiple attributes to split over'),
    })

    def split(self, job):

        subjobs = []

        # sort out multiple arg splitting
        if (self.attribute != '' or len(self.values) > 0) and len(self.multi_attrs) > 0:
            raise ApplicationConfigurationError(
                None, "Setting both 'attribute'/'values' and 'multi_attrs' is unsupported")

        if self.attribute != '':
            attrlist = [self.attribute]
            values = []
            for v in self.values:
                values.append([v])
        else:
            # check we have enough values in the dictionary
            numjobs = -1
            attrlist = []
            for attr in self.multi_attrs:
                if numjobs == -1:
                    numjobs = len(self.multi_attrs[attr])
                else:
                    if len(self.multi_attrs[attr]) != numjobs:
                        raise ApplicationConfigurationError(
                            None, "Number of values for '%s' doesn't equal others '%d'" % (attr, numjobs))

                attrlist.append(attr)

            # now get everything organised
            values = []
            for i in range(0, numjobs):
                valtmp = []
                for attr in attrlist:
                    valtmp.append(self.multi_attrs[attr][i])
                values.append(valtmp)

        # check we have enough values to cover the attributes
        for vallist in values:
            if len(attrlist) != len(vallist):
                raise ApplicationConfigurationError(
                    None, "Number of attributes to split over doesn't equal number of values in list '%s'" % vallist)

        # now perform the split
        for vallist in values:

            # for each list of values, set the attributes
            j = addProxy(self.createSubjob(job))

            for i in range(0, len(attrlist)):
                attrs = attrlist[i].split('.')
                obj = j
                for attr in attrs[:-1]:
                    obj = getattr(obj, attr)
                attr = attrs[-1]
                setattr(obj, attr, vallist[i])
                logger.debug('set %s = %s to subjob.' %
                             (attrlist[i], getattr(obj, attr)))

            subjobs.append(stripProxy(j))

        return subjobs
#
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/07/17 16:41:00  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.3.4.3  2008/07/03 08:36:16  wreece
# Typesystem fix for Splitters
#
# Revision 1.3.4.2  2008/03/12 12:42:38  wreece
# Updates the splitters to check for File objects in the list
#
# Revision 1.3.4.1  2008/02/06 17:04:58  moscicki
# disabled type checking for GenericSplitter
#
# Revision 1.3  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.2  2007/01/25 16:25:59  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.1.2.2  2006/10/30 16:15:47  hclee
# change the in-code documentation
#
# Revision 1.1.2.1  2006/10/30 16:10:51  hclee
# Add GenericSplitter
#
#
#
