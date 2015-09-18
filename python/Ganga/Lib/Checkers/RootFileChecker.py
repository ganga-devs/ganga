##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IChecker import IFileChecker
from Ganga.GPIDev.Schema import FileItem, SimpleItem
from Ganga.Utility.logging import getLogger
import commands
import copy
import os
import re


logger = getLogger()


def SortedValues(adict):
    items = sorted(adict.items())
    return [value for key, value in items]


def GetKeyNames(f, dir=""):
    import ROOT
    f.cd(dir)
    return [key.GetName() for key in ROOT.gDirectory.GetListOfKeys()]


def GetTreeObjects(f, dir=""):
    import ROOT
    tree_dict = {}
    for tdir in GetKeyNames(f, dir):
        if tdir == "":
            continue
        absdir = os.path.join(dir, tdir)
        if isinstance(f.Get(tdir), ROOT.TDirectory):
            for absdir, tree in GetTreeObjects(f, absdir).iteritems():
                tree_dict[absdir] = tree
        if isinstance(f.Get(absdir), ROOT.TTree):
            tree_dict[absdir] = f.Get(absdir)
    return tree_dict


class RootFileChecker(IFileChecker):

    """
    Checks ROOT files to see if they are zombies.
    For master job, also checks to see if merging performed correctly.
    self.files are the files you would like to check.
    self.fileMustExist toggles whether to fail the job if the specified file doesn't exist (default is True).
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['checkMerge'] = SimpleItem(
        defvalue=True, doc='Toggle whether to check the merging proceedure')
    _category = 'postprocessor'
    _name = 'RootFileChecker'
    _exportmethods = ['check']

    def checkBranches(self, mastertrees, subtrees):
        import ROOT
        for masterpath, mastertree in mastertrees.iteritems():
            for subpath, subtree in subtrees.iteritems():
                if (subpath == masterpath):
                    subbranches = [branch.GetName()
                                   for branch in subtree.GetListOfBranches()]
                    masterbranches = [branch.GetName()
                                      for branch in mastertree.GetListOfBranches()]
                    if (subbranches != masterbranches):
                        return self.failure
        return self.success

    def addEntries(self, mastertrees, subtrees, entries_dict):
        import ROOT
        for masterpath, mastertree in mastertrees.iteritems():
            for subpath, subtree in subtrees.iteritems():
                if (subpath == masterpath):
                    if (subpath in entries_dict):
                        entries_dict[subpath] += subtree.GetEntries()
                    else:
                        entries_dict[subpath] = subtree.GetEntries()
        return entries_dict

    def checkMergeable(self, f):
        import ROOT
        tf = ROOT.TFile.Open(f)
        if tf.IsZombie():
            logger.info('ROOT file %s is a zombie, failing job', f)
            tf.Close()
            return self.failure
        if not len(GetKeyNames(tf)):
            logger.info('ROOT file %s has no keys, failing job', f)
            tf.Close()
            return self.failure
        tf.Close()
        if (os.path.getsize(f) < 330):
            logger.info('ROOT file %s has no size, failing job', f)
            return self.failure
        return self.success

    def check(self, job):
        """
        Check that ROOT files are not zombies and were closed properly, also (for master job only) checks that the merging performed correctly.
        """
        import ROOT
        self.result = True
        filepaths = self.findFiles(job)
        if self.result is False:
            return self.failure
        if not len(filepaths):
            raise PostProcessException(
                'None of the files to check exist, RootFileChecker will do nothing!')
        for f in filepaths:
            if f.find('.root') < 0:
                raise PostProcessException('The file "%s" is not a ROOT file, RootFileChecker will do nothing!' % os.path.basename(f))
            if not self.checkMergeable(f):
                return self.failure
            if (len(job.subjobs) and self.checkMerge):
                haddoutput = f + '.hadd_output'
                if not os.path.exists(haddoutput):
                    logger.warning('Hadd output file %s does not exist, cannot perform check on merging.', haddoutput)
                    return self.success

                for failString in ['Could not find branch', 'One of the export branches', 'Skipped file']:
                    grepoutput = commands.getoutput('grep "%s" %s' % (failString, haddoutput))
                    if len(grepoutput):
                        logger.info('There was a problem with hadd, the string "%s" was found. Will fail job', failString)
                        return self.failure

                tf = ROOT.TFile.Open(f)
                mastertrees = GetTreeObjects(tf)
                entries_dict = {}
                for sj in job.subjobs:
                    if (sj.status == 'completed'):
                        for subfile in self.findFiles(sj):
                            if (os.path.basename(subfile) == os.path.basename(f)):
                                subtf = ROOT.TFile.Open(subfile)
                                subtrees = GetTreeObjects(subtf)

                                substructure = sorted(subtrees.keys())
                                masterstructure = sorted(mastertrees.keys())
                                if (substructure != masterstructure):
                                    logger.info('File structure of subjob %s is not the same as master job, failing job', sj.fqid)
                                    return self.failure

                                if not self.checkBranches(mastertrees, subtrees):
                                    logger.info('The tree structure of subjob %s is not the same as merged tree, failing job', sj.fqid)
                                    return self.failure
                                entries_dict = self.addEntries(
                                    mastertrees, subtrees, entries_dict)
                                subtf.Close()

                master_entries_dict = dict(
                    (n, mastertrees[n].GetEntries()) for n in set(mastertrees))
                if (SortedValues(entries_dict) != SortedValues(master_entries_dict)):
                    logger.info(
                        'Sum of subjob tree entries is not the same as merged tree entries for file %s, failing job (check hadd output)', os.path.basename(f))
                    return self.failure
                tf.Close()
        return self.result


