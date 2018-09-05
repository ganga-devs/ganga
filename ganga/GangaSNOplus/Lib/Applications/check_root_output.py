# Tools to check the output file
# Useful!
import os
import sys
import re
import subprocess
import optparse

all_env = {}

class CheckRootException(Exception):
    def __init__(self, error):
        Exception.__init__(self, error)


class OutputChecker(object):
    '''Base class for the output checker
    '''
    def __init__(self):
        pass
    def get_root_entries(self, filename, n_entries):
        pass


class OutputCheckerDev(OutputChecker):
    '''Output checker for RAT-dev.

    Does nothing, because we have no idea which version of RAT is being used
    without checking the git log and doing loads of unreliable stuff.
    '''
    def __init__(self):
        super(OutputCheckerDev, self).__init__()


class OutputCheckerPre460(OutputChecker):
    '''Output checker for RAT-4.5.0 and earlier.
    '''
    def __init__(self):
        super(OutputCheckerPre460, self).__init__()
    def get_root_entries(self, filename, n_entries):
        import ROOT
        import rat
        try:
            tf = ROOT.TFile(filename)
            tt = ROOT.TTree()
            # check root files (and soc files?)
            if tf.Get("T"):
                tt = tf.Get("T")
            # check ntuples
            elif tf.Get("output"):
                tt = tf.Get("output")
            else:
                raise CheckRootException("Neither TTree 'T' nor TTree 'output' exist")

            # check entries in TTrees
            if n_entries == tt.GetEntries():
                pass
            else:
                raise CheckRootException("Number of events simulated is incorrect")
        except Exception as e:
            raise CheckRootException("Cannot get TTree: %s" % e)
    

class OutputCheckerPost460(OutputChecker):
    '''Output checker for RAT-4.6.0 and later.
    '''
    def __init__(self):
        super(OutputCheckerPost460, self).__init__()
    def get_root_entries(self, filename, n_entries):
        import ROOT
        import rat
        try:
            # check standard root files
            if rat.dsreader(filename):
                for ds, run in rat.dsreader(filename):
                    if n_entries == run.GetNumberOfEventsSimulated(run):
                        break # assume simulations only do one run
                    else:
                        raise CheckRootException("Number of events simulated is incorrect")

            #check soc files
            elif rat.socreader(filename):
                for soc, run in rat.socreader(filename):
                    if n_entries == run.GetNumberOfEventsSimulated(run):
                        break # assume simulations only do one run
                    else:
                        raise CheckRootException("Number of events simulated is incorrect")
            #check ntuples
            else:
                tf = ROOT.TFile(filename)
                tt = tf.Get("output")

                # check entries in TTrees
                if n_entries == tt.GetEntries():
                    pass
                else:
                    raise CheckRootException("Number of events simulated is incorrect")            
        except Exception as e:
            raise CheckRootException("Cannot get TTree: %s" % e)


def get_checker(rat_version):
    try:
        (major, minor, patch) = (int(s) for s in rat_version.split('.'))
        if major>4 or (major==4 and minor>5):
            return OutputCheckerPost460()
        else:
            return OutputCheckerPre460()
    except ValueError:
        if rat_version=='dev':
            return OutputCheckerDev()
        else:
            raise CheckRootException("Unknown rat version: %s" % rat_version)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-f", dest="filename")
    parser.add_option("-n", dest="n_entries")
    parser.add_option("-v", dest="rat_version")
    (options, args) = parser.parse_args()

    output_checker = get_checker(options.rat_version)
    output_checker.get_root_entries(options.filename, int(options.n_entries))
    # Signal that checks ran fine
    open("__CHECK_ROOT_SUCCESS__", 'w').close()
