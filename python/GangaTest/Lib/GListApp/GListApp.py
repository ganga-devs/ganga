##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GListApp.py,v 1.2 2008-10-22 11:59:19 wreece Exp $
##########################################################################
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.File import File, ShareDir


class GListApp(IPrepareApp):

    """Test File object with well known equality properties -i.e. Does not reply on proxy!"""
# summary_print
    _category = 'applications'
    _exportedmethods = ['configure']
    _name = 'GListApp'
    _schema = Schema(Version(1, 0), {
        'bound_print_comp': ComponentItem('files', defvalue=[], sequence=1, summary_print='_print_summary_bound_comp', typelist=['str', 'Ganga.test.GPI.GangaList.TFile.TFile']),
        'bound_print_simple': SimpleItem(defvalue=[], sequence=1, summary_print='_print_summary_bound_simple'),
        'no_summary': SimpleItem(defvalue=[], sequence=1, summary_sequence_maxlen=-1, typelist=['str']),
        'seq': SimpleItem(defvalue=[], sequence=1, typelist=['int']),
        'gList': SimpleItem(defvalue=[], sequence=1, typelist=['str']),
        'gListComp': ComponentItem('files', defvalue=[], sequence=1),
        'simple_print': SimpleItem(defvalue='', summary_print='_print_summary_simple_print'),
        'comp_print': ComponentItem('backends', defvalue=None, summary_print='_print_summary_comp_print'),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=['type(None)', 'bool', ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
    })

    def configure(self, master_appconfig):
        return (None, None)

    def _print_summary_bound_comp(self, value, verbosity_level):
        return '_print_summary_bound_comp'

    def _print_summary_bound_simple(self, value, verbosity_level):
        return '_print_summary_bound_simple'

    def _print_summary_simple_print(self, value, verbosity_level):
        return '_print_summary_simple_print'

    def _print_summary_comp_print(self, value, verbosity_level):
        return '_print_summary_comp_print'


class Handler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        return 0
allHandlers.add('GListApp', 'TestSubmitter', Handler)
