#
# Dictionary with all parameters available for the
# section CMSSW.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class CMSSW(GangaObject):

    _comments = []
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#datasetpath__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#runselection__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#use_parent')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#pset__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#pycfg_params__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#lumi_mask')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#total_number_of_events__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#events_per_job_')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#total_number_of_lumis__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#lumis_per_job_')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#number_of_jobs__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#split_by_run')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#output_file__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#skip_tfileservice_output')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#get_edm_output')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#increment_seeds')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#preserve_seeds')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#first_lumi')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#generator')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#executable')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dbs_url')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#show_prod')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#subscribed')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#no_block_boundary')
    _comments.append('--- TO DO ---')

    schemadic = {}
    schemadic['datasetpath']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[0])
    schemadic['runselection']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[1])
    schemadic['use_parent']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[2])
    schemadic['pset']                     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[3])
    schemadic['pycfg_params']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[4])
    schemadic['lumi_mask']                = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[5])
    schemadic['total_number_of_events']   = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[6])
    schemadic['events_per_job']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[7])
    schemadic['split_by_event']           = SimpleItem(defvalue=None, typelist=['type(None)','int'])
    schemadic['total_number_of_lumis']    = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[8])
    schemadic['lumis_per_job']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[9])
    schemadic['number_of_jobs']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[10])
    schemadic['split_by_run']             = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[11])
    schemadic['output_file']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[12])
    schemadic['skip_TFileService_output'] = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[13])
    schemadic['get_edm_output']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[14])
    schemadic['increment_seeds']          = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[15])
    schemadic['preserve_seeds']           = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[16])
    schemadic['first_lumi']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[17])
    schemadic['generator']                = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[18])
    schemadic['executable']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[19])
    schemadic['dbs_url']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[20])
    schemadic['show_prod']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[21])
    schemadic['subscribed']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[22])
    schemadic['no_block_boundary']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[23])
    schemadic['ignore_edm_output']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[24])
    schemadic['use_dbs_1']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[24])


    _schema =  Schema(Version(0,0), {})
    _hidden = 1
