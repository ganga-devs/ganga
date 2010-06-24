#
# Dictionary with all parameters available for the
# section CMSSW.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Schema import SimpleItem

class CMSSW:

    comments = []
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#datasetpath__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#runselection__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#use_parent')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#pset__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#pycfg_params__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#lumi_mask')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#total_number_of_events__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#events_per_job_')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#total_number_of_lumis__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#lumis_per_job_')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#number_of_jobs__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#split_by_run')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#output_file__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#skip_tfileservice_output')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#get_edm_output')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#increment_seeds')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#preserve_seeds')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#first_lumi')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#generator')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#executable')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dbs_url')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#show_prod')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#subscribed')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#no_block_boundary')
    comments.append('--- TO DO ---')

    schemadic = {}
    schemadic['datasetpath']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[0])
    schemadic['runselection']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[1])
    schemadic['use_parent']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[2])
    schemadic['pset']                     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[3])
    schemadic['pycfg_params']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[4])
    schemadic['lumi_mask']                = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[5])
    schemadic['total_number_of_events']   = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[6])
    schemadic['events_per_job']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[7])
    schemadic['total_number_of_lumis']    = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[8])
    schemadic['lumis_per_job']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[9])
    schemadic['number_of_jobs']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[10])
    schemadic['split_by_run']             = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[11])
    schemadic['output_file']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[12])
    schemadic['skip_TFileService_output'] = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[13])
    schemadic['get_edm_output']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[14])
    schemadic['increment_seeds']          = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[15])
    schemadic['preserve_seeds']           = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[16])
    schemadic['first_lumi']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[17])
    schemadic['generator']                = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[18])
    schemadic['executable']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[19])
    schemadic['dbs_url']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[20])
    schemadic['show_prod']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[21])
    schemadic['subscribed']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[22])
    schemadic['no_block_boundary']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[23])
    schemadic['ignore_edm_output']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[24])
    schemadic['use_dbs_1']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[24])

    GUIPrefs = [
                  { 'attribute' : 'datasetpath'              , 'widget' : 'String' },
                  { 'attribute' : 'runselection'             , 'widget' : 'String' },
                  { 'attribute' : 'use_parent'               , 'widget' : 'String' },
                  { 'attribute' : 'pset'                     , 'widget' : 'String' },
                  { 'attribute' : 'pycfg_params'             , 'widget' : 'String' },
                  { 'attribute' : 'lumi_mask'                , 'widget' : 'String' },
                  { 'attribute' : 'total_number_of_events'   , 'widget' : 'String' },
                  { 'attribute' : 'events_per_job'           , 'widget' : 'String' },
                  { 'attribute' : 'total_number_of_lumis'    , 'widget' : 'String' },
                  { 'attribute' : 'lumis_per_job'            , 'widget' : 'String' },
                  { 'attribute' : 'number_of_jobs'           , 'widget' : 'String' },
                  { 'attribute' : 'split_by_run'             , 'widget' : 'String' },
                  { 'attribute' : 'output_file'              , 'widget' : 'String' },
                  { 'attribute' : 'skip_TFileService_output' , 'widget' : 'String' },
                  { 'attribute' : 'get_edm_output'           , 'widget' : 'String' },
                  { 'attribute' : 'increment_seeds'          , 'widget' : 'String' },
                  { 'attribute' : 'preserve_seeds'           , 'widget' : 'String' },
                  { 'attribute' : 'first_lumi'               , 'widget' : 'String' },
                  { 'attribute' : 'generator'                , 'widget' : 'String' },
                  { 'attribute' : 'executable'               , 'widget' : 'String' },
                  { 'attribute' : 'dbs_url'                  , 'widget' : 'String' },
                  { 'attribute' : 'show_prod'                , 'widget' : 'String' },
                  { 'attribute' : 'subscribed'               , 'widget' : 'String' },
                  { 'attribute' : 'no_block_boundary'        , 'widget' : 'String' }
                ]



