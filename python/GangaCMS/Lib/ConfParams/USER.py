#
# Dictionary with all parameters available for the
# section User.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Schema import SimpleItem

class USER:

    comments = []
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#additional_input_files')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#script_exe')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#script_arguments')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ui_working_dir')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#thresholdlevel')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#email')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#email')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#return_data__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#outputdir')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#logdir')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#copy_data__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_element')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#user_remote_dir')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_path')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_pool')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_port')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#local_stage_out__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_data_')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_data_name')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dbs_url_for_publication')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_zero_event')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#srm_version')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#xml_report')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#usenamespace')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#debug_wrapper')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#deep_debug')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dontcheckspaceleft')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#check_user_remote_dir')

    schemadic = {} 
    schemadic['additional_input_files']  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[0])
    schemadic['script_exe']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[1])
    schemadic['script_arguments']        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[2])
    schemadic['ui_working_dir']          = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[3])
    schemadic['thresholdLevel']          = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[4])
    schemadic['eMail']                   = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[5])
    schemadic['client']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[6])
    schemadic['return_data']             = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[7])
    schemadic['outputdir']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[8])
    schemadic['logdir']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[9])
    schemadic['copy_data']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[10])
    schemadic['storage_element']         = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[11])
    schemadic['user_remote_dir']         = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[12])
    schemadic['storage_path']            = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[13])
    schemadic['storage_pool']            = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[14])
    schemadic['storage_port']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[15])
    schemadic['local_stage_out']         = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[16])
    schemadic['publish_data']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[17])
    schemadic['publish_data_name']       = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[18])
    schemadic['dbs_url_for_publication'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[19])
    schemadic['publish_zero_event']      = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[20])
    schemadic['srm_version']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[21])
    schemadic['xml_report']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[22])
    schemadic['usenamespace']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[23])
    schemadic['debug_wrapper']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[24])
    schemadic['deep_debug']              = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[25])
    schemadic['dontCheckSpaceLeft']      = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[26])
    schemadic['check_user_remote_dir']   = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[27])

    _GUIPrefs = [
                  { 'attribute' : 'additional_input_files'  , 'widget' : 'String' },
                  { 'attribute' : 'script_exe'              , 'widget' : 'String' },
                  { 'attribute' : 'script_arguments'        , 'widget' : 'String' },
                  { 'attribute' : 'ui_working_dir'          , 'widget' : 'String' },
                  { 'attribute' : 'thresholdLevel'          , 'widget' : 'String' },
                  { 'attribute' : 'eMail'                   , 'widget' : 'String' },
                  { 'attribute' : 'client'                  , 'widget' : 'String' },
                  { 'attribute' : 'return_data'             , 'widget' : 'String' },
                  { 'attribute' : 'outputdir'               , 'widget' : 'String' },
                  { 'attribute' : 'logdir'                  , 'widget' : 'String' },
                  { 'attribute' : 'copy_data'               , 'widget' : 'String' },
                  { 'attribute' : 'storage_element'         , 'widget' : 'String' },
                  { 'attribute' : 'user_remote_dir'         , 'widget' : 'String' },
                  { 'attribute' : 'storage_path'            , 'widget' : 'String' },
                  { 'attribute' : 'storage_pool'            , 'widget' : 'String' },
                  { 'attribute' : 'storage_port'            , 'widget' : 'String' },
                  { 'attribute' : 'local_stage_out'         , 'widget' : 'String' },
                  { 'attribute' : 'publish_data'            , 'widget' : 'String' },
                  { 'attribute' : 'publish_data_name'       , 'widget' : 'String' },
                  { 'attribute' : 'dbs_url_for_publication' , 'widget' : 'String' },
                  { 'attribute' : 'publish_zero_event'      , 'widget' : 'String' },
                  { 'attribute' : 'srm_version'             , 'widget' : 'String' },
                  { 'attribute' : 'xml_report'              , 'widget' : 'String' },
                  { 'attribute' : 'usenamespace'            , 'widget' : 'String' },
                  { 'attribute' : 'debug_wrapper'           , 'widget' : 'String' },
                  { 'attribute' : 'deep_debug'              , 'widget' : 'String' },
                  { 'attribute' : 'dontCheckSpaceLeft'      , 'widget' : 'String' },
                  { 'attribute' : 'check_user_remote_dir'   , 'widget' : 'String' }
                ]

