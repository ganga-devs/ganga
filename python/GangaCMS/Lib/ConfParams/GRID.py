#
# Dictionary with all parameters available for the
# section GRID.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Schema import SimpleItem

class GRID:

    comments = []
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#rb')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#proxy_server')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#role')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#group')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dont_check_proxy')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dont_check_myproxy')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#requirements')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#additional_jdl_parameters_')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#wms_service')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#max_cpu_time')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#max_wall_clock_time')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ce_black_list')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ce_white_list')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#se_black_list')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#se_white_list')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#remove_default_blacklist')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#virtual_organization')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#retry_count')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#shallow_retry_count')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#maxtarballsize')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#maxtarballsize')

    schemadic = {}
    schemadic['rb']                        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[0])
    schemadic['proxy_server']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[1])
    schemadic['role']                      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[2])
    schemadic['group']                     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[3])
    schemadic['dont_check_proxy']          = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[4])
    schemadic['dont_check_myproxy']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[5])
    schemadic['requirements']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[6])
    schemadic['additional_jdl_parameters'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[7])
    schemadic['wms_service']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[8])
    schemadic['max_cpu_time']              = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[9])
    schemadic['max_wall_clock_time']       = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[10])
    schemadic['CE_black_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[11])
    schemadic['CE_white_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[12])
    schemadic['SE_black_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[13])
    schemadic['SE_white_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[14])
    schemadic['remove_default_blacklist']  = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[15])
    schemadic['virtual_organization']      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[16])
    schemadic['retry_count']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[17])
    schemadic['shallow_retry_count']       = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[18])
    schemadic['maxtarballsize']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[19])
    schemadic['skipwmsauth']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[20])

    GUIPrefs = [
                  { 'attribute' : 'rb'                        , 'widget' : 'String' },
                  { 'attribute' : 'proxy_server'              , 'widget' : 'String' },
                  { 'attribute' : 'role'                      , 'widget' : 'String' },
                  { 'attribute' : 'group'                     , 'widget' : 'String' },
                  { 'attribute' : 'dont_check_proxy'          , 'widget' : 'String' },
                  { 'attribute' : 'dont_check_myproxy'        , 'widget' : 'String' },
                  { 'attribute' : 'requirements'              , 'widget' : 'String' },
                  { 'attribute' : 'additional_jdl_parameters' , 'widget' : 'String' },
                  { 'attribute' : 'wms_service'               , 'widget' : 'String' },
                  { 'attribute' : 'max_cpu_time'              , 'widget' : 'String' },
                  { 'attribute' : 'max_wall_clock_time'       , 'widget' : 'String' },
                  { 'attribute' : 'ce_black_list'             , 'widget' : 'String' },
                  { 'attribute' : 'ce_white_list'             , 'widget' : 'String' },
                  { 'attribute' : 'se_black_list'             , 'widget' : 'String' },
                  { 'attribute' : 'se_white_list'             , 'widget' : 'String' },
                  { 'attribute' : 'remove_default_blacklist'  , 'widget' : 'String' },
                  { 'attribute' : 'virtual_organization'      , 'widget' : 'String' },
                  { 'attribute' : 'retry_count'               , 'widget' : 'String' },
                  { 'attribute' : 'shallow_retry_count'       , 'widget' : 'String' },
                  { 'attribute' : 'maxtarballsize'            , 'widget' : 'String' },
                  { 'attribute' : 'skipwmsauth'               , 'widget' : 'String' }
                ]

