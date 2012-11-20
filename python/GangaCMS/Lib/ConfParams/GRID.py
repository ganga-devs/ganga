#
# Dictionary with all parameters available for the
# section GRID.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class GRID(GangaObject):

    _comments = []
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#rb')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#proxy_server')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#role')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#group')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dont_check_proxy')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dont_check_myproxy')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#requirements')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#additional_jdl_parameters_')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#wms_service')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#max_cpu_time')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#max_wall_clock_time')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ce_black_list')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ce_white_list')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#se_black_list')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#se_white_list')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#remove_default_blacklist')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#virtual_organization')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#retry_count')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#shallow_retry_count')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#maxtarballsize')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#maxtarballsize')

    schemadic = {}
    schemadic['rb']                        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[0])
    schemadic['proxy_server']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[1])
    schemadic['role']                      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[2])
    schemadic['group']                     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[3])
    schemadic['dont_check_proxy']          = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[4])
    schemadic['dont_check_myproxy']        = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[5])
    schemadic['requirements']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[6])
    schemadic['additional_jdl_parameters'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[7])
    schemadic['wms_service']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[8])
    schemadic['max_cpu_time']              = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[9])
    schemadic['max_wall_clock_time']       = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[10])
    schemadic['CE_black_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[11])
    schemadic['CE_white_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[12])
    schemadic['SE_black_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[13])
    schemadic['SE_white_list']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[14])
    schemadic['remove_default_blacklist']  = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[15])
    schemadic['virtual_organization']      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[16])
    schemadic['retry_count']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[17])
    schemadic['shallow_retry_count']       = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[18])
    schemadic['maxtarballsize']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[19])
    schemadic['skipwmsauth']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[20])


    _schema =  Schema(Version(0,0), {})
    _hidden = 1
