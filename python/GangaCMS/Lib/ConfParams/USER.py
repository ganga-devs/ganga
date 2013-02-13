#
# Dictionary with all parameters available for the
# section User.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class USER(GangaObject):

    _comments = []
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#additional_input_files')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#script_exe')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#script_arguments')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ui_working_dir')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#thresholdlevel')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#email')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#email')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#return_data__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#outputdir')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#logdir')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#copy_data__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_element')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#user_remote_dir')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_path')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_pool')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#storage_port')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#local_stage_out__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_data_')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_data_name')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dbs_url_for_publication')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#publish_zero_event')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#srm_version')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#xml_report')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#usenamespace')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#debug_wrapper')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#deep_debug')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#dontcheckspaceleft')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#check_user_remote_dir')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#tasktype')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#ssh_control_persist')

    schemadic = {} 
    schemadic['additional_input_files']  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[0])
    schemadic['script_exe']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[1])
    schemadic['script_arguments']        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[2])
    schemadic['ui_working_dir']          = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[3])
    schemadic['thresholdLevel']          = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[4])
    schemadic['eMail']                   = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[5])
    schemadic['client']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[6])
    schemadic['return_data']             = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[7])
    schemadic['outputdir']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[8])
    schemadic['logdir']                  = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[9])
    schemadic['copy_data']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[10])
    schemadic['storage_element']         = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[11])
    schemadic['user_remote_dir']         = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[12])
    schemadic['storage_path']            = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[13])
    schemadic['storage_pool']            = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[14])
    schemadic['storage_port']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[15])
    schemadic['local_stage_out']         = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[16])
    schemadic['publish_data']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[17])
    schemadic['publish_data_name']       = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[18])
    schemadic['dbs_url_for_publication'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[19])
    schemadic['publish_zero_event']      = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[20])
    schemadic['srm_version']             = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[21])
    schemadic['xml_report']              = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[22])
    schemadic['usenamespace']            = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[23])
    schemadic['debug_wrapper']           = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[24])
    schemadic['deep_debug']              = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[25])
    schemadic['dontCheckSpaceLeft']      = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[26])
    schemadic['check_user_remote_dir']   = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[27])
    schemadic['tasktype']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[28])
    schemadic['ssh_control_persist']     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[29])


    _schema =  Schema(Version(0,0), {})
    _hidden = 1
 
