#
# Dictionary with all parameters available for the
# section CRAB.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class CRAB(GangaObject):

    _comments = []
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#jobtype__')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#server_name') 
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#use_server')
    _comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#scheduler')
    _comments.append('--- TO DO ---')

    schemadic = {}
    schemadic['jobtype']     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[0])
    schemadic['server_name'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[1])
    schemadic['use_server']  = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=_comments[2])
    schemadic['scheduler']   = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[3])
    schemadic['submit_host'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=_comments[4])


    _schema =  Schema(Version(0,0), {})
    _hidden = 1
