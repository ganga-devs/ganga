#
# Dictionary with all parameters available for the
# section CRAB.
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Schema import SimpleItem

class CRAB:

    comments = []
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#jobtype__')
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#server_name') 
    comments.append('http://belforte.home.cern.ch/belforte/misc/test/crab-v2.7.2.html#use_server')
    comments.append('--- TO DO ---')

    schemadic = {}
    schemadic['jobtype']     = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[0])
    schemadic['server_name'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[1])
    schemadic['use_server']  = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc=comments[2])
    schemadic['scheduler']   = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[3])

    GUIPrefs = [
                  { 'attribute' : 'jobtype'     , 'widget' : 'String' },
                  { 'attribute' : 'server_name' , 'widget' : 'String' },
                  { 'attribute' : 'use_server'  , 'widget' : 'String' }
                ]
