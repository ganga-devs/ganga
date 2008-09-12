from qt import QCustomEvent
from threading import Lock

UPDATE_PREVIEW_EVENT          = 65536
UPDATE_RESULTS_EVENT          = 65537
UPDATE_MONITOR_EVENT          = 65538
CALLBACK_EVENT                = 65539
UPDATE_CLIENTS_EVENT          = 65540
UPDATE_LOG_EVENT              = 65541
JB_TABWIDGET_SETBUTTONS_EVENT = 65542
JOB_SETWIDGETS_EVENT          = 65543
UPDATE_PROGRESS_EVENT         = 65544
JB_TABWIDGET_CLOSETAB_EVENT   = 65545
CMDIALOG_EVENT                = 65546
ACTION_PROGRESS_EVENT         = 65547
REMOVE_ACTION_EVENT           = 65548

customEVNum                   = 65600
customEventDict = {}
geLock = Lock()

def getNewEventNumber( eventName ):
   global customEVNum, customEventDict
   if not geLock.acquire( False ):
      return None
   if eventName in customEventDict:
      geLock.release()
      return None
   customEVNum += 1
   n = customEVNum
   customEventDict[ eventName ] = n
   geLock.release()
   return n

class UpdatePreview_CustomEvent( QCustomEvent ):
   def __init__( self, repository, uid ):
      QCustomEvent.__init__( self, UPDATE_PREVIEW_EVENT )
      self.repository = repository
      self.uid = uid


class UpdateResults_CustomEvent( QCustomEvent ):
   def __init__( self, _results, _schema, numRecordsFound ):
      QCustomEvent.__init__( self, UPDATE_RESULTS_EVENT )
      self._results = _results
      self._schema = _schema
      self.numRecordsFound = numRecordsFound


class UpdateMonitorStatus_CustomEvent( QCustomEvent ):
   def __init__( self, serverDown ):
      QCustomEvent.__init__( self, UPDATE_MONITOR_EVENT )
      self.serverDown = serverDown

        
class Callback_CustomEvent( QCustomEvent ):
   def __init__( self, jobStatusDictFunc ):
      QCustomEvent.__init__( self, CALLBACK_EVENT )
      self.jobStatusDictFunc = jobStatusDictFunc


class UpdateClients_CustomEvent( QCustomEvent ):
   def __init__( self ):
      QCustomEvent.__init__( self, UPDATE_CLIENTS_EVENT )


class UpdateLog_CustomEvent( QCustomEvent ):
   def __init__( self, text ):
      QCustomEvent.__init__( self, UPDATE_LOG_EVENT )
      self.text = text


class JB_SetButtons_CustomEvent( QCustomEvent ):
   def __init__( self, flag ):
      QCustomEvent.__init__( self, JB_TABWIDGET_SETBUTTONS_EVENT )
      self.flag = flag


class Job_SetWidgets_CustomEvent( QCustomEvent ):
   def __init__( self, enable, widgetList ):
      QCustomEvent.__init__( self, JOB_SETWIDGETS_EVENT )
      self.enable = enable
      self.widgetList = widgetList


class UpdateProgress_CustomEvent( QCustomEvent ):
   def __init__( self, progress ):
      QCustomEvent.__init__( self, UPDATE_PROGRESS_EVENT )
      self.progress = progress


class JB_CloseTab_CustomEvent( QCustomEvent ):
   def __init__( self, id_str, quick ):
      QCustomEvent.__init__( self, JB_TABWIDGET_CLOSETAB_EVENT )
      self.id_str = id_str
      self.quick = quick


class CMDialog_CustomEvent( QCustomEvent ):
   def __init__( self, credObj ):
      QCustomEvent.__init__( self, CMDIALOG_EVENT )
      self.credObj = credObj


class ActionProgress_CustomEvent( QCustomEvent ):
   def __init__( self, action, progressTuple ):
      QCustomEvent.__init__( self, ACTION_PROGRESS_EVENT )
      self.action = action
      self.progressTuple = progressTuple


class RemoveAction_CustomEvent( QCustomEvent ):
   def __init__( self, action ):
      QCustomEvent.__init__( self, REMOVE_ACTION_EVENT )
      self.action = action
