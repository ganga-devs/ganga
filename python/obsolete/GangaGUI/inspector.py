# Inspector
# Provides access to Ganga objects for inspection from
# a single module namespace. (historical)
#
# *NEW* Thread pool to perform Ganga functions e.g. submit

import threading, Queue
import Ganga
from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobAction
from Ganga.GPI import *

THREAD_POOL_SIZE = 5

Qin = Queue.Queue()
ThreadPool = {} #[]
ThreadPool_RLock = threading.RLock()

def _execJobAction():
   while True:
      action = Qin.get()
      if not isinstance( action, JobAction ):
         continue
      if action.function == 'stop':
         break
      ThreadPool_RLock.acquire()
      ThreadPool[ threading.currentThread() ] = action
      ThreadPool_RLock.release()
      try:
         result = action.function( *action.args, **action.kwargs )
      except:
         ThreadPool_RLock.acquire()
         ThreadPool[ threading.currentThread() ] = None
         ThreadPool_RLock.release()
         action.callback_Failure()
#         Qerr.put( sys.exc_info()[ :2 ] )
      else:
         ThreadPool_RLock.acquire()
         ThreadPool[ threading.currentThread() ] = None
         ThreadPool_RLock.release()
         if result in action.success:
            action.callback_Success()
         else:
            action.callback_Failure()

def queueJobAction( jobAction ):
   Qin.put( jobAction )

def _makeThreadPool( threadPoolSize = THREAD_POOL_SIZE, daemonic = True ):
   for i in range( THREAD_POOL_SIZE ):
      t = threading.Thread( name = "GANGA_Job_Thread_%s" % i, 
                            target = _execJobAction )
      t.setDaemon( daemonic )
#      ThreadPool.append( t )
      ThreadPool[ t ] = None
      t.start()

def stop_and_free_thread_pool():
   for i in range( len( ThreadPool ) ):
      queueJobAction( JobAction( 'stop' ) )
   for t in ThreadPool.iterkeys():
      t.join( 5 )
   ThreadPool.clear()
#   for t in ThreadPool:
#      t.join(5)
#   del ThreadPool[:]

_makeThreadPool()

def getExportMethods( job ):
   def validExportMethods( obj ):
      try:
         return filter( lambda x:hasattr( obj, x ), obj._impl._exportmethods )
      except:
         return []
   exportMethods = {}
   for attrName in job._impl._schema.datadict.keys():
      attr = getattr( job, attrName )
      itemEMList = validExportMethods( attr )
      if itemEMList:
         exportMethods[ attrName ] = itemEMList
   return exportMethods