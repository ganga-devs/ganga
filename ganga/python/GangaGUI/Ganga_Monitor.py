import os, sys, shutil, Queue
import qt
from GangaGUI.Ganga_Monitor_Panel_BASE import Ganga_Monitor_Panel_BASE
from Ganga.Core.MonitoringComponent.Monitoring import MonitoringClient
from GangaGUI.customTables import *
from GangaGUI.customEvents import *
from GangaGUI import miscDialogs, customDialogs, inspector, miscQtToolbox, Ganga_Errors
from GangaGUI.cfg_manager import GUIConfig
from GangaGUI.widget_set import getGUIConfig

# Setup logging ---------------
import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger( "Ganga.Ganga_Monitor" )

class Ganga_Monitor( Ganga_Monitor_Panel_BASE ):
   def __init__( self, parent = None, name = None, fl = qt.Qt.WDestructiveClose, actionsDict = {} ):
      Ganga_Monitor_Panel_BASE.__init__( self, parent, name, fl )
      self.actionsDict = actionsDict
      self.__jobActionInProgressDict = {}
      self.__guiParent = self.actionsDict[ '__PARENT__' ]
      # Get dictionary of jobs and status
      self.cachedJobStatus = {}
      self.diffCachedJobStatus = {}
      self.diffQueue = Queue.Queue()
      self.jobFields = self.__checkJobFields( GUIConfig.getList( 'Job_Monitor_Fields' ) )
      self.__statusPos = self.getStatusPos()
      self.__initialRun = True
      self.jobCache = {}

      # Adding customised listviews (jobs and logical folders) and table (filter) to the Ganga Monitoring panel.
      self.listView_Jobs = Jobs_ListView( self.frame_JobsListView, self.actionsDict, self.createExportActions )
      self.frame_JobsListView.layout().addWidget( self.listView_Jobs )
      self.listView_Jobs.show()
      self.listView_LogicalFolders = LogicalFolders_ListView( self.frame_LogicalFolders, self.actionsDict )
      self.frame_LogicalFolders.layout().addWidget( self.listView_LogicalFolders )
      self.listView_LogicalFolders.show()

      _cs = [ x[2] for x in GUIConfig.getList( 'Job_Monitor_Fields' ) ]
      self.changeHeaderDialog = customDialogs.ChangeMonitoringFieldsDialog( componentDict = self.__getAttributeSelectionTableDict(), currentSelection = _cs, parent = self.listView_Jobs, caption = 'Select fields to actively monitor:' )
      del _cs
      
      # Adding action progress dialog
      self.actionProgressDialog = customDialogs.ActionProgressDialog( self )

      # Adding dummy action group for the Extras action.
      self.actionsDict[ 'Extras' ] = self.__createEmptyExtrasActionGroup()
      self.__emSlotList = [] # keep reference to emSlot functions (and connections) alive.

      # Connections for job monitoring table
      self.connect( self.listView_Jobs.header(),
                    qt.SIGNAL( "indexChange( int, int, int )" ),
                    self.slotUpdateHeaderSequence )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "selectionChanged()" ),
                    self.slotDisplayJob )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "collapsed( QListViewItem* )" ),
                    self.slotDisplayJob )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "expanded( QListViewItem* )" ),
                    self.slotExpandItem )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "collapsed( QListViewItem* )" ),
                    self.slotCollapseItem )
      self.connect( self.listView_Jobs, 
                    qt.SIGNAL( "doubleClicked( QListViewItem*, const QPoint&, int )" ), 
                    self.slotForceDisplayJob )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "returnPressed( QListViewItem* )" ),
                    self.slotOpenJob )
      self.connect( self.pushButton_Find,
                    qt.SIGNAL( "clicked()" ),
                    self.slotFind )
      self.connect( self.lineEdit_Find,
                    qt.SIGNAL( "textChanged( const QString& )" ),
                    self.slotCheckFindText )
      self.connect( self.toolButton_Refresh,
                    qt.SIGNAL( "clicked()" ),
                    self.slotForceFullRefresh )
      self.connect( self.toolButton_ShowJobDetails, 
                    qt.SIGNAL( "clicked()" ), 
                    self.listView_Jobs, 
                    qt.SIGNAL( "selectionChanged()" ))

      # Connections for job monitoring context menu
      self.connect( self.__guiParent, qt.PYSIGNAL( "openJob()" ), self.slotOpenJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "saveAsTemplateJob()" ), self.slotSaveAsTemplateJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "copyJob()" ), self.slotCopyJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "removeJob()" ), self.slotRemoveJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "killJob()" ), self.slotKillJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "submitJob()" ), self.slotSubmitJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "copySubmitJob()" ), self.slotCopySubmitJob )
      self.connect( self.__guiParent, qt.PYSIGNAL( "currentJobChanged(source,id_str)"), self.slotChangeSelection )
      self.connect( self.__guiParent, qt.PYSIGNAL( "retrieveOutput()"), self.slotOutputRetrieve )
      self.connect( self.__guiParent, qt.PYSIGNAL( "headerChange()" ), self.slotHeaderChange )
      

      # Connections for logical folder
      self.connect( self.listView_LogicalFolders,
                    qt.SIGNAL( "selectionChanged()" ),
                    self.slotLFDisplayJob )

      self.frame_Left.hide()
      self.listView_Jobs.updateLVHeaders( self.jobFields )

      self._JM = MonitoringClient( Ganga.Core.monitoring_component )
      self._JM.makeUpdateJobStatusFunction( self.diffCalculation )
      self._JM.bindClientFunction( self.listView_Jobs.updateLV_Helper, { 'jobStatusDictFunc' : self.__getJobStatus } )
   
   def minimumSizeHint( self ):
      return qt.QSize( 800, 400 )
   
   def isJobActionInProgress( self, job ):
      try:
         currAction = self.__jobActionInProgressDict[ job ]
      except KeyError:
         return False
      miscDialogs.infoMessage( None, "Job %s is currently executing a '%s' action. Try again later." % ( job.id, currAction ) )
      return True

   def __getAttributeSelectionTableDict( self ):
      componentDict = {}
      componentDict[ 'application' ] = {}
      for x in inspector.plugins( 'applications' ):
         componentDict[ 'application' ].update( { x : getGUIConfig( eval( 'inspector.' + x ), "Job." + x ) } )
      componentDict[ 'backend' ] = {}
      for x in inspector.plugins( 'backends' ):
         componentDict[ 'backend' ].update( { x : getGUIConfig( eval( 'inspector.' + x ), "Job." + x ) } )
      componentDict[ 'Job' ] = filter( lambda x : not ( x[ 'objPrefix' ].startswith( 'Job.application' ) and x[ 'objPrefix' ].startswith( 'Job.backend' ) ), getGUIConfig( inspector.Job ) )
      return componentDict
   
   def __getCurrentSelected( self ):
      return qt.QListViewItemIterator( self.listView_Jobs, qt.QListViewItemIterator.Selected ).current()

   def __getSelection( self, selectionCriteria = 0 ):
      """
      Selection Criteria:
      0 -- every single item selected (i.e. master jobs and subjobs)
      1 -- master jobs only
      2 -- subjobs only
      3 -- return independent selections.
           if master is selected, ignore all its subjobs (even they are selected).
      """
      # Load ids of entire selection
      _selection = []
      itemsSelected = qt.QListViewItemIterator( self.listView_Jobs, qt.QListViewItemIterator.Selected )
      currentItem = itemsSelected.current()
      while currentItem:
         parentItem = currentItem.parent()
         if parentItem: # currentItem is a subjob
            _selection.append( ( int( str( parentItem.text( 0 ) ) ), int( str( currentItem.text( 0 ) ) ) ) )
         else: # currentItem is a master job
            _selection.append( ( int( str( currentItem.text( 0 ) ) ), None ) )
         itemsSelected += 1
         currentItem = itemsSelected.current()

      # filter selection based on selection criteria
      if selectionCriteria == 0:
         return _selection
      elif selectionCriteria == 1:
         return filter( lambda x: x[1] is None, _selection )
      elif selectionCriteria == 2:
         return filter( lambda x: x[1] is not None, _selection )
      elif selectionCriteria == 3:
         filteredSelection = []
         lastMasterId = -1
         for masterId, childId in _selection:
            if childId == None: # id_str is id of master job
               lastMasterId = masterId
            elif lastMasterId != masterId:
               lastMasterId = -1 # reset _masterId
            else:
               continue
            filteredSelection.append( ( masterId, childId ) )
         return filteredSelection
      else:
         return []

   def slotUpdateHeaderSequence( self, section, fromIndex, toIndex ):
      _h = self.listView_Jobs.header()
      cfLabels = [ x[0] for x in self.jobFields ]
      GUIConfig.setList( [ self.jobFields[ cfLabels.index( str( _h.label( _h.mapToSection( hIndex ) ) ) ) ] for hIndex in xrange( _h.count() ) ], 'Job_Monitor_Fields', force = True )

   def slotHeaderChange( self ):
      if self.changeHeaderDialog.exec_loop() == qt.QDialog.Accepted:
         self.setFields( fieldList = self.changeHeaderDialog.selectionTable.getSelection() )
         self.slotForceFullRefresh()

   def slotForceRefresh( self ):
      self._JM.update()
   
   def slotForceFullRefresh( self ):
      self.listView_Jobs.updateLVHeaders( self.jobFields )
      self.cachedJobStatus.clear()
      self.slotForceRefresh()

   def __createActionDescription( self, job, action ):
      if job.master is None:
         return "Job %s: %s" % ( job.id, action.function.__name__ )
      return "Job %s.%s: %s" % ( job.master, job.id, action.function.__name__ )

   def _killJob( self, job ):
      if self.isJobActionInProgress( job ):
         return
      else:
         self.__jobActionInProgressDict[ job ] = "kill"
      
      _action = inspector.JobAction( function = job.kill )
      _action.description = self.__createActionDescription( job, _action )

      def cb():
         self.actionProgressDialog.removeAPEntry_Helper( _action )
         del self.__jobActionInProgressDict[ job ]

      _action.callback_Success = cb
      _action.callback_Failure = cb
      self.actionProgressDialog.addAPEntry( _action )
      try:
         inspector.queueJobAction( _action )
      except:
         cb()
   
   def slotKillJob( self ):
      for masterId, childId in self.__getSelection( selectionCriteria = 3 ):
         if childId is None:
            _j = inspector.jobs( masterId )
         else:
            _j = inspector.jobs( masterId ).subjobs( childId )
         if _j and _j.status in [ 'submitted', 'running' ]: #, 'completing' ]:
            if not childId and self.__guiParent.jobBuilder.isJobTabOpen( str( masterId ) ):
               self.__guiParent.jobBuilder.slotKillJob( str( masterId ) )
            else:
               self._killJob( _j )
      # refresh action not placed inside _submitJob method
      # to ensure a single update for each bulk action.
      self.slotForceRefresh()
      self.slotDisplayJob()

   def slotOpenJob( self, *args):
      for masterId, _ in self.__getSelection( selectionCriteria = 1 ):
         _j = inspector.jobs( masterId )
         if _j:
            self.__guiParent.jobBuilder.slotNewJob( existingJob = _j, makeCopy = False )
      self.slotForceRefresh()
   
   def slotChangeSelection( self, source, id_str ):
      if source is self:
         return
      if id_str in self.listView_Jobs.indexDict:
         item = self.listView_Jobs.indexDict[ id_str ]
         self.listView_Jobs.clearSelection()
         self.listView_Jobs.setCurrentItem( item )
         self.listView_Jobs.setSelected( item, True )
         self.listView_Jobs.ensureItemVisible( item )

   def slotForceDisplayJob( self, item, p, q ):
      self.slotDisplayJob( item, True )

   def slotDisplayJob( self, item = None, show = None ):
      self.setFindAgainButton()
      if show is None:
         show = False
      if self.frame_Right.isHidden():
         if show:
            self.toolButton_ShowJobDetails.setOn( True )
            self.frame_Right.setShown( True )
         else:
            return
      if item is None:
         item = self.__getCurrentSelected()
      if not item:
         self.textEdit_JobDetails.clear()
         return
      jobDetails = ''
      currentItemID = int( str( item.text( 0 ) ) )
      if item.parent() is None:
         _id = currentItemID
         jobDetails = inspector.jobs( _id ).__str__()
      else:
         _id = int( str( item.parent().text( 0 ) ) )
         for sjob in inspector.jobs( _id ).subjobs:
            if sjob.id == currentItemID:
               jobDetails = sjob.__str__()
               break
         currentItemID = str( _id ) + ':' + str( currentItemID )
      
      self.emit( qt.PYSIGNAL( "currentJobChanged(source,id_str)" ), ( self, str( currentItemID ) ) )
      self.textEdit_JobDetails.setText( jobDetails )

   def slotExpandItem( self, expandedItem, update = True ):
      self.listView_Jobs.expandedJobs[ str( expandedItem.text( 0 ) ) ] = None
      if update:
         self._JM.update()

   def slotAddExpandedItem( self, expandedItem ):
      self.slotExpandItem( expandedItem, False )

   def slotCollapseItem( self, collapsedItem ):
      del self.listView_Jobs.expandedJobs[ str( collapsedItem.text( 0 ) ) ]

   def _removeJob( self, job ):
      if self.isJobActionInProgress( job ):
         return
      else:
         self.__jobActionInProgressDict[ job ] = "remove"

      _action = inspector.JobAction( function = job.remove )
      _action.description = self.__createActionDescription( job, _action )

      def cb():
         self.actionProgressDialog.removeAPEntry_Helper( _action )
         del self.__jobActionInProgressDict[ job ]
      
      _action.callback_Success = cb
      _action.callback_Failure = cb
      self.actionProgressDialog.addAPEntry( _action )
      try:
         inspector.queueJobAction( _action )
      except:
         cb()
   
   def slotRemoveJob( self ):
      for masterId, _ in self.__getSelection( selectionCriteria = 1 ):
         _j = inspector.jobs( masterId )
         if _j:
            try:
               del self.listView_Jobs.expandedJobs[ str( masterId ) ]
            except:
               pass
            if self.__guiParent.jobBuilder.isJobTabOpen( str( masterId ) ):
               self.__guiParent.jobBuilder.slotRemoveJob( str( masterId ) )
            else:
               self._removeJob( _j )
      # refresh action not placed inside _submitJob method
      # to ensure a single update for each bulk action.
      self.slotForceRefresh()
      self.slotDisplayJob( '' ) # Force clearing of display.

   def slotSaveAsTemplateJob( self ):
      __nameAdded = False
      for masterId, childId in self.__getSelection( selectionCriteria = 0 ):
         if childId is None:
            _j = inspector.jobs( masterId )
         else:
            _j = inspector.jobs( masterId ).subjobs( childId )
         if _j:
            if childId is None and self.__guiParent.jobBuilder.isJobTabOpen( str( masterId ) ):
               self.__guiParent.jobBuilder.slotSaveAsTemplateJob( str( masterId ) )
            else:
               _jt = inspector.JobTemplate( _j )
               _name, _ok = qt.QInputDialog.getText( "Creating new template from Job %s" % _j.id, "Name of new template", qt.QLineEdit.Normal, "New Job %s Template" % _j.id, self, '' )
               if _ok and _name:
                  _jt.name = str( _name )
                  __nameAdded = True
               self.__guiParent.createNewTemplateAction( _jt )
      if __nameAdded:
         self.slotForceRefresh()

   def slotCopyJob( self ):
      for masterId, childId in self.__getSelection( selectionCriteria = 0 ):
         if childId is None:
            _j = inspector.jobs( masterId )
         else:
            _j = inspector.jobs( masterId ).subjobs( childId )
         if _j:
            self.__guiParent.jobBuilder.slotNewJob( existingJob = _j, makeCopy = True )

   def slotCopySubmitJob( self ):
      for masterId, childId in self.__getSelection( selectionCriteria = 3 ):
         if childId is None:
            _j = inspector.jobs( masterId )
         else:
            _j = inspector.jobs( masterId ).subjobs( childId )
         self._submitJob( _j.copy() )
   
   def _submitJob( self, job ):
      if self.isJobActionInProgress( job ):
         return
      else:
         self.__jobActionInProgressDict[ job ] = "submit"

      _action = inspector.JobAction( function = job.submit )
      _action.description = self.__createActionDescription( job, _action )

      def cb():
         self.actionProgressDialog.removeAPEntry_Helper( _action )
         del self.__jobActionInProgressDict[ job ]

      _action.callback_Success = cb
      _action.callback_Failure = cb
      self.actionProgressDialog.addAPEntry( _action )
      try:
         inspector.queueJobAction( _action )
      except:
         cb()

   def slotSubmitJob( self ):
      for masterId, _ in self.__getSelection( selectionCriteria = 1 ):
         _j = inspector.jobs( masterId )
         if _j and _j.status == 'new':
            if self.__guiParent.jobBuilder.isJobTabOpen( str( masterId ) ):
               self.__guiParent.jobBuilder.slotSubmitJob( str( masterId ) )
            else:
               self._submitJob( _j )
      # refresh action not placed inside _submitJob method
      # to ensure a single update for each bulk action.
      self.slotForceRefresh()

   def __checkJobFields( self, jobFields ):
      _JOBFIELDS = [ ( 'id', 'id', 'Job.id' ), 
                     ( 'status', 'status', 'Job.status' ), 
                     ( 'name', 'name', 'Job.name' ),
                     ( 'application', 'application', 'Job.application' ), 
                     ( 'exe filename', 'application.exe', 'Job.Local.exe' ), 
                     ( 'backend', 'backend', 'Job.backend' ), 
                     ( 'backend id', 'backend.id', 'Job.Local.id' ) ]
      if not isinstance( jobFields, list ):
         log.warning( "Job fields from GUI config file corrupt. Using default." )
         return _JOBFIELDS
      currFields = []
      for jobField in jobFields:
         if not isinstance( jobField, tuple ) or len( jobField ) != 3:
            log.warning( "Job fields from GUI config file corrupt. Using default." )
            return _JOBFIELDS
         else: # General format correct
            currFields.append( jobField[ 1 ] )
      if 'status' not in currFields:
         jobFields[ :0 ] = [ ( 'status', 'status', 'Job.status' ) ]
         currFields[ :0 ] = [ 'status' ] # update currFields to get correct id pos.
         log.warning( "Job status missing amongst the GUI config file's job fields. Re-inserting..." )
      try:
         idPos = currFields.index( 'id' )
      except ValueError:
         idPos = 0
         jobFields[ :0 ] = [ ( 'id', 'id', 'Job.id' ) ]
         log.warning( "Job id missing amongst the GUI config file's job fields. Re-inserting..." )
      else:
         if idPos: # id not at position 0.
            jobFields[ :0 ] = [ jobFields.pop( idPos ) ] # reinsert id to pos 0.
            log.debug( "Job id position in GUI config file's job fields is wrong. Should be the first field. Correcting..." )
      return jobFields

   def __constructCmdList( self, shellCmd, cmd, argList, targetFile ):
      cList = []
      if sys.platform == 'darwin':
         if shellCmd:
            cList.extend( shellCmd.split( ' ' ) )
         if shellCmd != 'open':
            cList.extend( cmd.split( ' ' ) )
            cList.extend( argList )
      else:
         cList.extend( shellCmd.split( ' ' ) )
         cList.extend( cmd.split( ' ' ) )
         cList.extend( argList )
      cList.append( targetFile )
      return filter( bool, cList )

   def slotOutputRetrieve( self ):
      _s = self.__getSelection( selectionCriteria = 0 )
      if len( _s ) == 0:
         return
      elif len( _s ) > 1:
         log.warning( "Retrieving output for last item in selection only." )
      masterId, childId = _s[ -1 ] # pick only the last in the selection
      if childId is None:
         _j = inspector.jobs( masterId )
      else:
         _j = inspector.jobs( masterId ).subjobs( childId )
      if _j:
         _retrieveOutputDialog = customDialogs.RetrieveJobOutputDialog( dirName = _j.outputdir, fileTypeFilter = "", parent = self, name = 'Retrieve Output Dialog', modal = True )
         while _retrieveOutputDialog.notDone:
            if _retrieveOutputDialog.exec_loop() == qt.QDialog.Accepted:
               _sFiles = list( _retrieveOutputDialog.selectedFiles() )
               if _retrieveOutputDialog.dirSelected:
                  if _sFiles:
                     _errMsgs = ''
                     for _f in _sFiles:
                        _f = str( _f )
                        try:
                           shutil.copy2( _f, _retrieveOutputDialog.dirSelected )
                        except Exception, msg:
                           _errMsgs += "\n%s\n    --> %s\n" % ( _f, msg )
                           continue
                     if _errMsgs:
                        miscDialogs.warningMessage( self, "Error encountered copying the following file(s):\n%s" % _errMsgs )
                     else:
                        _retrieveOutputDialog.notDone = False
                  else:
                     miscDialogs.warningMessage( self, "Please selected at least one file to open." )
               else: # Open file instead of retrieve
                  if len( _sFiles ) > 1: 
                     miscDialogs.warningMessage( self, "Please select a single file to open." )
                  else: # Open the selected file
                     _sFile = str( _sFiles[0] )
                     _root, _ext = os.path.splitext( _sFile )
                     _ext = _ext.replace( '.', '' )
                     _fa = GUIConfig.getDict( 'File_Association' )
                     _openWith = ''
                     if _ext in _fa:
                        _openWith, _gui = _fa[ _ext ]
                     else:
                        try:
                           _openWith, _gui = _fa[ '' ]
                        except KeyError:
                           reply = 1
                        else:
                           reply = miscDialogs.questionDialog( None, "Open %s file using %s?" % ( _ext, _openWith ) )
                           if reply == 2: # Cancel
                              return

                        if reply == 1:
                           # Browse for suitable executable to open file with.
                           _openWith = str( qt.QFileDialog.getOpenFileName( "", "", self, "BrowseExe", "Choose an executable:" ) )
                           _gui = True # Assume it's a gui application to avoid asking.
                     if _openWith:
                        if _gui:
                           _sc = ''
                        else:
                           _sc = GUIConfig.getString( option = 'Shell_Command' )
                        self.__proc = miscQtToolbox.ExtProcess( cmdList = self.__constructCmdList( _sc, _openWith, [], _sFile ),
                                                                stdout = self.__guiParent.log, 
                                                                stderr = self.__guiParent.log )
                        if self.__proc.launch( '' ):
                           _retrieveOutputDialog.notDone = False
                           if _ext not in _fa:
                              GUIConfig.add_to_dict( { _ext: [ _openWith, _gui ] }, 'File_Association' )
                        else:
                           miscDialogs.warningMessage( self, "Failed to start %s" % _openWith )
            else:
               _retrieveOutputDialog.notDone = False

   def slotFind( self ):
      findAgain = str( self.pushButton_Find.text() ) == 'Find Again'

      self.disconnect( self.listView_Jobs,
                       qt.SIGNAL( "expanded( QListViewItem* )" ),
                       self.slotExpandItem )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "expanded( QListViewItem* )" ),
                    self.slotAddExpandedItem )
      # expand all jobs to ensure that all items are traversed.
      if not findAgain: # new find
         self._JM.update()
      self.setFindAgainButton( self.listView_Jobs.find( self.lineEdit_Find.text(),   findAgain ) )
      self.disconnect( self.listView_Jobs,
                       qt.SIGNAL( "expanded( QListViewItem* )" ),
                       self.slotAddExpandedItem )
      self.connect( self.listView_Jobs,
                    qt.SIGNAL( "expanded( QListViewItem* )" ),
                    self.slotExpandItem )

   def slotCheckFindText( self, findText ):
      self.setFindAgainButton()
      self.pushButton_Find.setEnabled( bool( str( findText ) ) )

   def slotLFDisplayJob( self ):
      i = self.listView_LogicalFolders.selectedItem()
      if i is not None and i.isJob:
         item = self.listView_Jobs.indexDict[ str( i.text( 0 ) ) ]
      else:
         item = ''
      self.slotDisplayJob( item )

   def setFindAgainButton( self, setToFindAgain = False ):
      if setToFindAgain:
         self.pushButton_Find.setText( "Find Again" )
      else:
         self.pushButton_Find.setText( "Find" )

   def getStatusPos( self ):
       for x in xrange( len( self.jobFields ) ):
           if self.jobFields[x][1] == 'status':
               return x
       log.error( "Status position not found!!!" )
       return None

   def setFields( self, fieldList ):
       if isinstance( fieldList, list ):
           for i in xrange( len( self.jobFields ) - 1, -1, -1 ):
               try:
                   pos = fieldList.index( self.jobFields[ i ] )
               except ValueError:
                   del self.jobFields[ i ]
               else:
                   self.jobFields[ i ] = fieldList.pop( pos )
           self.jobFields.extend( fieldList )
           self.cachedJobStatus.clear()
           self.__statusPos = self.getStatusPos()
           GUIConfig.setList( self.jobFields, 'Job_Monitor_Fields', force = True )

   def diffCalculation( self ):
       """This method populates two internal dictionaries __cachedJobStatus and
       __diffCachedJobStatus. The former dictionary contains the latest job status (returned 
       only at client connection and client forced update) and the latter keeps the changes 
       (returned automatically to subscriber clients when there are changes) since the last 
       update. """
       def makeJobDetailDict( j ):
           _content = []
           _meta = {}
           for _, _command, _ in self.jobFields[1:]:
               _content1 = j
               for x in _command.split( '.' ):
                   try:
                       _content1 = getattr( _content1, x )
                   except AttributeError:
                       _content1 = "*N/A*"
                       break
               try:
                   _content1 = _content1._name
               except AttributeError:
                   pass
               _content.append( str( _content1 ) )
           if j.master:
              _meta[ 'master' ] = str( j.master.id )
           else:
              if len( j.subjobs ):
                 _meta[ 'subjobs' ] = True
              else:
                 _meta[ 'subjobs' ] = False
           return { 'content': _content, 'meta': _meta }
       
       def processDiff( j ):
           if j.master: # j is a subjob
               jid = "%s.%s" % ( j.master.id, j.id )
           else:
               jid = str( j.id )
#           jid = str( j.id )
           _old = {}
           _new = makeJobDetailDict( j )
           try:
               _old = self.cachedJobStatus[ jid ]
           except KeyError:
               # New Entry
               log.debug( "New entry %s has value %s." % ( jid, _new ) )
               self.cachedJobStatus[ jid ] = _new
               self.diffCachedJobStatus[ jid ] = _new
               return
               
           # Entry exists but status has changed.
           if _new != _old: # Check all fields
               log.debug( "Existing entry %s has value changed to %s." % ( jid, _new ) )
               self.cachedJobStatus[ jid ] = _new
               self.diffCachedJobStatus[ jid ] = _new

       self.jobCache.clear()
       activeBackends = {}
       oldkeys = self.cachedJobStatus.keys()
       # Inititate internal Ganga job registry update of job status
       for j in self._JM._getRegistry():
           jid = str( j.id )
           try:
               # Remove current entry from the old list as these still exists.
               # Only remaining entries in the old list will be
               # marked for removal later.
               oldkeys.remove( jid )
           except:
               # First iteration (i.e. self.cachedJobStatus=={}), 
               # new job or possible rouge job? 
               self.jobCache[ j ] = None
           # Remove subjob keys from oldkeys for subjobs 
           # currently being monitored
           if jid in self.listView_Jobs.expandedJobs:
               statusFinal = True
               for sjob in j.subjobs:
                   try:
#                       oldkeys.remove( str( sjob.id ) )
                       oldkeys.remove( "%s.%s" % ( jid, sjob.id ) )
                   except:
                       # Current job expansion was just initiated i.e. subjobs
                       # are not visible. set statusFinal to false to force
                       # current job to be added to the job cache.
                       statusFinal = False
                       continue
                   else:
                       # Check is the subjob is in a non terminal state.
                       if sjob.status in [ 'new', 'submitted', 'running' ]: #, 'completing' ]:
                           statusFinal = False
               # There are still subjobs running so add 
               # the master job to the job cache.
               if not statusFinal:
                   self.jobCache[ j ] = None 
                           
           if j.status in [ 'submitted', 'running' ]: #, 'completing' ]:
               bn = j.backend._name
               activeBackends.setdefault( bn, [] )
               activeBackends[ bn ].append( j )
               self.jobCache[ j ] = None
           else:
               # job status may have changed (e.g. to killed)
               if not self.cachedJobStatus.has_key( jid ):
                  continue
               if self.cachedJobStatus[ jid ][ 'content' ][ self.__statusPos - 1 ] != j.status:
                   log.debug( "Job %s has status %s but cache has %s. Adding job to be refreshed." % ( jid, j.status, self.cachedJobStatus[ jid ][ 'content' ][ self.__statusPos - 1 ] ) )
                   self.jobCache[ j ] = None
   
       # Mark entries that have disappeared in self.diffCachedJobStatus
       # Remove these entries from self.cachedJobStatus
       self.diffCachedJobStatus.clear()
       for x in oldkeys:
           del self.cachedJobStatus[ x ]
           self.diffCachedJobStatus[ x ] = {}
       
       if oldkeys:
           log.debug( "Removed non-existing job entries: %s" % oldkeys )
   
       # Monitoring component job status update
       for j in self.jobCache:
           if str( j.id ) in self.listView_Jobs.expandedJobs:
               for sjob in j.subjobs:
                   processDiff( sjob )
           processDiff( j )
       if self.diffCachedJobStatus:
           self.diffQueue.put( self.diffCachedJobStatus.copy() )
       return activeBackends
   
   def __getJobStatus( self ):
      if self.__initialRun:
         self.__initialRun = False
         return self.cachedJobStatus
      try:
          return self.diffQueue.get( False )
      except Queue.Empty:
          return {}

   def __customisedActionSlot( self, emObj, emObjStr, methodClass_name ):
      def _f():
         argumentDialog = customDialogs.EMArguments_Dialog( self.listView_Jobs, emObj, emObjStr, methodClass_name )
         while True:
            if not argumentDialog.argumentList:
               response = qt.QDialog.Accepted
            else:
               response = argumentDialog.exec_loop()
            if response == qt.QDialog.Accepted:
               try:
                  func, args = argumentDialog.getExportMethod()
                  func( **args )
               except Ganga_Errors.ArgumentException, x:
                  miscDialogs.warningMessage( self, x )
                  continue
               else:
#               job = func.im_self.getJobObject()
#               if self.isJobActionInProgress( job ):
#                  return
#               else:
#                  self.__jobActionInProgressDict[ job ] = func.__name__
#               
#               _action = inspector.JobAction( function = func, kwargs = args )
#               _action.description = self.__createActionDescription( job, _action )
#         
#               def cb():
#                  self.actionProgressDialog.removeAPEntry_Helper( _action )
#                  del self.__jobActionInProgressDict[ job ]
#         
#               _action.callback_Success = cb
#               _action.callback_Failure = cb
#               self.actionProgressDialog.addAPEntry( _action )
#               try:
#                  inspector.queueJobAction( _action )
#               except:
#                  cb()
                  break
            elif response == qt.QDialog.Rejected:
               break
      return _f

   def __createEmptyExtrasActionGroup( self ):
      # Creating export methods action group
      extrasActionGroup = qt.QActionGroup( self.listView_Jobs, "extrasActionGroup" )
      extrasActionGroup.setText( qt.QString( 'Extras' ) )
      extrasActionGroup.setMenuText( qt.QString( 'Extras' ) )
      extrasActionGroup.setUsesDropDown( True )
      return extrasActionGroup

   def createExportActions( self ):
      del self.__emSlotList[:]
      _s = self.__getSelection( selectionCriteria = 0 )
      self.actionsDict[ 'Extras' ] = self.__createEmptyExtrasActionGroup()
      if len( _s ) != 1:
         return
      for masterId, childId in _s:
         if childId is None:
            _job = inspector.jobs( masterId )
         else:
            _job = inspector.jobs( masterId ).subjobs( childId )
      emDict = inspector.getExportMethods( _job )
      for _category in emDict:
         for exportMethod in emDict[ _category ]:
            emAction = qt.QAction( self.actionsDict[ 'Extras' ], "%sAction" % exportMethod )
            emAction.setText( qt.QString( "Job.%s.%s" % ( _category, exportMethod ) ) )
            emAction.setMenuText( qt.QString( "Job.%s.%s" % ( _category, exportMethod ) ) )
            emSlot = self.__customisedActionSlot( eval( "_job.%s.%s" % ( _category, exportMethod ) ), exportMethod, eval( "_job.%s.__class__.__name__" % _category ) )
            self.__emSlotList.append( emSlot )
            self.connect( emAction, qt.SIGNAL( "activated()" ), emSlot )
