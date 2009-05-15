import inspect
from GangaGUI.Ganga_Jobs_TabWidget_BASE import Ganga_Jobs_TabWidget_BASE
from GangaGUI.customDialogs import EMArguments_Dialog
from GangaGUI.Ganga_Job import Ganga_Job
from GangaGUI import miscDialogs, Ganga_Errors
from GangaGUI.cfg_manager import GUIConfig
from GangaGUI.customEvents import *
import qt

# PyQt Hack. Defined in the event Qt.Dock does not exist.
DOCK = { 0: qt.Qt.DockUnmanaged,
         1: qt.Qt.DockTornOff,
         2: qt.Qt.DockTop,
         3: qt.Qt.DockBottom,
         4: qt.Qt.DockRight,
         5: qt.Qt.DockLeft,
         6: qt.Qt.DockMinimized }

closeTabImage_data = \
    "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d" \
    "\x49\x48\x44\x52\x00\x00\x00\x08\x00\x00\x00\x08" \
    "\x08\x06\x00\x00\x00\xc4\x0f\xbe\x8b\x00\x00\x00" \
    "\x96\x49\x44\x41\x54\x78\x9c\x4d\xce\xa1\x6e\xc2" \
    "\x00\x00\x84\xe1\x6f\x4b\x91\xcc\x6d\xbe\x1e\x8b" \
    "\x43\xf1\x00\x13\xf0\x08\x98\x3e\x09\xc9\x1e\x61" \
    "\x7d\x8c\xf9\x21\x91\x78\xc4\x5c\xb3\x05\x03\x21" \
    "\xa9\xec\x72\x88\x96\x14\x71\xb9\xdc\x5d\x72\xf9" \
    "\xe1\x09\xc1\x2b\x8a\x24\x50\x0c\xdd\x14\xd2\x6d" \
    "\x36\xf9\xeb\x8b\x12\x93\xff\xba\x4e\xdb\xe7\xc0" \
    "\xdb\x0f\xb9\x90\x33\x69\xab\x2a\x17\xb2\xea\xc7" \
    "\xf7\xfb\x5d\xd9\x90\xdf\x41\x9f\xfd\xb8\x46\xf9" \
    "\x9c\xa4\x43\xa3\xaa\x5c\x71\xc5\x1c\x38\xa2\x81" \
    "\xe2\x54\xd7\x39\x90\x0f\xb2\x27\x07\xb2\x1b\x99" \
    "\xe4\x9b\x2c\xc7\xdb\xd9\x17\xd9\x3e\x40\xbe\x18" \
    "\x81\x4a\x4c\x06\x0f\x16\x37\x3c\x29\x46\x05\x3b" \
    "\xd1\xea\x1b\x00\x00\x00\x00\x49\x45\x4e\x44\xae" \
    "\x42\x60\x82"


class Ganga_Jobs_TabWidget( qt.QDockWindow ):
   def __init__( self, parent = None, name = None, fl = 0 ):
      qt.QDockWindow.__init__( self, parent, name, fl )
      self._parent = parent
      self.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Preferred, qt.QSizePolicy.Preferred, 0, 0, self.sizePolicy().hasHeightForWidth() ) )
      self.setResizeEnabled( True )
      self.setNewLine( True )
      self.setOpaqueMoving( False )
      self.setCaption( qt.QString( "Job builder" ) )

      self.__closeTabImage = qt.QPixmap()
      self.__closeTabImage.loadFromData( closeTabImage_data, "PNG" )

      self.tabPageIdDict = {}
      self.tabPageDict = {}
      self.__emSlotList = [] # This list is simply to keep reference to emSlot functions alive.
      
      self.Ganga_Jobs_TabWidget_BASE_Widget = Ganga_Jobs_TabWidget_BASE( self )

      self.tabWidget_Jobs = qt.QTabWidget( self.Ganga_Jobs_TabWidget_BASE_Widget, "tabWidget_Jobs" )
      self.tabWidget_Jobs.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Ignored, qt.QSizePolicy.Preferred, 0, 1, self.tabWidget_Jobs.sizePolicy().hasHeightForWidth() ) )
      self.Ganga_Jobs_TabWidget_BASE_Widget.layout().addWidget( self.tabWidget_Jobs )
      
      self.toolButton_CloseTab = qt.QToolButton( self.tabWidget_Jobs, "toolButton_CloseTab" )
      self.toolButton_CloseTab.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Fixed, qt.QSizePolicy.Fixed, 0, 0, self.toolButton_CloseTab.sizePolicy().hasHeightForWidth() ) )
      self.toolButton_CloseTab.setMaximumSize( qt.QSize( 12, 12 ) )
      self.toolButton_CloseTab.setCursor( qt.QCursor( 13 ) )
      self.toolButton_CloseTab.setBackgroundOrigin( qt.QToolButton.ParentOrigin )
      self.toolButton_CloseTab.setIconSet( qt.QIconSet( self.__closeTabImage ) )
      self.tabWidget_Jobs.setCornerWidget( self.toolButton_CloseTab )
      self.toolButton_CloseTab.setGeometry( qt.QRect( 0, 0, 12, 12 ) )
      self.toolButton_CloseTab.hide()
      
      self.boxLayout().addWidget( self.Ganga_Jobs_TabWidget_BASE_Widget )
      self._loadGeometry()
      self.jobCopyAction = qt.QAction( self, "jobCopyAction" )
      self.jobCopyAction.setEnabled( 1 )
      self.jobCopyAction.setIconSet( qt.QIconSet( self._parent.image3 ) )
      self.jobCopyAction.setText( qt.QString( "Copy" ) )
      self.jobSaveAsTemplateAction = qt.QAction( self, "jobSaveAsTemplateAction" )
      self.jobSaveAsTemplateAction.setEnabled( 1 )
      self.jobSaveAsTemplateAction.setIconSet( qt.QIconSet( self._parent.image2 ) )
      self.jobSaveAsTemplateAction.setText( qt.QString( "Save As Template" ) )

      # Connections ----------
      self.connect( self.tabWidget_Jobs, qt.SIGNAL( "currentChanged(QWidget*)" ), self.slotCurrentChanged )
      self.connect( self.toolButton_CloseTab, qt.SIGNAL("clicked()"), self.slotCloseTab )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_NewJob, qt.SIGNAL("clicked()"), self._parent.jobNewAction, qt.SIGNAL("activated()") )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_RemoveJob, qt.SIGNAL("clicked()"), self.slotRemoveJob )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_KillJob, qt.SIGNAL("clicked()"), self.slotKillJob )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SaveJob, qt.SIGNAL("clicked()"), self.slotSaveJob )
      self.connect( self.jobSaveAsTemplateAction, qt.SIGNAL( "activated()" ), self.slotSaveAsTemplateJob )
      self.connect( self.jobCopyAction, qt.SIGNAL( "activated()" ), self.slotCopyJob )
      self.connect( self._parent, qt.PYSIGNAL( "newJob()" ), self.slotNewJob )
      self.connect( self._parent, qt.PYSIGNAL( "newJobFromTemplate()" ), self.slotNewJob )
      self.connect( self._parent, qt.PYSIGNAL( "currentJobChanged(source,id_str)" ), self.slotActivateTab )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SubmitJob, qt.SIGNAL( "clicked()" ), self.slotSubmitJob )
      self.connect( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_ExportMethods, qt.SIGNAL( "pressed()" ), self.updateExportMethods )

      # Context menu construction
      self.contextMenu_New = qt.QPopupMenu( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_NewJob )
      self.contextMenu_Save = qt.QPopupMenu( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SaveJob )
      self.contextMenu_Export = qt.QPopupMenu( self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_ExportMethods )
      self._parent.actionsDict['NewFromTemplate'].addTo( self.contextMenu_New )
      self.jobCopyAction.addTo( self.contextMenu_Save )
      self.jobSaveAsTemplateAction.addTo( self.contextMenu_Save )
      self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_NewJob.setPopup( self.contextMenu_New )
      self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SaveJob.setPopup( self.contextMenu_Save )
      self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_ExportMethods.setPopup( self.contextMenu_Export )
      self.setButtons( False )

   def minimumSizeHint( self ):
      return qt.QSize( 520, 55 )

   def _loadGeometry( self ):
      try:
         _wg = GUIConfig.getDict( 'Window_Geometry' )[ str( self.name() ) ]
      except KeyError:
         _wg = None
      if _wg:
         self.__defaultDockSettings = [ DOCK[ _wg[0][1] ], _wg[0][3], _wg[0][2], _wg[0][4] ]
         self.__lastSavedGeometry = ( _wg[1][0], _wg[1][1], _wg[1][2], _wg[1][3] )
         if _wg[0][1] == qt.Qt.DockTornOff: # dock window floating
            self.move( _wg[1][0], _wg[1][1] )
            self.resize( _wg[1][2], _wg[1][3] )
         else: # dock window in dock
            #self.move( _wg[1][0], _wg[1][1] )
            self.resize( _wg[1][2], _wg[1][3] )
      else: # error obtaining window geometry from config file
         self.__defaultDockSettings = [ qt.Qt.DockTornOff, True, 2, -1 ]
         self.__lastSavedGeometry = ( 200, 200, 760, 580 )

   def _setLastGeometry( self ):
      self.__lastSavedGeometry = ( self.x(), self.y(), self.width(), self.height() ) 

   def _getLastGeometry( self ):
      return self.__lastSavedGeometry

   def _getDefaultDockSettings( self ):
      return self.__defaultDockSettings

   def closeAllTabs( self ):
      for id_str in self.tabPageIdDict.keys():
         if not self.slotCloseTab( id_str ):
            return False
      return True

   def customEvent( self, myEvent ):
      if myEvent.type() == JB_TABWIDGET_SETBUTTONS_EVENT:
         self.setButtons( myEvent.flag )
      elif myEvent.type() == JB_TABWIDGET_CLOSETAB_EVENT:
         self.slotCloseTab( myEvent.id_str, myEvent.quick )

   def setButtons_FromThread( self, enabled ):
      qt.QApplication.postEvent( self, JB_SetButtons_CustomEvent( enabled ) )

   def setButtons( self, enabled ):
      if enabled in [ True, False ]:
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_RemoveJob.setEnabled( enabled )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SaveJob.setEnabled( enabled )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SubmitJob.setEnabled( enabled )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_KillJob.setEnabled( enabled )
      else: # 'enabled' is a tab widget page
         _job = self.tabPageDict[ enabled ]
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SubmitJob.setEnabled( not _job.isSubmitted() )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_KillJob.setEnabled( _job.job.status in [ 'submitted', 'running' ] ) #, 'completing' ] )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_RemoveJob.setEnabled( True )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_SaveJob.setEnabled( True )
         self.Ganga_Jobs_TabWidget_BASE_Widget.toolButton_ExportMethods.setEnabled( True )
      if self.tabPageDict:
         self.toolButton_CloseTab.show()
      else:
         self.toolButton_CloseTab.hide()

   def __customisedActionSlot( self, emObj, emObjStr, methodClass_name ):
      def _f():
         argumentDialog = EMArguments_Dialog( self, emObj, emObjStr, methodClass_name )
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
                  break
            elif response == qt.QDialog.Rejected:
               break
      return _f
   
   def notVisible( self ):
      return self.isHidden() or self in self._parent.dockWindows( qt.Qt.DockMinimized )

   def updateExportMethods( self ):
      del self.__emSlotList[:]
      self.contextMenu_Export.clear()
      try:
         _job = self.tabPageDict[ self.tabWidget_Jobs.currentPage() ]
      except KeyError:
         return
      emDict = _job.getExportMethods()
      for _category in emDict:
         emActionGroup = qt.QActionGroup( self, "%sActionGroup" % _category )
         emActionGroup.setText( qt.QString( _category ) )
         emActionGroup.setMenuText( qt.QString( _category ) )
         emActionGroupPopup = qt.QPopupMenu( self )
         self.contextMenu_Export.insertItem( _category, emActionGroupPopup )
         for exportMethod in emDict[ _category ]:
            emAction = qt.QAction( emActionGroup, "%sAction" % exportMethod )
            emAction.setText( qt.QString( exportMethod ) )
            emAction.setMenuText( qt.QString( exportMethod ) )
            emAction.addTo( emActionGroupPopup )
            emSlot = self.__customisedActionSlot( eval( "%s.%s" % ( _category.replace( 'Job', '_job.job' ), exportMethod ) ), exportMethod, eval( "%s.__class__.__name__" % ( _category.replace( 'Job', '_job.job' ) ) ) )
            self.__emSlotList.append( emSlot )
            self.connect( emAction, qt.SIGNAL( "activated()" ), emSlot )

   def isJobTabOpen( self, id_str ):
      return self.tabPageIdDict.has_key( id_str )

   def slotNewJob( self, t_id = None, existingJob = None, makeCopy = None ):
      if not makeCopy and existingJob and str( existingJob.id ) in self.tabPageIdDict:
         miscDialogs.infoMessage( None, "Job %s already open." % existingJob.id )
         return
      _sv = qt.QScrollView( self.tabWidget_Jobs, "ScrollView_Job" )
      _sv.enableClipper( True )
      _sv.setResizePolicy( qt.QScrollView.AutoOneFit )
      _svPort = qt.QVBox( _sv.viewport() )
      _j = Ganga_Job( _svPort, fl = qt.QWidget.WDestructiveClose, template_id = t_id, existingJob = existingJob, makeCopy = makeCopy )
      _sv.addChild( _j )
      self.tabPageDict[ _sv ] = _j
      self.tabPageIdDict[ str( _j.job.id ) ] = _sv
      self.tabWidget_Jobs.insertTab( _sv, qt.QString( 'Job ' + str( _j.job.id ) ) )
      self.tabWidget_Jobs.setCurrentPage( self.tabWidget_Jobs.indexOf( _sv ) )
      self.setButtons( _sv )
      if self.notVisible():
         self._parent.slotFloatJobBuilder()
#         self.show()
      else:
         _j.show()
      self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )

   def slotCopyJob( self ):
      self.slotNewJob( existingJob = self.tabPageDict[ self.tabWidget_Jobs.currentPage() ].job, makeCopy = True )

   def slotKillJob( self, id_str = None ):
      if id_str is None:
         _cPage = self.tabWidget_Jobs.currentPage()
      else:
         _cPage = self.tabPageIdDict[ id_str ]

      def cb_Success():
         self.setButtons_FromThread( _cPage )
         self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )
      
      def cb_Failure():
         self.setButtons_FromThread( _cPage )
      
      self.tabPageDict[ _cPage ].kill( cb_Success, cb_Failure )

   def slotRemoveJob( self, id_str = None ):
      if id_str is None:
         _cPage = self.tabWidget_Jobs.currentPage()
         try:
            id_str = str( self.tabPageDict[ _cPage ].job.id )
         except KeyError:
            return True
      else:
         _cPage = self.tabPageIdDict[ id_str ]   

      def cb():
         self.setButtons_FromThread( _cPage )
         self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )
         self.slotCloseTab_FromThread( id_str, True )
      
      self.tabPageDict[ _cPage ].remove( cb, cb )

   def slotCloseTab_FromThread( self, id_str, quick = False ):
      qt.QApplication.postEvent( self, JB_CloseTab_CustomEvent( id_str, quick ) )

   def slotCloseTab( self, id_str = None, quick = False ):
      if id_str is None:
         _cPage = self.tabWidget_Jobs.currentPage()
         try:
            id_str = str( self.tabPageDict[ _cPage ].job.id )
         except KeyError:
            return True
      else:
         _cPage = self.tabPageIdDict[ id_str ]
      if self.tabPageDict.has_key( _cPage ):
         _jobWidget = self.tabPageDict[ _cPage ]
#         if self.slotSaveJob( _jobWidget ) and _jobWidget.close(): #, quick ):
         if not quick and not self.slotSaveJob( _jobWidget ):
            return False
         if _jobWidget.close(): #, quick ):
            self.tabWidget_Jobs.removePage( _cPage )
            del self.tabPageDict[ _cPage ]
            del self.tabPageIdDict[ id_str ]
            if not self.tabPageDict:
               self.setButtons( False )
         else:
            return False
      return True

   def slotCloseTab_Original( self, id_str = None, removeJob = None, quick = False ):
      if removeJob is None:
         removeJob = False
      if id_str is None:
         _cPage = self.tabWidget_Jobs.currentPage()
         try:
            id_str = str( self.tabPageDict[ _cPage ].job.id )
         except KeyError:
            return True
      else:
         _cPage = self.tabPageIdDict[ id_str ]
      if self.tabPageDict.has_key( _cPage ):
         _jobWidget = self.tabPageDict[ _cPage ]
         if removeJob:
            _c = _jobWidget.remove
         else:
            if self.slotSaveJob( _jobWidget, quick ):
               _c = _jobWidget.close
            else:
               return False
         if _c():
            if not quick:
               self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )
            self.tabWidget_Jobs.removePage( _cPage )
            del self.tabPageDict[ _cPage ]
            del self.tabPageIdDict[ id_str ]
            if not self.tabPageDict:
               self.setButtons( False )
               self.toolButton_CloseTab.hide()
         else:
            return False
      return True

   def slotCurrentChanged( self, _cPage ):
      self.setButtons( _cPage )
      self.emit( qt.PYSIGNAL( "currentJobChanged(source,id_str)" ), ( self, str( self.tabPageDict[ _cPage ].job.id ) ) )

   def slotActivateTab( self, source, id_str ):
      if source is self:
         return
      if self.tabPageIdDict.has_key( id_str ):
         self.tabWidget_Jobs.showPage( self.tabPageIdDict[ id_str ] )

   def slotSaveAsTemplateJob( self, id_str = None ):
      if id_str is None:
         jobWidget = self.tabPageDict[ self.tabWidget_Jobs.currentPage() ]
      else:
         jobWidget = self.tabPageDict[ self.tabPageIdDict[ id_str ] ]
      _jt = jobWidget.makeTemplate()
      if _jt:
         self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )
         self._parent.createNewTemplateAction( _jt )
   
   def slotSaveJob( self, jobWidget = None ): #, quick = False ):
      if jobWidget:
         _ok = jobWidget.saveChanges()
      else:
         _ok = self.tabPageDict[ self.tabWidget_Jobs.currentPage() ].saveChanges()
      return _ok
   
   def slotSubmitJob( self, id_str = None ):
      if id_str is None:
         _cPage = self.tabWidget_Jobs.currentPage()
      else:
         _cPage = self.tabPageIdDict[ id_str ]

      def cb_Success():
         self.setButtons_FromThread( _cPage )
         self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )
      
      def cb_Failure():
         self.setButtons_FromThread( True )
      
      self.tabPageDict[ _cPage ].submit( cb_Success, cb_Failure )

