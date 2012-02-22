import os, cPickle, tempfile, shutil
from GangaGUI.Ganga_Scriptor_BASE import Ganga_Scriptor_BASE
from GangaGUI.Ganga_PyCute import PyCute
from GangaGUI import miscDialogs
from GangaGUI.cfg_manager import GUIConfig
import qt


# PyQt Hack. Defined in the event Qt.Dock does not exist.
DOCK = { 0: qt.Qt.DockUnmanaged,
         1: qt.Qt.DockTornOff,
         2: qt.Qt.DockTop,
         3: qt.Qt.DockBottom,
         4: qt.Qt.DockRight,
         5: qt.Qt.DockLeft,
         6: qt.Qt.DockMinimized }


class SListViewItem( qt.QListViewItem ):
   def __init__( self, parent = None, sTuple = None ):
      qt.QListViewItem.__init__( self, parent )
      if sTuple is None:
         sTuple = ( 'New script', '', '' )
      self.scriptName = sTuple[0]
      self.scriptContent = sTuple[1]
      self.scriptDescription = sTuple[2]
      self.setText( 0, self.scriptName )
      self.setText( 1, self.scriptDescription )
      self.setRenameEnabled( 0, True )
      self.setRenameEnabled( 1, True )


class Ganga_Scriptor( qt.QDockWindow ):
   def __init__( self, parent = None, name = None, fl = 0, std={} ):
      qt.QDockWindow.__init__( self, parent, name, fl )
      self.setResizeEnabled( True )
      self.setNewLine( True )
      self.setOpaqueMoving( False )
      self.setCaption( qt.QString( "Scriptor" ) )
      self.Ganga_Scriptor_BASE_Widget = Ganga_Scriptor_BASE( self )
      self.setSizePolicy( qt.QSizePolicy(qt.QSizePolicy.Preferred, qt.QSizePolicy.Preferred, 0, 2, self.sizePolicy().hasHeightForWidth() ) )
      self.pythoniShell = PyCute( self.Ganga_Scriptor_BASE_Widget.splitter_ScriptorVertical, **std )
      self.boxLayout().addWidget( self.Ganga_Scriptor_BASE_Widget )
      self._loadGeometry()
      self.contextMenu_ActiveScripts = qt.QPopupMenu( self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts )
      self.fillFavourites()

      # Active script actions -------------
      self.execScriptAction = qt.QAction( self, "execScript" )
      self.execScriptAction.setText( "Exec" )
      self.execScriptAction.setMenuText( "Exec" )
      self.execScriptAction.setToolTip( "Execute script." )
      self.execScriptAction.setWhatsThis( "Execute script." )

      self.cloneScriptAction = qt.QAction( self, "cloneScript" )
      self.cloneScriptAction.setText( "Clone" )
      self.cloneScriptAction.setMenuText( "Clone" )
      self.cloneScriptAction.setToolTip( "Clone script." )
      self.cloneScriptAction.setWhatsThis( "Clone script." )

      self.exportScriptAction = qt.QAction( self, "exportScript" )
      self.exportScriptAction.setText( "Export" )
      self.exportScriptAction.setMenuText( "Export" )
      self.exportScriptAction.setToolTip( "Export script." )
      self.exportScriptAction.setWhatsThis( "Export script." )

      self.removeScriptAction = qt.QAction( self, "removeScript" )
      self.removeScriptAction.setText( "Remove" )
      self.removeScriptAction.setMenuText( "Remove" )
      self.removeScriptAction.setToolTip( "Remove script." )
      self.removeScriptAction.setWhatsThis( "Remove script." )

      self.importScriptAction = qt.QAction( self, "importScript" )
      self.importScriptAction.setText( "Import" )
      self.importScriptAction.setMenuText( "Import" )
      self.importScriptAction.setToolTip( "Import script." )
      self.importScriptAction.setWhatsThis( "Import script." )

      self.newScriptAction = qt.QAction( self, "newScript" )
      self.newScriptAction.setText( "New" )
      self.newScriptAction.setMenuText( "New" )
      self.newScriptAction.setToolTip( "New script." )
      self.newScriptAction.setWhatsThis( "New script." )

      # Connections --------------------------
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.pushButton_ExecScript, 
                    qt.SIGNAL( "clicked()" ), 
                    self.slotExecScript )
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.textEdit, 
                    qt.SIGNAL( "textChanged()" ), 
                    self.slotEditorTextChanged )
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, 
                    qt.SIGNAL( "clicked(QListViewItem*)" ), 
                    self.slotDisplayScript )
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, 
                    qt.SIGNAL( "selectionChanged(QListViewItem*)" ), 
                    self.slotDisplayScript )
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, 
                    qt.SIGNAL( "contextMenuRequested(QListViewItem*,const QPoint&,int)" ), 
                    self.slotContextMenu_ActiveScripts )
      self.Ganga_Scriptor_BASE_Widget.connect( 
                    self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, 
                    qt.SIGNAL( "itemRenamed(QListViewItem*,int,const QString&)" ), 
                    self.slotScriptNameModified )

      # Connections for actions
      self.connect( self.execScriptAction, qt.SIGNAL("activated()"), self.slotExecScript )
      self.connect( self.newScriptAction, qt.SIGNAL("activated()"), self.slotNewScript )
      self.connect( self.cloneScriptAction, qt.SIGNAL("activated()"), self.slotCloneScript )
      self.connect( self.removeScriptAction, qt.SIGNAL("activated()"), self.slotRemoveScript )
      self.connect( self.importScriptAction, qt.SIGNAL("activated()"), self.slotImportScript )
      self.connect( self.exportScriptAction, qt.SIGNAL("activated()"), self.slotExportScript )

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
            self.move( _wg[1][0], _wg[1][1] )
            self.resize( _wg[1][2], _wg[1][3] )
      else: # error obtaining window geometry from config file
         self.__defaultDockSettings = [ qt.Qt.DockBottom, True, 0, -1 ]
         self.__lastSavedGeometry = ( 320, 200, 600, 200 )
   
   def _setLastGeometry( self ):
      self.__lastSavedGeometry = ( self.x(), self.y(), self.width(), self.height() ) 

   def _getLastGeometry( self ):
      return self.__lastSavedGeometry

   def __getFavouritesList( self ):
      _favouritesFilename = os.path.join( GUIConfig.GangaGUI_CONFIG_DIR, 'favourites.dat' )
      if os.path.exists( _favouritesFilename ):
         try:
            return cPickle.load( file( _favouritesFilename, 'r' ) )
         except cPickle.UnpicklingError, msg:
            miscDialogs.warningMessage( None, "The file containing the scriptor favourites\n[%s]\nmay be corrupt:\n%s" % ( _favouritesFilename, msg ) )
         except:
            pass
      return []

   def fillFavourites( self ):
      self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.clear()
      for s in self.__getFavouritesList():
         SListViewItem( self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, s )

   def saveFavourites( self ):
      _favouritesFilename = os.path.join( GUIConfig.GangaGUI_CONFIG_DIR, 'favourites.dat' )
      _creationFailed = False
      while True:
         if os.path.exists( _favouritesFilename ) or _creationFailed:
            if not os.access( _favouritesFilename, os.W_OK ):
               _reply = qt.QMessageBox.warning( self, "Warning!", "Error creating %s to save Scriptor favourites.\nSave to alternative location?" % _favouritesFilename, "Ok", "No", "Cancel", 2, 2 )
               if _reply == 0: # Ok
                  _favouritesFilename = str( qt.QFileDialog.getSaveFileName( GUIConfig.GangaGUI_CONFIG_DIR, "", self, "BrowseDir_Favourites", "Select alternative location to save [favourites.dat]", None ) )
                  continue
               elif _reply == 1: # No
                  return True
               elif _reply == 2: # Cancel
                  return False
         else:
            try:
               file( _favouritesFilename, 'w' )
            except IOError:
               _creationFailed = True
               continue
         
         # Prepare the list of scripts for pickling
         favouritesList = []
         _i = self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.firstChild()
         while _i:
            favouritesList.append( ( _i.scriptName, _i.scriptContent, _i.scriptDescription ) )
            _i = _i.nextSibling()
         
         # Create a temporary file to dump favouritesList so that the original 
         # favourites.dat file is not lost if the pickle action fails
         _tempFavouritesFileDescriptor, _tempFavouritesFilename = tempfile.mkstemp()
         os.close( _tempFavouritesFileDescriptor )
         _tempFavouritesFile = file( _tempFavouritesFilename, 'w' )
         try:
            cPickle.dump( favouritesList, _tempFavouritesFile, cPickle.HIGHEST_PROTOCOL )
            _tempFavouritesFile.close()
         except cPickle.PicklingError, msg:
            miscDialogs.warningMessage( self, "Error saving Scriptor favourites:\n%s\nThis should not happen!" % msg )
            raise
         # replace the original but back it up first.
         try:
            shutil.copyfile( _favouritesFilename, _favouritesFilename + '_backup' )
         except:
            pass
         try:
            shutil.move( _tempFavouritesFilename, _favouritesFilename )
         except:
            return False
         return True

   def slotContextMenu_ActiveScripts( self, item, point, column ):
      self.contextMenu_ActiveScripts.clear()
      self.execScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.contextMenu_ActiveScripts.insertSeparator()
      self.newScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.cloneScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.removeScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.contextMenu_ActiveScripts.insertSeparator()
      self.importScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.exportScriptAction.addTo( self.contextMenu_ActiveScripts )
      self.contextMenu_ActiveScripts.exec_loop( point )

   # Scriptor slots --------------------

   def slotExecScript( self ):
      self.pythoniShell.fakeUser( str( self.Ganga_Scriptor_BASE_Widget.textEdit.text() ).split( '\n' ) )
      self.emit( qt.PYSIGNAL( "forceUpdate()" ), () )

   def slotEditorTextChanged( self ):
      _LVItem = self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.selectedItem()
      if _LVItem:
         _LVItem.scriptContent = str( self.Ganga_Scriptor_BASE_Widget.textEdit.text() )

   def slotCloneScript( self ):
      _LVItem = self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.selectedItem()
      if _LVItem:
         SListViewItem( self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, ( _LVItem.scriptName, _LVItem.scriptContent, _LVItem.scriptDescription ) ).startRename( 0 )

   def slotRemoveScript( self ):
      _LVItem = self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.selectedItem()
      if _LVItem:
         self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.takeItem( _LVItem )
         del _LVItem
         self.slotDisplayScript( None )

   def slotExportScript( self ):
      _LVItem = self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts.selectedItem()
      if _LVItem:
         # Possible PyQt bug:
         # file dialog with 'self' as parent will not accept keyboard input!
         # Using 'None' as work around.
         _exportFilename = str( qt.QFileDialog.getSaveFileName( os.path.expanduser( '~' ), "", None, "BrowseDir_Export", "Select destination filename", None ) )
         if _exportFilename:
            try:
               _exportFile = file( _exportFilename, 'w' )
               _exportFile.write( _LVItem.scriptContent )
               _exportFile.close()
            except IOError, msg:
               miscDialogs.warningMessage( self, "Error creating %s:\n%s" % ( os.path.basename( _exportFilename ), msg ) )
               return

   def slotImportScript( self ):
      _scriptFilename = str( qt.QFileDialog.getOpenFileName( GUIConfig.GangaGUI_CONFIG_DIR, "", self, "BrowseDir_Favourites", "Load an external script", None ) )
      if _scriptFilename:
         try:
            _script = file( _scriptFilename )
         except IOError, msg:
            miscDialogs.warningMessage( self, "Error reading %s:\n%s" % ( os.path.basename( _scriptFilename ), msg ) )
            return
         else:
            SListViewItem( self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts, ( os.path.basename( _scriptFilename ), _script.read(), '' ) )
      
   def slotNewScript( self ):
      _item = SListViewItem( self.Ganga_Scriptor_BASE_Widget.listView_ActiveScripts )
      _item.startRename( 0 )

   def slotDisplayScript( self, selectedItem ):
      if selectedItem is None:
         self.Ganga_Scriptor_BASE_Widget.textEdit.clear()
      else:
         self.Ganga_Scriptor_BASE_Widget.textEdit.setText( selectedItem.scriptContent )

   def slotScriptNameModified( self, _LVItem, col, newText ):
      if col == 0:
         _LVItem.scriptName = str( newText )
      elif col == 1:
         _LVItem.scriptDescription = str( newText )

   def _getDefaultDockSettings( self ):
      return self.__defaultDockSettings
