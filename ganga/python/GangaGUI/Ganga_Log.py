from GangaGUI.Ganga_Log_BASE import Ganga_Log_BASE
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


class Ganga_Log( qt.QDockWindow ):
   def __init__( self, parent = None, name = None, fl = 0 ):
      qt.QDockWindow.__init__( self, parent, name, fl )
      self.setResizeEnabled( True )
      self.setNewLine( True )
      self.setOpaqueMoving( False )
      self.setCaption( qt.QString( "Log" ) )
      self.Ganga_Log_BASE_Widget = Ganga_Log_BASE( self )
      self.Ganga_Log_BASE_Widget.textEdit_Log.setMaxLogLines( 500 )
      self.boxLayout().addWidget( self.Ganga_Log_BASE_Widget )
      self._loadGeometry()

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
         self.__defaultDockSettings = [ qt.Qt.DockBottom, True, 1, -1 ]
         self.__lastSavedGeometry = ( 320, 460, 600, 200 )
   
   def _setLastGeometry( self ):
      self.__lastSavedGeometry = ( self.x(), self.y(), self.width(), self.height() ) 

   def _getLastGeometry( self ):
      return self.__lastSavedGeometry

   def _getDefaultDockSettings( self ):
      return self.__defaultDockSettings
   
   def customEvent( self, myEvent ):
      if myEvent.type() == UPDATE_LOG_EVENT:
         self.Ganga_Log_BASE_Widget.textEdit_Log.append( myEvent.text )
   
   def write( self, text ):
      qt.QApplication.postEvent( self, UpdateLog_CustomEvent( str( text ) ) )
   
   def flush( self ):
      pass
