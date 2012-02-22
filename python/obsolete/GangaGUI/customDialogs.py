import os.path, inspect, time, threading
import qt
from GangaGUI.customTables import AttributeSelectionListView, SortedListView, ColourLVItem
from GangaGUI.customEvents import *
from GangaGUI import widget_set, Ganga_Errors
from GangaGUI.Ganga_GUI_Configuration_BASE import GUI_Configuration_BASE
from GangaGUI.StatusColourWidget_BASE import StatusColourWidget_BASE
from GangaGUI.EMArguments_Dialog_BASE import EMArguments_Dialog_BASE
from GangaGUI.ActionProgressDialog_BASE import ActionProgressDialog_BASE
from GangaGUI.ActionProgressEntry_BASE import ActionProgressEntry_BASE
from GangaGUI.Credentials_Dialog_BASE import Credentials_Dialog_BASE

# Setup logging ---------------
import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger( "Ganga.customDialogs" )

class StatusColourWidget( StatusColourWidget_BASE ):
   def __init__( self, guiConfig, parent = None, name = None, modifiedCallback = None, fl = 0 ):
      StatusColourWidget_BASE.__init__( self, parent, name, fl )
      self.guiConfig = guiConfig
      self.modifiedCallback = modifiedCallback
      self._currentColourSchema = self.guiConfig.getDict( 'Status_Colour_Schema', 'SESSION' )
      self._currentSubjobMask = self._currentColourSchema.pop( '*subjob*' )
      self._newColourSchema = self._currentColourSchema.copy()
      self._newSubjobMask = self._currentSubjobMask
      
      self.colourStatusLV = SortedListView( self.frame_StatusColourLV )
      self.colourStatusLV.setRootIsDecorated( False )
      self.colourStatusLV.setSelectionMode( qt.QListView.NoSelection )
      self.colourStatusLV.addColumn( 'Master' )
      self.colourStatusLV.addColumn( 'Subjob' )

      # Setting of slider defaults need to come after definition of 
      # self.colourStatusLV as the (slider setting) action sends a 
      # valueChanged(int) signal that is connect to a updateColourStatusLV() which 
      # assumes that self.colourStatusLV is defined.
      self.slider_Foreground.setValue( self._currentSubjobMask[ 0 ] )
      self.slider_Background.setValue( self._currentSubjobMask[ 1 ] )

      qt.QToolTip.add( self.colourStatusLV, "Click for master jobs status change. Subjobs status colours are similar differing only in brightness (use sliders)." )
      self.fillColourStatusLV()
      self.frame_StatusColourLV.layout().addWidget( self.colourStatusLV )
      
      self.colourStatusLV.contextMenu.insertItem( 'Foreground', 0 )
      self.colourStatusLV.contextMenu.insertItem( 'Background', 1 )

      self.connect( self.colourStatusLV, 
                    qt.SIGNAL( 'clicked( QListViewItem*, const QPoint&, int )' ), 
                    self.slotContextMenuRequested )
      self.connect( self.colourStatusLV, 
                    qt.SIGNAL( 'contextMenuRequested( QListViewItem*, const QPoint&, int )' ), 
                    self.slotContextMenuRequested )

   def minimumSizeHint( self ):
      return qt.QSize( 250, 190 )
      
   def slotContextMenuRequested( self, colourItem, point, column ):
      if colourItem is None or column is 1: # colour change should only affect master jobs.
         return
      whichGround = self.colourStatusLV.contextMenu.exec_loop( point )
      if whichGround == -1:
         return
      initialColour = colourItem.columnColour[ column ][ whichGround ]
      newColour = qt.QColorDialog.getColor( qt.QColor( initialColour ), self )
      if newColour.isValid():
         status = str( colourItem.text( 0 ) )
         if whichGround: # background
            self._newColourSchema[ status ] = ( self._newColourSchema[ status ][ 0 ], str( newColour.name() ) )
         else: # foreground
            self._newColourSchema[ status ] = ( str( newColour.name() ), self._newColourSchema[ status ][ 1 ] )
         self.updateColourStatusLV( colourItem )

   def fillColourStatusLV( self ):
      for status in self._currentColourSchema:
         fg, bg = self.__getStatusColour( status = status, subjob = False )
         colourStatusItem = ColourLVItem( self.colourStatusLV, status, fg, bg )
         fg, bg = self.__getStatusColour( status = status, subjob = True )
         colourStatusItem.setColouredText( 1, status, fg, bg )
   
   def updateColourStatusLV( self, colourStatusItem = None ):
      if colourStatusItem is None:
         colourStatusItemIterator = qt.QListViewItemIterator( self.colourStatusLV )
         while colourStatusItemIterator.current():
            i = colourStatusItemIterator.current()
            status = str( i.text( 1 ) )
            fg, bg = self.__getStatusColour( status = status, subjob = True, useCurrentSchema = False )
            i.setColouredText( 1, status, fg, bg )
            colourStatusItemIterator += 1
      else:
         status = str( colourStatusItem.text( 0 ) )
         # master job status update
         fg, bg = self.__getStatusColour( status = status, subjob = False, useCurrentSchema = False )
         colourStatusItem.setColouredText( 0, status, fg, bg )
         # subjob status update
         fg, bg = self.__getStatusColour( status = status, subjob = True, useCurrentSchema = False )
         colourStatusItem.setColouredText( 1, status, fg, bg )

   def __getStatusColour( self, status, subjob = False, useCurrentSchema = True ):
      if useCurrentSchema:
         colourSchema = self._currentColourSchema
         subjobMask = self._currentSubjobMask
      else:
         colourSchema = self._newColourSchema
         subjobMask = self._newSubjobMask
      try:
         colourTuple = colourSchema[ status ]
      except:
         colourTuple = ( 'black', None )
      fg = colourTuple[ 0 ]
      bg = colourTuple[ 1 ]
      if fg is not None:
         modifiedFG = qt.QColor( fg )
         if subjob:
            h, s, v = modifiedFG.getHsv()
            modifiedFG.setHsv( h, s, subjobMask[ 0 ] )
         fg = modifiedFG
      if bg is not None:
         modifiedBG = qt.QColor( bg )
         if subjob:
            h, s, v = modifiedBG.getHsv()
            modifiedBG.setHsv( h, s, subjobMask[ 1 ] )
         bg = modifiedBG
      return fg, bg

   def slotBGSliderMoved( self, newValue ):
      self._newSubjobMask = ( self._newSubjobMask[ 0 ], newValue )
      self.updateColourStatusLV()

   def slotFGSliderMoved( self, newValue ):
      self._newSubjobMask = ( newValue, self._newSubjobMask[ 1 ] )
      self.updateColourStatusLV()

   def get( self ):
      self._newColourSchema[ '*subjob*' ] = self._newSubjobMask
      self._currentColourSchema[ '*subjob*' ] = self._currentSubjobMask
      if self._newColourSchema != self._currentColourSchema:
         self.modifiedCallback( "refreshMonitor()", () )
      return self._newColourSchema


class GUI_Configuration( GUI_Configuration_BASE ):
   def __init__( self, guiConfig, parent = None, name = None, modal = False, fl = 0 ):
      GUI_Configuration_BASE.__init__( self, parent, name, modal, fl )
      self.parent = parent
      self.guiConfig = guiConfig
      self._signalDict = {}
      self.tabPageIndex = {}
      self.populateTabPages()
   
   def minimumSizeHint( self ):
      return qt.QSize( 610, 350 )
   
   def populateTabPages( self ):
      self._populateGeneral()
      self._populateMonitoring()
   
   def _populateGeneral( self ):

      def updateGUIStyle():
         qt.QApplication.setStyle( str( __widgetDict[ 'GUI_Style' ].comboBox.currentText() ) )
      def updatePixmapSize():
         self.parent.setUsesBigPixmaps( eval( str( __widgetDict[ 'Use_Big_Pixmaps' ].comboBox.currentText() ) ) )

      __widgetDict = {}
      dialogLayout = qt.QVBoxLayout( self.tab_General, 0, 0,"dialogLayout")
      _sv = qt.QScrollView( self.tab_General, "ScrollView_General_TabPage" )
      _sv.enableClipper( True )
      _sv.setResizePolicy( qt.QScrollView.AutoOneFit )
      _svPort = qt.QVBox( _sv.viewport() )
      configLayoutWidget = qt.QWidget( _svPort, "configLayoutWidget")
      configLayout = qt.QVBoxLayout( configLayoutWidget, 0, 0,"configLayout")
      # File Association
      _option = 'File_Association'
      try:
         _optionValue = self.guiConfig.getDict( _option, 'SESSION' )
      except:
         try:
            _optionValue = self.guiConfig.getDict( _option, 'DEFAULT' )
         except:
            _optionValue = {}
#      w = widget_set.WIDGETS_AVAILABLE[ 'DictOfString' ]( parent = configLayoutWidget, textLabel = _option, default = _optionValue, tooltip = 'Define default applications to be used to open output files of particular extensions.', modifiedCallback = None, newEntryFormat = '.ext : App' )
#      w.default = _optionValueDefault

      w = widget_set.File_Association( parent = configLayoutWidget, default = _optionValue, modifiedCallback = None )
      configLayout.addWidget( w )
      __widgetDict[ _option ] = w

      # Shell Command
      _option = 'Shell_Command'
      try:
         _optionValue = self.guiConfig.getString( _option, 'SESSION' )
      except:
         _optionValue = ''
      try:
         _optionValueDefault = self.guiConfig.getString( _option, 'DEFAULT' )
      except:
         _optionValueDefault = ''
      w = widget_set.WIDGETS_AVAILABLE[ 'String' ]( parent = configLayoutWidget, textLabel = _option, default = _optionValue, tooltip = 'Define the shell command that will allow non-GUI external programmes to run in the current window manager.', modifiedCallback = None )
      w.default = _optionValueDefault
      configLayout.addWidget( w )
      __widgetDict[ _option ] = w

      # GUI Style
      _option = 'GUI_Style'
      try:
         _optionValue = self.guiConfig.getString( _option, 'SESSION' )
      except:
         _optionValue = ''
      try:
         _optionValueDefault = self.guiConfig.getString( _option, 'DEFAULT' )
      except:
         _optionValueDefault = ''
      w = widget_set.WIDGETS_AVAILABLE[ 'String_Choice' ]( parent = configLayoutWidget, textLabel = _option, choices = [ str( x ) for x in qt.QStyleFactory.keys() ], default = _optionValue, tooltip = 'Change the current GUI style.', modifiedCallback = updateGUIStyle )
      w.default = _optionValueDefault
      configLayout.addWidget( w )
      __widgetDict[ _option ] = w

      # Button Size
      _option = 'Use_Big_Pixmaps'
      try:
         _optionValue = self.guiConfig.getBool( _option, 'SESSION' )
      except:
         _optionValue = False
      try:
         _optionValueDefault = self.guiConfig.getBool( _option, 'DEFAULT' )
      except:
         _optionValueDefault = False
      w = widget_set.WIDGETS_AVAILABLE[ 'Bool' ]( parent = configLayoutWidget, textLabel = _option, default = _optionValue, tooltip = 'Use large pixmaps.', modifiedCallback = updatePixmapSize )
      w.default = _optionValueDefault
      configLayout.addWidget( w )
      __widgetDict[ _option ] = w
      _sv.addChild( configLayoutWidget )
      dialogLayout.addWidget( _sv )
      self.tabPageIndex[ 'General' ] = __widgetDict

   def _populateMonitoring( self ):
      __widgetDict = {}
      dialogLayout = qt.QVBoxLayout( self.tab_Monitoring, 0, 0,"dialogLayout")
      _sv = qt.QScrollView( self.tab_Monitoring, "ScrollView_General_TabPage" )
      _sv.enableClipper( True )
      _sv.setResizePolicy( qt.QScrollView.AutoOneFit )
      _svPort = qt.QVBox( _sv.viewport() )
      configLayoutWidget = qt.QWidget( _svPort, "configLayoutWidget")
      configLayout = qt.QVBoxLayout( configLayoutWidget, 0, 0,"configLayout")

      _option = 'Status_Colour_Schema'
      w = StatusColourWidget( guiConfig = self.guiConfig, parent = configLayoutWidget, modifiedCallback = self.addSignal )
      configLayout.addWidget( w )
      __widgetDict[ _option ] = w

      _sv.addChild( configLayoutWidget )
      dialogLayout.addWidget( _sv )
      
      self.tabPageIndex[ 'Monitoring' ] = __widgetDict

   def addSignal( self, signal, args ):
      self._signalDict[ signal ] = args
   
   def delSignal( self, signal ):
      del self._signalDict[ signal ]

   def __signalChanges( self ):
      for signal in self._signalDict:
         self.emit( qt.PYSIGNAL( signal ), self._signalDict[ signal ] )

   def updateConfig( self ):
      for section in self.tabPageIndex:
         widgetDict = self.tabPageIndex[ section ]
         for option in widgetDict:
            self.guiConfig.setAuto( widgetDict[ option ].get(), option )
      self.__signalChanges()

   def slotApply(self):
      pass

   def slotTabChanged(self,a0):
      pass
   

class ChangeMonitoringFieldsDialog( qt.QDialog ):
   def __init__( self, componentDict = {}, currentSelection = [], parent = None, name = None, caption = '', modal = False, f = 0 ):
      qt.QDialog.__init__( self, parent, name, modal, f )
      self.setModal(1)
      self.setSizeGripEnabled(0)
      self.setCaption( caption )
      dialogLayout = qt.QVBoxLayout( self, 0, 0,"dialogLayout")
      self.selectionTable = AttributeSelectionListView( self, componentDict, currentSelection )
      dialogLayout.addWidget( self.selectionTable )
      buttonLayout = qt.QHBoxLayout(None,0,6,"buttonLayout")
      buttonSpacer = qt.QSpacerItem(10,0,qt.QSizePolicy.Expanding,qt.QSizePolicy.Minimum)
      buttonLayout.addItem( buttonSpacer )
      pushButton_Cancel = qt.QPushButton( self, "pushButton_Cancel" )
      pushButton_Cancel.setText( "Cancel" )
      buttonLayout.addWidget( pushButton_Cancel )
      pushButton_Ok = qt.QPushButton( self, "pushButton_Ok" )
      pushButton_Ok.setText( "OK" )
      pushButton_Ok.setDefault( True )
      buttonLayout.addWidget( pushButton_Ok )
      dialogLayout.addLayout( buttonLayout )
      qt.QObject.connect( pushButton_Ok, qt.SIGNAL("clicked()"), self.accept )
      qt.QObject.connect( pushButton_Cancel, qt.SIGNAL("clicked()"), self.reject )


class RetrieveJobOutputDialog( qt.QFileDialog ):
   def __init__( self, dirName, fileTypeFilter = '', parent = None, name = None, modal = False ):
      qt.QFileDialog.__init__( self, dirName, fileTypeFilter, parent, name, modal )
      self.setCaption( "Select file(s) for retrieval to separate location:" )
      self.highlightedFile = ''
      self.dirSelected = ''
      self.startDir = dirName
      self.notDone = True
      self.setMode( qt.QFileDialog.ExistingFiles )

      self.toolButton_ShowHiddenFiles = qt.QToolButton(self,"toolButton_ShowHiddenFiles")
      self.toolButton_ShowHiddenFiles.setUsesTextLabel(1)
      self.toolButton_ShowHiddenFiles.setAutoRaise(1)
      self.toolButton_ShowHiddenFiles.setText(qt.QString.null)
      self.toolButton_ShowHiddenFiles.setTextLabel(".?")
      qt.QToolTip.add( self.toolButton_ShowHiddenFiles, "Show/Hide hidden files." )
      self.toolButton_ShowHiddenFiles.setToggleButton( True )
      self.addToolButton( self.toolButton_ShowHiddenFiles, True )
      self.toolButton_Preview = qt.QToolButton(self,"toolButton_Preview")
      self.toolButton_Preview.setUsesTextLabel(1)
      self.toolButton_Preview.setAutoRaise(1)
      self.toolButton_Preview.setText(qt.QString.null)
      self.toolButton_Preview.setTextLabel("Preview")
      qt.QToolTip.add( self.toolButton_Preview, "Display contents of selected file." )
      self.toolButton_Preview.setToggleButton( True )
      self.addToolButton( self.toolButton_Preview, True )
      self.lineEdit_RetrieveTo = qt.QLineEdit( self )
      self.pushButton_Browse = qt.QPushButton( 'Browse', self )
      self.pushButton_Retrieve = qt.QPushButton( 'Retrieve', self )
      self.pushButton_Retrieve.setEnabled( False )
      self.addWidgets( qt.QLabel( "Retrieve to: ", self ), self.lineEdit_RetrieveTo, self.pushButton_Browse )
      dummy1 = qt.QLabel( "", self )
      dummy1.setSizePolicy(qt.QSizePolicy(qt.QSizePolicy.Expanding,qt.QSizePolicy.Fixed,1,0,dummy1.sizePolicy().hasHeightForWidth()))
      dummy2 = qt.QWidget( self )
      dummy2.setSizePolicy(qt.QSizePolicy(qt.QSizePolicy.Expanding,qt.QSizePolicy.Fixed,1,0,dummy2.sizePolicy().hasHeightForWidth()))
      self.addWidgets( dummy1, dummy2, self.pushButton_Retrieve )
      self.groupBox_Preview = qt.QGroupBox( self, "groupBox_Preview" )
      self.groupBox_Preview.setColumnLayout( 0, qt.Qt.Vertical )
      groupBox_PreviewLayout = qt.QVBoxLayout( self.groupBox_Preview.layout() )
      groupBox_PreviewLayout.setAlignment( qt.Qt.AlignTop )
      self.previewWidget = qt.QTextEdit( self.groupBox_Preview )
      self.previewWidget.setTextFormat( qt.QTextEdit.AutoText )
      self.previewWidget.setLinkUnderline( 0 )
      self.previewWidget.setWordWrap( qt.QTextEdit.NoWrap )
      self.previewWidget.setReadOnly( 1 )
      self.previewWidget.setUndoRedoEnabled( 0 )
      self.previewWidget.setAutoFormatting( qt.QTextEdit.AutoNone )
      groupBox_PreviewLayout.addWidget( self.previewWidget )
      self.setExtension( self.groupBox_Preview)
      # Connections
      self.connect( self, qt.SIGNAL( 'fileHighlighted(const QString&)' ), self.slotSetHighlightedFile )
      self.connect( self.toolButton_ShowHiddenFiles, qt.SIGNAL( 'toggled(bool)' ), self.setShowHiddenFiles )
      self.connect( self.toolButton_Preview, qt.SIGNAL( 'toggled(bool)' ), self.showExtension )
      self.connect( self.pushButton_Browse, qt.SIGNAL( 'clicked()' ), self.slotBrowse )
      self.connect( self.lineEdit_RetrieveTo, qt.SIGNAL( 'textChanged(const QString&)' ), self.slotEnableRetrieveButton )
      self.connect( self.pushButton_Retrieve, qt.SIGNAL( 'clicked()' ), self.accept )

   def slotEnableRetrieveButton( self, *args ):
      _enable = os.path.isdir( str( self.lineEdit_RetrieveTo.text() ) )
      self.pushButton_Retrieve.setEnabled( _enable )
      self.pushButton_Retrieve.setDefault( _enable )

   def slotSetHighlightedFile( self, _fn ):
      self.highlightedFile = str( _fn )
      self.previewWidget.clear()
      _fullpath = str( self.highlightedFile )
      if os.path.isfile( _fullpath ):
         try:
            f = file( _fullpath )
         except IOError:
            self.previewWidget.append( 'Cannot read file' )
            return
         _linecount = 0
         for line in f:
            if _linecount > 20:
               self.previewWidget.append( '...' )
               break
            try:
               self.previewWidget.append( str( line ).strip() )
            except:
               self.previewWidget.clear()
               break
            _linecount += 1
         f.close()

#   def slotSetSelectedfiles( self, _fileList ):
#      self.__fileList = self.selectedFiles() #_fileList
#      self.slotEnableRetrieveButton()

   def slotBrowse( self ):
      if self.highlightedFile:
         _startBrowseDir = os.path.dirname( self.highlightedFile )
      else:
         _startBrowseDir = self.startDir
      self.dirSelected = str( qt.QFileDialog.getExistingDirectory( _startBrowseDir, self, "BrowseDir", "Select a directory to save selected file(s) to:" ) )
      self.lineEdit_RetrieveTo.setText( self.dirSelected )


class EMArguments_Dialog( EMArguments_Dialog_BASE ):
   def __init__( self, parent, emObj, emObjStr, methodClass_name ):
      self.argumentList, _, _, self.argumentDefTuple = inspect.getargspec( emObj )
      try:
         self.argumentList.remove( 'self' )
      except:
         self.isStaticFunction = True
      else:
         self.isStaticFunction = False
      EMArguments_Dialog_BASE.__init__( self, parent )
      self.emObj = emObj
      self.emObjStr = emObjStr
      self.methodClass_name = methodClass_name
      if self.argumentDefTuple:
         a_revList = self.argumentList[:]
         a_revList.reverse()
         v_revList = list( self.argumentDefTuple )
         v_revList.reverse()
         self.argWithDefaults_Dict = dict( zip( a_revList, v_revList ) )
      else:
         self.argWithDefaults_Dict = {}
      argumentStringList = []
      item = None
      for arg in self.argumentList:
         argumentString = arg
         item = qt.QListViewItem( self.listView_Arguments, item )
         item.setRenameEnabled( 0, False )
         item.setRenameEnabled( 1, True )
         if arg in self.argWithDefaults_Dict:
            item.setText( 0, qt.QString( arg ) )
            argValue = self.argWithDefaults_Dict[ arg ].__str__()
            argumentString += '=%s' % argValue
            item.setText( 1, qt.QString( argValue ) )
         else:
            item.setText( 0, qt.QString( "* %s" % arg ) )
         argumentStringList.append( argumentString )
      argumentString = ', '.join( argumentStringList )
      self.textLabel_FuncDef.setText( qt.QString( "%s (%s)" % ( emObjStr, argumentString ) ) )
      self.connect( self.listView_Arguments, qt.SIGNAL( "itemRenamed( QListViewItem *, int )" ), self.__enableOk )
      self.__enableOk()

   def minimumSizeHint( self ):
      return qt.QSize( 470, 150 )

   def __enableOk( self, **args ):
      item = self.listView_Arguments.firstChild()
      while item:
         if str( item.text(0) )[0]=='*' and item.text( 1 ).isEmpty():
            self.pushButton_Ok.setEnabled( 0 )
            return
         item = item.nextSibling()
      self.pushButton_Ok.setEnabled( 1 )

   def _restoreType( self, valString ):
      valString = valString.strip()
      try:
         return { 'None' : None, 'True' : True, 'False' : False }[ valString.capitalize() ]
      except:
         pass
      try:
         return int( valString )
      except:
         pass
      try:
         return float( valString )
      except:
         pass
      if valString[ 0 ] == '[' and valString[ -1 ] == ']':
         return [ self._restoreType( x ) for x in valString[ 1 : -1 ].split( ',' ) ]
      if valString[ 0 ] == '(' and valString[ -1 ] == ')':
         return tuple( [ self._restoreType( x ) for x in valString[ 1 : -1 ].split( ',' ) ] )
      if valString[ 0 ] == '{' and valString[ -1 ] == '}':
         _v = {}
         for dictEntry in [ x for x in valString[ 1 : -1 ].split( ',' ) ]:
            try:
               arg, val = dictEntry.split( ':' )
            except ValueError:
               raise Ganga_Errors.ArgumentException( "Error unpacking dictionary %s." % valString )
            _v[ arg.strip() ] = self._restoreType( val.strip() )
         return _v
      return valString

   def getExportMethod( self ):
      argDict = {}
      item = self.listView_Arguments.firstChild()
      while item:
         arg = str( item.text( 0 ) ).strip()
         if arg[0] == '*': # compulsory argument
            arg = arg[ 1: ].strip()
            if item.text( 1 ).isEmpty():
               raise Ganga_Errors.ArgumentException( "No value given for the argument %s." % arg )
            argDict[ arg ] = self._restoreType( str( item.text( 1 ) ) )
         else: # optional argument
            if item.text( 1 ).isEmpty():
               pass
            else:
               argDict[ arg ] = self._restoreType( str( item.text( 1 ) ) )
         item = item.nextSibling()
      if self.isStaticFunction:
         return ( eval( "%s.%s" % ( self.methodClass_name, self.emObjStr ) ), argDict )
      else:
         return ( self.emObj, argDict )


class ActionProgressEntry( ActionProgressEntry_BASE ):
   def __init__( self, parent = None, name = None, fl = 0 ):
      ActionProgressEntry_BASE.__init__( self, parent, name, fl )
      self.pushButton_Cancel.hide()
      self.pushButton_Cancel.setEnabled( False )
   
   def minimumSizeHint( self ):
      return qt.QSize( 370, 60 )


class ActionProgressDialog( ActionProgressDialog_BASE ):
   def __init__( self, parent = None, name = None, modal = False, fl = 0 ):
      ActionProgressDialog_BASE.__init__( self, parent, name, modal, fl )
      self.qTimer = qt.QTimer( self )
      self.connect( self.qTimer, qt.SIGNAL( "timeout()" ), self.incAllProgress )
      self.__widgetDict = {}
      self._sv = qt.QScrollView( self, "ScrollView_ActionList" )
      self._sv.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Preferred, qt.QSizePolicy.Preferred, 0, 0, self._sv.sizePolicy().hasHeightForWidth() ) )
      self._sv.enableClipper( True )
      self._sv.setResizePolicy( qt.QScrollView.AutoOneFit )
      self._svPort = qt.QVBox( self._sv.viewport() )
      self.configLayoutWidget = qt.QWidget( self._svPort, "configLayoutWidget")
      self.configLayoutWidget.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Expanding, qt.QSizePolicy.Expanding, 0, 0, self.configLayoutWidget.sizePolicy().hasHeightForWidth() ) )
      self.configLayout = qt.QVBoxLayout( self.configLayoutWidget, 0, 0,"configLayout")
      self.configLayout.addItem( qt.QSpacerItem(10,0,qt.QSizePolicy.Minimum,qt.QSizePolicy.Expanding) )
      self._sv.addChild( self.configLayoutWidget )
      self.layout().insertWidget(0, self._sv )

   def minimumSizeHint( self ):
      return qt.QSize( 350, 280 )

   def addAPEntry( self, action ):
      w = ActionProgressEntry( parent = self.configLayoutWidget )
      self.configLayout.insertWidget( 0, w, 0, 0 )
      w.textLabel_ActionName.setText( action.description )
      w.show()
      self.__widgetDict[ action ] = w
      if not self.isShown():
         self.emit( qt.PYSIGNAL( "toggleAPAction()" ), () )
      if len( self.__widgetDict ) == 1:
         self.qTimer.start( 1000, False )

   def removeAPEntry_Helper( self, action ):
      qt.QApplication.postEvent( self, RemoveAction_CustomEvent( action ) )

   def removeAPEntry( self, action ):
      self.__widgetDict[ action ].hide()
      del self.__widgetDict[ action ]
      if not self.__widgetDict:
         self.qTimer.stop()
         if self.isShown():
            self.emit( qt.PYSIGNAL( "toggleAPAction()" ), () )
   
   def customEvent( self, myEvent ):
      if myEvent.type() == ACTION_PROGRESS_EVENT:
         self.setProgress( myEvent.action, myEvent.progressTuple )
      elif myEvent.type() == REMOVE_ACTION_EVENT:
         self.removeAPEntry( myEvent.action )

   def incAllProgress( self ):
      for action in self.__widgetDict.iterkeys():
         self.incProgress( action )

   def incProgress( self, action ):
      _p = int( self.__widgetDict[ action ].progressBar_Action.progress() )
      if _p > 30:
         self.setProgress_Helper( action, ( 0, 0 ) )
      else:
         self.setProgress_Helper( action, ( _p + 2, _p + 8 ) )

   def setProgress_Helper( self, action, progressTuple ):
      qt.QApplication.postEvent( self, ActionProgress_CustomEvent( action, progressTuple ) )

   def setProgress( self, action, progressTuple ):
      if action not in self.__widgetDict:
         return
      if progressTuple[1] == 0:
         self.__widgetDict[ action ].progressBar_Action.reset()
      else:
         self.__widgetDict[ action ].progressBar_Action.setProgress( *progressTuple )

   def killAPEntry( self, action ):
      # Cannot kill Python threads yet. Waiting for Python 2.5 or inclusion of ctypes package in Ganga (Python 2.3 and above only).
      pass


class Credential_Tab( qt.QWidget ):
   def __init__( self, parent, credObj, name = None, autorenew = False, enabled = True ):
      # credObj is assumed to be a _proxyObject
      if not hasattr( credObj, '_impl' ):
         raise Ganga_Errors.TypeException( "Valid GANGA credential object expected!" )
      if name is None:
         name = credObj._impl._name
      qt.QWidget.__init__( self, parent, name )
      self.autorenew = bool( autorenew )
      self.enabled = bool( enabled )
      self.__pw = ''
      self.credObj = credObj
      self.parent = parent
      self.name = name
      
      tabLayout = qt.QVBoxLayout( self, 11, 6, "tabLayout_%s" % name )
      
      # defining the password request question text label
      self.textLabel_pwQuestion = qt.QLabel( self, "textLabel_pwQuestion_%s" % name )
      self.textLabel_pwQuestion.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Preferred, qt.QSizePolicy.Fixed, 1, 0, self.textLabel_pwQuestion.sizePolicy().hasHeightForWidth() ) )
      self.textLabel_pwQuestion.setTextFormat( qt.QLabel.PlainText )
      self.textLabel_pwQuestion.setText( "Please enter %s passphrase/password:" % name )
      tabLayout.addWidget( self.textLabel_pwQuestion )
      
      # defining password line edit widget
      self.lineEdit_password = qt.QLineEdit( self, "lineEdit_password_%s" % name )
      self.lineEdit_password.setSizePolicy( qt.QSizePolicy( qt.QSizePolicy.Expanding, qt.QSizePolicy.Fixed, 1, 0, self.lineEdit_password.sizePolicy().hasHeightForWidth() ) )
      self.lineEdit_password.setCursor( qt.QCursor( 4 ) )
      self.lineEdit_password.setAcceptDrops( 0 )
      self.lineEdit_password.setEchoMode( qt.QLineEdit.Password )
      self.lineEdit_password.setDragEnabled( 0 )
      tabLayout.addWidget( self.lineEdit_password )

      # Add all widgets corresponding to the credential's parameters to the tab.
      combinedPrefList = widget_set.getGUIConfig( gangaObj = credObj, ignoreComponentItems = True )
      widgetList = widget_set.buildGUI( self, combinedPrefList )
      self.propertyWidgetDict = dict( zip( [ combinedPref[ 'attribute' ] for combinedPref in combinedPrefList ], widgetList ) )

   def getAttributeValue( self, attr ):
      return self.propertyWidgetDict[ attr ].get()

   def update( self ):
      for attr in self.propertyWidgetDict:
         attrVal = self.getAttributeValue( attr )
         if attrVal != getattr( self.credObj, attr ): # change of attribute value
            setattr( self.credObj, attr, attrVal )

   def setPW( self ):
      self.__pw = str( self.lineEdit_password.text() )
      self.lineEdit_password.clear()
   
   def getPW( self ):
      if self.isPWAvail():
         if not self.autorenew:
            __pw = self.__pw[:]
            self.clearPW()
            return __pw
         return self.__pw
      else:
         return ''
   
   def clearPW( self ):
      self.__pw = ''
   
   def isPWAvail( self ):
      return bool( self.__pw )


class Credentials_Dialog( Credentials_Dialog_BASE ):
   def __init__( self, parent = None, credDict = {} ):
      Credentials_Dialog_BASE.__init__( self, parent )
      self.tabWidget_Credentials.removePage( self.tab )
      self.pushButton_Destroy.hide()
      self.credAttributeDict = {}
      self.credObj = None
      self.done = False # Flag to force getPassword() to wait until ask() has completed (i.e. until a password has been provided by the user.)
      self.CM_RLock = threading.RLock() # To ensure that ask() and getPassword() pairs originate from the same thread.
      for credObj in credDict.itervalues():
         # proxy object used to allow widget_set.getGUIConfig() to work.
         self._setCred( credObj._proxyObject )

   def minimumSizeHint( self ):
      return qt.QSize( 300,200 )

   def customEvent( self, myEvent ):
      if myEvent.type() == CMDIALOG_EVENT:
         self.ask1( myEvent.credObj )

   def _setCred( self, credObj ):
      if credObj not in self.credAttributeDict:
         self.credAttributeDict[ credObj ] = Credential_Tab( self.tabWidget_Credentials, credObj )
         self.tabWidget_Credentials.insertTab( self.credAttributeDict[ credObj ], credObj._impl._name )
         self.credAttributeDict[ credObj ].show()
         self.connect( self.credAttributeDict[ credObj ].lineEdit_password, qt.SIGNAL( "returnPressed()" ), self.pushButton_Renew, qt.SIGNAL( "clicked()" ) )
      else:
         self.credAttributeDict[ credObj ].update()

   def getAttributeValue( self, credObj, attr ):
      return self.credAttributeDict[ credObj ].getAttributeValue( attr )
   
   def getPassword( self, credObj ):
      # To retrieve the password, the dialog has to be locked once by the same thread.
      if not self.CM_RLock._is_owned():
         log.debug( "getPassword(): Aborting. ask() was called from different thread. [%s]" % self.CM_RLock._RLock__count )
         return ''
      while self.done == False:
         # Waiting for ask() to provide password.
         time.sleep( 1 )
      __pw = self.credAttributeDict[ credObj ].getPW()
      if not __pw:
         log.debug( "getPassword(): Password is empty. [%s]" % self.CM_RLock._RLock__count )
      self.CM_RLock.release()
      return __pw

   def isMonitoringEnabled( self, credObj ):
      return self.credAttributeDict[ credObj ].enabled

   def isAutoRenew( self, credObj ):
      return self.credAttributeDict[ credObj ].autorenew

   def isMainThread( self ):
      return threading.currentThread().getName() == 'MainThread'

   def ask1( self, credObj ):
      self._setCred( credObj )
      self.tabWidget_Credentials.setCurrentPage( self.tabWidget_Credentials.indexOf( self.credAttributeDict[ credObj ] ) )
      if self.exec_loop() == qt.QDialog.Accepted:
         log.debug( "ask1(): CM dialog accepted." )
         self.done = True
      else:
         log.debug( "ask1(): CM dialog not accepted!" )
      return

   def ask( self, credObj = None ):
      if self.CM_RLock._is_owned():
         log.debug( "ask(): CM dialog already called (probably invoked from menu bar). Proceeding to getPassword()." )
         return True
      if not self.CM_RLock.acquire(0):
         log.debug( "ask(): CM_RLock could not be acquired [%s]" % self.CM_RLock._RLock__count )
         return False
      log.debug('ask(): CM_RLock acquired [%s]' % self.CM_RLock._RLock__count )
      if credObj is None:
         if self.credAttributeDict:
            credObj = self.credAttributeDict.keys()[-1]
         else:
            from GangaGUI.miscDialogs import warningMessage
            warningMessage( None, "No credentials available!" )
            self.CM_RLock.release()
            return False
      if self.isMainThread():
         log.debug( "ask(): CM dialog invocation requested by MainThread (e.g. by user from the menu bar)" )
         self.ask1( credObj )
         return
      else: # Called from credential checking thread
         log.debug( "ask(): CM dialog invocation requested by credential checking thread." )
         try:
            tab = self.credAttributeDict[ credObj ]
         except KeyError:
            pass
         else:
            # cred object disabled. Returns if calling thread is not the main one.
            if not tab.enabled:
               self.CM_RLock.release()
               return False
            # renew cred object automatically provided the password is available
            if tab.autorenew and tab.isPWAvail():
               self.done = True
               return True
         self.done = False
         qt.QApplication.postEvent( self, CMDialog_CustomEvent( credObj ) )
         return True

   def renewalStatus( self, credObj, status ):
      # Prevents auto updates when previous credential renewal failed.
      tab = self.credAttributeDict[ credObj ]
      if status!=0 and tab.autorenew:
         tab.autorenew = False
      log.debug( "renewalStatus(): Releasing CM_RLock. [%s]" % self.CM_RLock._RLock__count )
      self.CM_RLock.release()

   def slotRenew( self ):
      tab = self.tabWidget_Credentials.currentPage()
      tab.setPW()
      tab.update()
      if self.CM_RLock._RLock__owner.getName() == 'MainThread':
         self.done = True
         if not tab.credObj.renew( check = False ):
            from GangaGUI.miscDialogs import warningMessage
            warningMessage( None, "%s renewal not successful." % tab.name )
      self.accept()

   def slotClose( self ):
      for tab in self.credAttributeDict.itervalues():
         if not tab.autorenew:
            tab.clearPW()
      if self.CM_RLock._RLock__owner.getName() == 'MainThread':
         log.debug( "slotClose(): Closing CM dialog. Releasing CM_RLock. [%s]" % self.CM_RLock._RLock__count )
         self.CM_RLock.release()
      self.accept()

   def slotAutoRenew( self, enabled ):
      tab = self.tabWidget_Credentials.currentPage()
      tab.autorenew = enabled
      if enabled:
         if not self.checkBox_credMonitoring.isChecked():
            self.checkBox_credMonitoring.setChecked( enabled )
      else:
         tab.clearPW()

   def slotCredMonitoring( self, enabled ):
      tab = self.tabWidget_Credentials.currentPage()
      tab.enabled = enabled
      if not enabled and self.checkBox_autorenew.isChecked():
         self.checkBox_autorenew.setChecked( enabled )
   
   def slotDestroy( self ):
      pass
   
   def slotUpdateButtons( self, currentTab ):
      self.checkBox_credMonitoring.setChecked( currentTab.enabled )
      self.checkBox_autorenew.setChecked( currentTab.autorenew )
      self.textLabel_AttentionMsg.setText( "%s will expire in %s." % ( currentTab.name, currentTab.credObj.timeleft() ) )
      currentTab.lineEdit_password.setFocus()
