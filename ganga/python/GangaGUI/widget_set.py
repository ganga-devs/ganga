import os.path
from qt import *
from GangaGUI.inputline_widget_BASE import InputLine_Widget_BASE
from GangaGUI.inputlist_widget_BASE import InputList_Widget_BASE
from GangaGUI.inputchoice_widget_BASE import InputChoice_Widget_BASE
from GangaGUI.file_association_BASE import File_Association_BASE
from GangaGUI import miscDialogs, inspector, Ganga_Errors, customGUIManager
from Ganga.GPIDev.Lib.GangaList import *
# Setup logging ---------------
import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger( "Ganga.widget_set" )

class TypeValidator( object ):
   def __init__( self, validator = lambda x:None, typeList = [] ):
      self.validator = validator
      self.typeList = typeList
   
   def validate( self, val, omitTypeList = [] ):
      if not isinstance( omitTypeList, list ):
         raise Ganga_Errors.TypeException( "Expecting an list for omitTypeList argument" )
      if not self.typeList:
         return True
      if omitTypeList:
         _tl = self.typeList[:]
         for _o in omitTypeList:
            try:
               _tl.remove( _o )
            except:
               continue
         return self.validator( val, _tl )
      return self.validator( val, self.typeList )
   
   def inUse( self ):
      return True


def _makeCustomValidator( tv_Func, typeList ):
   def validator( val ):
      return tv_Func( val, typeList )
   return validator

def getGUIConfig( gangaObj, objPrefix = None, advancedView = None, ignoreComponentItems = False ):
   if not hasattr( gangaObj, '_impl' ):
      return []
   if advancedView is None:
      advancedView = False
   if objPrefix is None:
      objPrefix = gangaObj._impl._name
   combinedPrefList = []
   attributes = {}
   attributes.update( gangaObj._impl._schema.datadict )
   guiPrefs = []
   if advancedView:
      if hasattr( gangaObj._impl, '_GUIAdvancedPrefs' ):
         guiPrefs.extend( gangaObj._impl._GUIAdvancedPrefs )
   else:
      if hasattr( gangaObj._impl, '_GUIPrefs' ):
         guiPrefs.extend( gangaObj._impl._GUIPrefs )
   # Remove entries in attributes where the plugin developer has specified a 
   # preference.

   for guiPref in guiPrefs:
      try:
         attributes.pop( guiPref[ 'attribute' ] )
      except KeyError, msg:
         pass
#         raise Ganga_Errors.InspectionError( "%s %s" % ( msg, objPrefix ) )
   # For the remainder of the attributes, add to the guiPrefs as minimal entries.
   for attribute in attributes:
      guiPrefs.append( { 'attribute': attribute } )
   # Loop over combined list of preferences (includes entries not 
   # specified by the plugin developer as well )
   guiPrefs.reverse()
   for guiPref in guiPrefs:
      # Of the keys specified in the GUI preferences, 'attribute' is complusory.
      if 'attribute' not in guiPref:
         raise Ganga_Errors.InspectionError( "Preference entry in %s missing 'attribute'." % guiPref )
      combinedPref = {}
      currentAttribute = guiPref[ 'attribute' ]
      # Ignore if attribute specified in the GUI preferences is invalid for the
      # gangaObj concerned.
      if not hasattr( gangaObj, currentAttribute  ):
          continue
      _attrObj = gangaObj._impl._schema.datadict[ currentAttribute ]
      _defaultPreferences = _attrObj._meta
      _attrValue = getattr( gangaObj, currentAttribute )
      combinedPref.update( _defaultPreferences )
      combinedPref[ 'currentValue' ] = _attrValue
#      combinedPref[ 'objType' ] = gangaObj._impl._name
      combinedPref[ 'objPrefix' ] = objPrefix + '.' + currentAttribute
      combinedPref[ 'displayLevel' ] = 0
      if isinstance( _attrObj, inspector.Ganga.GPIDev.Schema.ComponentItem ):
         if ignoreComponentItems:
            continue
         if isinstance( _attrObj, inspector.Ganga.GPIDev.Schema.FileItem ):
            if not combinedPref.has_key( 'widget' ) or combinedPref[ 'widget' ] not in WIDGETS_AVAILABLE:
               if combinedPref[ 'sequence' ]:
                  combinedPref[ 'widget' ] = 'File_List'
               else:
                  combinedPref[ 'widget' ] = 'File'
            # Explicitly add typelist info if plugin developer omits them.
            combinedPref.setdefault( 'typelist', [ 'Ganga.GPIDev.Lib.File.File.File', 'str' ] )
            if 'Ganga.GPIDev.Lib.File.File.File' not in combinedPref[ 'typelist' ]:
               combinedPref[ 'typelist' ] .append( 'Ganga.GPIDev.Lib.File.File.File' )
            if 'str' not in combinedPref[ 'typelist' ]:
               combinedPref[ 'typelist' ] .append( 'str' )
            combinedPref.update( guiPref )
            combinedPrefList.append( combinedPref )         
         else: # Complex item is not a FileItem
            combinedPref[ 'choices' ] = inspector.plugins( combinedPref[ 'category' ] )
            combinedPref.update( guiPref )
            if combinedPref[ 'sequence' ]: # a complex item sequence
               combinedPref[ 'widget' ] = 'ItemChoice_Add'
               # This should serve as a complex item marker.
               combinedPrefList.append( combinedPref )
               if not hasattr( _attrValue, '__iter__' ):
                  continue
               pos = 0
               for entry in _attrValue:
                  combinedPref1 = {}
                  combinedPref1.update( combinedPref )
                  combinedPref1[ 'currentValue' ] = entry
#                  combinedPref1[ 'objType' ] = gangaObj._impl._name
                  combinedPref1[ 'objPrefix' ] = "%s.%s[%d]" % ( objPrefix, currentAttribute, pos )
                  combinedPref1[ 'widget' ] = 'Item_Choice'
                  combinedPrefList.append( combinedPref1 )
                  combinedPrefList.extend( getGUIConfig( entry, "%s.%s[%d]" % ( objPrefix, currentAttribute, pos ), advancedView ) )
                  pos += 1
            else: # a single complex item i.e. not a list of complex items
               if combinedPref[ 'optional' ]:
                  combinedPref[ 'choices' ].append( combinedPref[ 'defvalue' ] )
               combinedPref[ 'widget' ] = 'Item_Choice'
               # This should serve as a complex item marker.
               combinedPrefList.append( combinedPref ) 
               if _attrValue: # recurse if not None
                  combinedPrefList.extend( getGUIConfig( _attrValue, "%s.%s" % ( objPrefix, currentAttribute ), advancedView ) )
      elif isinstance( _attrObj, inspector.Ganga.GPIDev.Schema.SimpleItem ):   
         if isinstance( _attrValue, bool ):
            combinedPref[ 'widget' ] = 'Bool'
            combinedPref[ 'choices' ] = [ True, False ]
         combinedPref.update( guiPref )
         combinedPrefList.append( combinedPref )
   return combinedPrefList

# Determine widget to use for SimpleItems
def _getPreferredWidget( displayDict ):
   try:
      return displayDict[ 'widget' ]
   except: # Widget not specified. Use heuristics.
      _currValue = displayDict[ 'currentValue' ]
      _defValue = displayDict[ 'defvalue' ]
      try:
         _typeList = displayDict[ 'typelist' ]
      except:
         _typeList = None
      # This case is left there to ensure that if the type system is not fully
      # implemented or developer omits 'typelist' in the Ganga object schema,
      # heuristics will kick in as validate() in widgets will not work.
      if not _typeList:
         if displayDict[ 'sequence' ]: # may be a *_List type widget
            try:
               int( _currValue[ 0 ] )
            except ( IndexError, TypeError ): # empty list or list of wrong type
               if _defValue is None: # This cannot be None.
                  return 'String_List'
               else:
                  try:
                     int( _defValue[ 0 ] )
                  except:
                     return 'String_List'
                  else:
                     if isinstance( _defValue[ 0 ], str ):
                        return 'String_List'
                     return 'Int_List'
            except ValueError: # a string list
               return 'String_List'
            else:
               if isinstance( _currValue[ 0 ], str ):
                  return 'String_List'
               return 'Int_List'

         elif isinstance( _currValue, dict ):
            return 'DictOfString'

         elif isinstance( _currValue, bool ):
            return 'Bool'

         else: # non-list widget
            if _currValue is None:
               if _defValue is None or isinstance( _defValue, str ):
                  return 'String'
               return 'Int'
            else:
               try:
                  int( _currValue )
               except: # It's a string
                  return 'String'
               else:
                  if isinstance( _currValue, str ):
                     return 'String'
                  return 'Int'

      # _typeList is defined and not an empty list.
      # String widgets are used. With the existance of the Type System, it is
      # possible to rely on validate() in String widgets to detect valid types.
      else: 
         if displayDict[ 'sequence' ]: # may be a *_List type widget
            return 'String_List'
         elif isinstance( _currValue, dict ):
            return 'DictOfString'
         elif isinstance( _currValue, bool ):
            return 'Bool'
         else: # non-list widget
            return 'String'
         
def buildGUI( parent, displayDictList, mCallback = None, typeValidator = None ):
   _widgetList = []
   LayoutWidget = QWidget( parent, "_wLayout")
   _wLayout = QVBoxLayout( LayoutWidget, 0, 0,"_wLayout")
   for displayDict in displayDictList:
      if displayDict[ 'hidden' ]:
         continue
      preferredWidget = WIDGETS_AVAILABLE[ _getPreferredWidget( displayDict ) ]
      widgetArgFullDict = {  'textLabel'    : 'objPrefix',
                             'choices'      : 'choices',
                             'default'      : 'currentValue',
                             'defvalue'     : 'defvalue',
                             'protected'    : 'protected',
                             'tooltip'      : 'doc', 
                             'command'      : 'objPrefix', 
                             'displayLevel' : 'displayLevel',
                             'customgui'    : 'customgui' }
      widgetArgDict = {}
      for arg, dictKey in widgetArgFullDict.iteritems():
         try:
            widgetArgDict[ arg ] = displayDict[ dictKey ]
         except KeyError:
            log.debug( "_GUIPrefs entry for the %s attribute does not contain the %s key." % ( displayDict[ 'attribute' ], dictKey ) )
      widgetArgDict[ 'parent' ] = LayoutWidget
      widgetArgDict[ 'modifiedCallback' ] = mCallback
      if typeValidator is None:
         try:
            tList = displayDict[ 'typelist' ] 
         except KeyError:
            tList = []
         typeValidator = TypeValidator( Ganga.GPIDev.Base.Proxy.valueTypeAllowed, tList )
      widgetArgDict[ 'typeValidator' ] = typeValidator
      _w = preferredWidget( **widgetArgDict )
      _wLayout.addWidget( _w )
      _widgetList.append( _w )
   _wLayout.addItem( QSpacerItem( 21, 20, QSizePolicy.Minimum, QSizePolicy.Expanding ) )
   parent.layout().addWidget( LayoutWidget )
   return _widgetList

def guiDialog( parent, displayDictList, mCallback = None, caption = '' ):
   _widgetList = []
   gDialog = QDialog( parent, "dialogLayout", 1, 0 )
   gDialog.setModal(1)
   gDialog.setSizeGripEnabled(0)
   gDialog.setCaption( caption )
   dialogLayout = QVBoxLayout( gDialog, 0, 0,"dialogLayout")
   for displayDict in displayDictList:
      if displayDict[ 'hidden' ]:
         continue
      _w = WIDGETS_AVAILABLE[ _getPreferredWidget( displayDict ) ]( parent = gDialog, textLabel = displayDict[ 'objPrefix' ], default = displayDict[ 'currentValue' ], defvalue = displayDict[ 'defvalue' ], protected = displayDict[ 'protected' ], tooltip = displayDict[ 'doc' ], command = displayDict[ 'objPrefix' ], displayLevel = displayDict[ 'displayLevel' ], modifiedCallback = mCallback )
      dialogLayout.addWidget( _w )
      _widgetList.append( _w )
   buttonLayout = QHBoxLayout(None,0,6,"buttonLayout")
   buttonSpacer = QSpacerItem(10,0,QSizePolicy.Expanding,QSizePolicy.Minimum)
   buttonLayout.addItem( buttonSpacer )
   pushButton_Cancel = QPushButton( gDialog, "pushButton_Cancel" )
   pushButton_Cancel.setText( "Cancel" )
   buttonLayout.addWidget( pushButton_Cancel )
   pushButton_Ok = QPushButton( gDialog, "pushButton_Ok" )
   pushButton_Ok.setText( "OK" )
   buttonLayout.addWidget( pushButton_Ok )
   dialogLayout.addLayout( buttonLayout )
   QObject.connect( pushButton_Ok, SIGNAL("clicked()"), gDialog.accept )
   QObject.connect( pushButton_Cancel, SIGNAL("clicked()"), gDialog.reject )
   return gDialog, _widgetList


class InputLine_Widget( InputLine_Widget_BASE ):
   def __init__( self,
                 parent = None, 
                 name = None, 
                 textLabel = '',
                 default = '',
                 defvalue = '',
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputLine_Widget_BASE.__init__( self, parent, name, fl )
      if modifiedCallback is None:
         modifiedCallback = lambda:None
      self.modifiedCallback = modifiedCallback
      if typeValidator is None:
         typeValidator = TypeValidator()
      self.typeValidator = typeValidator
      self.textLabel.setText( str( textLabel ) )
      self.pushButton_Browse.hide()
      self.pushButton_Edit.hide()
      if customgui is None:
         self.pushButton_Custom.hide()
      else:
         self.pushButton_Custom.setText( customGUIManager.customGUI_Dict[ customgui ].label )
      self.default = default
      self.defvalue = defvalue
      self.command = command
      self.protected = protected
      self.customgui = customgui
      self.slotRevert()
      QToolTip.add( self.lineEdit, tooltip )
      self.setButtons( not protected )
      
      self.connect( self.lineEdit, SIGNAL("textChanged(const QString&)"), modifiedCallback )
      
      if self.typeValidator.inUse():
         self.connect( self.lineEdit, SIGNAL("returnPressed()"), self.validate )
         self.connect( self.lineEdit, SIGNAL("lostFocus()"), self.validate )

   def minimumSizeHint( self ):
      return QSize( 380, 100 )

   def setButtons( self, enabled ):
      self.lineEdit.setEnabled( enabled )
      self.pushButton_Revert.setEnabled( enabled )
      self.pushButton_Clear.setEnabled( enabled )
      if enabled:
         self.pushButton_Revert.show()
         self.pushButton_Clear.show()
      else:
         self.pushButton_Revert.hide()
         self.pushButton_Clear.hide()

   def validate( self ):
      if not self.get( True ):
         self.lineEdit.selectAll()

   def get( self, validateOnly = False ):
      _str = str( self.lineEdit.text() ).strip()
      try:
         float( _str ) 
      except:
         if _str.lower() in [ 'true', 'false', 'none' ]:
            _str =  _str.capitalize()
         elif not _str or _str[0] not in ['"',"'"]:
            _str = '"%s"' % _str
      
      try:
         _val = eval( _str )
      except:
         if validateOnly:
            log.error( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.textLabel.text() ), _str ) )
            return False
         else:
            raise Ganga_Errors.TypeException( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.textLabel.text() ), _str ) )

      if self.typeValidator.validate( _val ):
         if validateOnly:
            return True
         else:
            return _val
      if validateOnly:
         log.error( "%s: Type validation failed. Valid types are: %s" % ( str( self.textLabel.text() ), self.typeValidator.typeList ) )
         return False
      raise Ganga_Errors.TypeException( "%s: Type validation failed. Valid types are: %s" % ( str( self.textLabel.text() ), self.typeValidator.typeList ) )

   def slotRevert( self ):
      quote = ''
      if isinstance( self.default, str ):
         try:
            float( self.default )
         except:
            pass
         else:
            quote = '"'
      self.lineEdit.setText( "%s%s%s" % ( quote, str( self.default ), quote ) )
      self.lineEdit.setFocus()
      
   def slotBrowse( self ):
      pass
   
   def slotClear( self ):
      self.lineEdit.clear()
      self.lineEdit.setFocus()

   def slotCustom( self, modal = True ):
      customGUIManager.customGUI_Dict[ self.customgui ].recipient = self.lineEdit.setText
      customGUIManager.customGUI_Dict[ self.customgui ].show()


class InputFile_Widget( InputLine_Widget ):
   def __init__( self,
                 parent = None, 
                 name = None, 
                 textLabel = 'Filename:',
                 default = None,
                 defvalue = None, 
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputLine_Widget.__init__( self, parent, name, textLabel, default, defvalue, protected, tooltip, command, displayLevel, modifiedCallback, typeValidator, customgui, fl )
      if not protected:
         self.connect( self.pushButton_Edit, SIGNAL( 'clicked()' ), self.slotEdit )
         self.pushButton_Browse.setEnabled( True )
         self.pushButton_Browse.show()
#         if displayLevel > 0:
         self.pushButton_Edit.setEnabled( True )
         self.pushButton_Edit.show()

   def slotBrowse( self ):
      fSelected = QFileDialog.getOpenFileName( "", "" , self, "BrowseFile", "Select a file." )
      if fSelected:
         self.lineEdit.setText( fSelected )
         self.lineEdit.setFocus()
         self.__updateFileObj()
         
   def __updateFileObj( self, existingFile = None ):
      if existingFile is None:
         existingFile = False
      fname = str( self.lineEdit.text() )
      if existingFile:
         if fname and not os.path.isfile( os.path.expandvars( fname ) ):
            raise Ganga_Errors.UpdateException( "%s does not exist" % fname )
      self.currFileObj.name = fname

   def __Str2File( self, fname ):
      """
      Converts a filename to a File object.
      This function assumes knowledge of the structure of the File object.
      """
      if isinstance( fname, inspector.File ):
         return fname
      fObj = inspector.File()
      if fname:
         fObj.name = fname
      return fObj

   def __File2Str( self, fObj ):
      """
      Reverse function of __Str2File().
      """
      if isinstance( fObj, str ):
         return fObj
      if not fObj.name:
         return ''
      return fObj.name #os.path.join( fObj.subdir, fObj.name )

   def get( self, validateOnly = False ):
      try:
         self.__updateFileObj( True )
      except Ganga_Errors.UpdateException, msg:
         if miscDialogs.warningDialog( None, "Error trying to update\n%s\n%s.\nIgnore and carry on?" % ( self.command, msg ) ) == 1:
            raise
         else:
            self.__updateFileObj( False )
      if 'Ganga.GPIDev.Lib.File.File.File' not in self.typeValidator.typeList:
         log.debug( "%s unable to accept a Ganga File object when it should! Returning a list of filenames instead." % self.command )
         return self.currFileObj.name
      else:
         return self.currFileObj

   def slotRevert( self ):
      if isinstance( self.default, str ):
         self.currFileObj = self.__Str2File( self.default )
      elif isinstance( self.default, inspector.File ): # assume default is a File object.
         self.currFileObj = self.default
      else:
         self.currFileObj = inspector.File()
      self.lineEdit.setText( self.__File2Str( self.currFileObj ) )
   
   def slotEdit( self ):
      self.__updateFileObj( False )
      _displayDictList = getGUIConfig( self.currFileObj, self.command[ self.command.rindex('.') + 1: ] )
      editDialog, _widgetList = guiDialog( self, _displayDictList, None, 'Edit file properties' )
      _notDone = True
      while _notDone:
         if editDialog.exec_loop() == QDialog.Accepted:
#            _f = _widgetList[0].get()
#            if _f and not os.path.exists( os.path.expandvars( _f ) ):
#               if miscDialogs.warningDialog( None, "%s does not exist.\nIgnore and carry on?" % _f ) == 1:
#                  [ x.slotRevert() for x in _widgetList ]
#                  continue
            for _w in _widgetList:
               if _w.protected:
                  continue
               _attrStr = _w.command[ _w.command.index('.') + 1: ]
               _fObj = self.currFileObj
               _attrList = _attrStr.split('.')
               for _attr in _attrList[:-1]:
                  _fObj = getattr( _fObj, _attr )
               try:
                  setattr( _fObj, _attrList[ -1 ], _w.get() )
               except Ganga_Errors.TypeException, msg:
                  miscDialogs.warningMessage( None, "Error trying to save:\n%s" % msg )
               except inspector.Ganga.GPIDev.Base.Proxy.ReadOnlyObjectError, msg:
                  miscDialogs.warningMessage( None, "Error trying to save:\n%s" % msg )
            self.lineEdit.setText( self.__File2Str( self.currFileObj ) )
            self.lineEdit.setFocus()
         _notDone = False


class InputList_Widget( InputList_Widget_BASE ):
   def __init__( self,
                 parent = None, 
                 name = None, 
                 textLabel = 'List',
                 default = [],
                 defvalue = [],
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputList_Widget_BASE.__init__( self, parent, name, fl )
      if modifiedCallback is None:
         modifiedCallback = lambda:None
      if typeValidator is None:
         typeValidator = TypeValidator()
      self.typeValidator = typeValidator
      self.listView.header().setLabel( 0, textLabel )
      self.pushButton_Edit.hide()
      self.pushButton_Browse.hide()
      if customgui is None:
         self.pushButton_Custom.hide()
      else:
         self.pushButton_Custom.setText( customGUIManager.customGUI_Dict[ customgui ].label )
      self.default = default
      self.defvalue = defvalue
      self.command = command
      self.protected = protected
      self.modifiedCallback = modifiedCallback
      self.customgui = customgui
      self.fillWithDefault()
      self.listView.setSorting( -1 )
      QToolTip.add( self.listView, tooltip )
      self.setButtons( not protected )

      self.connect( self.listView, SIGNAL( "itemRenamed(QListViewItem*,int)" ), modifiedCallback )

      if self.typeValidator.inUse():
         self.connect( self.listView, SIGNAL( "currentChanged(QListViewItem*)" ), self.validate )
         self.connect( self.listView, SIGNAL( "returnPressed(QListViewItem*)" ), self.validate )

   def minimumSizeHint( self ):
      return QSize( 210,210 )

   def setButtons( self, enabled ):
      self.listView.setEnabled( enabled )
      self.pushButton_Delete.setEnabled( enabled )
      self.pushButton_Insert.setEnabled( enabled )
      self.pushButton_Revert.setEnabled( enabled )
      self.pushButton_Edit.setEnabled( enabled )
      if enabled:
         self.pushButton_Delete.show()
         self.pushButton_Insert.show()
         self.pushButton_Revert.show()
      else:
         self.pushButton_Delete.hide()
         self.pushButton_Insert.hide()
         self.pushButton_Revert.hide()

   def validate( self, lvItem ):
      if lvItem is not None:
         self.get( lvItem, True )
   
   def get( self, lvItem = None, validateOnly = False ):
      _list = []
      if lvItem: # specific item requested
         _i = lvItem
      else:
         _i = self.listView.firstChild()
      while _i:
         _str = str( _i.text( 0 ) ).strip()
         try:
            float( _str ) 
         except:
            if _str.lower() in [ 'true', 'false', 'none' ]:
               _str =  _str.capitalize()
            elif not _str or _str[0] not in ["'",'"']:
               _str = '"%s"' % _str

         try:
            _val = eval( _str )
         except:
            if validateOnly:
               log.error( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.listView.header().label( 0 ) ), _str ) )
               return False # First error will stop validation.
            else:
               raise Ganga_Errors.TypeException( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.listView.header().label( 0 ) ), _str ) )

         if self.typeValidator.validate( _val ):
            if validateOnly:
               if lvItem:
                  return True
            else:
               if lvItem:
                  return _val
               _list.append( _val )
         else:
            if validateOnly:
               log.error( "%s: Type validation failed.\n%s not one of the following types: %s" % ( str( self.listView.header().label( 0 ) ), _val, self.typeValidator.typeList ) ) 
               return False # First error will stop validation.
            else:
               raise Ganga_Errors.TypeException( "%s: Type validation failed.\n%s not one of the following types: %s" % ( str( self.listView.header().label( 0 ) ), _val, self.typeValidator.typeList ) )
         _i = _i.nextSibling()

      # No need to test for single lvItem as all cases dealt with above.
      if validateOnly:
         return True
      else:
         return _list
   
   def fillWithDefault( self ):
      self.listView.clear()
      for _p in xrange( len( self.default ) - 1, -1, -1 ):
         val = self.default[ _p ]
         quote = ""
         if isinstance( self.default, str ):
            try:
               float( self.default )
            except:
               pass
            else:
               quote = '"'

         self.slotInsert( "%s%s%s" % ( quote, val, quote ) )

   def slotInsert( self, itemText = None ):
      editNow = False
      if itemText is None:
         editNow = True
         itemText = 'New item'
      lastItem = self.listView.lastItem()
      if isinstance( itemText, list ) or isinstance( itemText, GangaList ): # support use of GangaList
         for itemStr in itemText:
            item = QListViewItem( self.listView )
            item.setMultiLinesEnabled( True )
            item.setText( 0, str( itemStr ) )
      else:
         item = QListViewItem( self.listView )
         item.setMultiLinesEnabled( True )
         item.setText( 0, str( itemText ) )
         item.setRenameEnabled( 0, True )

      # Insert in a specific position may be important for certain attributes e.g. command sequences?
      if editNow:
         currItem = self.listView.currentItem()
         if currItem and currItem.isSelected():
            item.moveItem( currItem )
         else:
            item.moveItem( lastItem )
         self.listView.clearSelection()
         item.startRename( 0 )
      self.modifiedCallback()
   
   def slotDelete(self):
      _i = self.listView.firstChild()
      while _i:
         if _i.isSelected(): 
            _j = _i.nextSibling()
            self.listView.takeItem( _i )
            del _i
            _i = _j
         else:
            _i = _i.nextSibling()
      self.listView.setSelected( self.listView.currentItem(), True )
      self.modifiedCallback()

   def slotRevert(self):
      if miscDialogs.warningDialog( None, "Current list will be removed! Proceed?", 'Revert -' + str( self.listView.header().label( 0 ) ) ) == 0:
         self.fillWithDefault()
         self.modifiedCallback()

   def slotCustom( self, modal = True ):
      customGUIManager.customGUI_Dict[ self.customgui ].recipient = self.slotInsert
      customGUIManager.customGUI_Dict[ self.customgui ].show()


class InputFileList_Widget( InputList_Widget ):
   def __init__( self,
                 parent = None, 
                 name = None, 
                 textLabel = 'File Dict',
                 default = [],
                 defvalue = [],
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      self.currFileDict = {}
      InputList_Widget.__init__( self, parent, name, textLabel, default, defvalue, protected, tooltip, command, displayLevel, modifiedCallback, typeValidator, customgui, fl )
      if not protected:
         self.connect( self.pushButton_Edit, SIGNAL( 'clicked()' ), self.slotEdit )
#         if displayLevel > 0:
         self.pushButton_Edit.show()
         self.pushButton_Browse.show()

   def get( self, lvItem = None, validateOnly = False ):
#      try:
#         fDict = self.__updateFileDict( existingFile = True )
#      except Ganga_Errors.UpdateException, msg:
#         if miscDialogs.warningDialog( None, "Error trying to update\n%s\n%s.\nIgnore and carry on?" % ( self.command, msg ) ) == 1:
#            raise
#         else:
#            fDict = self.__updateFileDict( existingFile = False )
      fDict = self.__updateFileDict()
      if 'Ganga.GPIDev.Lib.File.File.File' not in self.typeValidator.typeList:
         log.error( "%s unable to accept a Ganga File object as an entry when it should!" % self.command )
         return fDict.keys()
      else:
         return fDict.values()

   def fillWithDefault( self ):
      if isinstance( self.default, list ):
         self.default = makeGangaListByRef( self.default )
      self.currFileDict.clear()
      for i in self.default:
         if isinstance( i, str ):
            i = i.strip()
            self.currFileDict[ i ] = self.__Str2File( i )
         elif isinstance( i, inspector.File ):
            self.currFileDict[ self.__File2Str( i ) ] = i
      self.listView.clear()
      self.slotInsert( self.currFileDict )

   def slotInsert( self, fDict = None ):
      editNow = False
      lastItem = self.listView.lastItem()
      if fDict is None:
         editNow = True
         item = QListViewItem( self.listView )
         item.setMultiLinesEnabled( True )
         item.setText( 0, 'New file item' )
         item.setRenameEnabled( 0, True )
      else:
         for fName in self.__updateFileDict( fDict ):
            item = QListViewItem( self.listView )
            item.setMultiLinesEnabled( True )
            item.setText( 0, fName )
            item.setRenameEnabled( 0, True )

      if editNow:
         currItem = self.listView.currentItem()
         if currItem and currItem.isSelected():
            item.moveItem( currItem )
         else:
            item.moveItem( lastItem )
         self.listView.clearSelection()
         item.startRename( 0 )

      self.modifiedCallback()

   def slotBrowse( self ):
      fDict = {}
      selectedList = QFileDialog.getOpenFileNames( "", "" , self, "BrowseFiles", "Select one or more files." )
      for f in selectedList:
         f = str( f ).strip()
         if f not in self.currFileDict:
            fDict[ f ] = self.__Str2File( f )
      self.slotInsert( fDict )

   def slotRevert( self ):
      if miscDialogs.warningDialog( None, "Current selection will be removed! Proceed?" ) == 0:
         self.fillWithDefault()
         self.modifiedCallback()

   def slotEdit( self ):
      try:
         _currItemStr = str( self.listView.currentItem().text( 0 ) ).strip()
      except AttributeError:
         return
      self.__updateFileDict( { _currItemStr : self.currFileDict[ _currItemStr ] }, False )
      _displayDictList = getGUIConfig( self.currFileDict[ _currItemStr ], self.command[ self.command.rindex('.') + 1: ] )
      editDialog, _widgetList = guiDialog( self, _displayDictList, None, 'Edit file properties' )
      _notDone = True
      while _notDone:
         if editDialog.exec_loop() == QDialog.Accepted:
#            _f = _widgetList[0].get()
#            if _f and not os.path.exists( os.path.expandvars( _f ) ):
#               if miscDialogs.warningDialog( None, "%s does not exist.\nIgnore and carry on?" % _f ) == 1:
#                  [ x.slotRevert() for x in _widgetList ]
#                  continue
            for _w in _widgetList:
               if _w.protected:
                  continue
               _attrStr = _w.command[ _w.command.index('.') + 1: ]
               _fObj = self.currFileDict[ _currItemStr ]
               _attrList = _attrStr.split('.')
               for _attr in _attrList[:-1]:
                  _fObj = getattr( _fObj, _attr )
               try:
                  setattr( _fObj, _attrList[ -1 ], _w.get() )
               except Ganga_Errors.TypeException, msg:
                  miscDialogs.warningMessage( None, "Error trying to save:\n%s" % msg )
               except inspector.Ganga.GPIDev.Base.Proxy.ReadOnlyObjectError, msg:
                  miscDialogs.warningMessage( None, "Error trying to save:\n%s" % msg )
            _newStr = self.__File2Str( _fObj )
            self.currFileDict[ _newStr ] = self.currFileDict.pop( _currItemStr )
            self.listView.currentItem().setText( 0, _newStr )
         _notDone = False

   def slotDelete( self ):
      _i = self.listView.firstChild()
      while _i:
         if _i.isSelected():
            del self.currFileDict[ str( _i.text( 0 ) ) ]
            _j = _i.nextSibling()
            self.listView.takeItem( _i )
            del _i
            _i = _j
         else:
            _i = _i.nextSibling()
      self.listView.setSelected( self.listView.currentItem(), True )
      self.modifiedCallback()

   def __updateFileDict( self, fDict = None, existingFile = None ):
      if existingFile is None:
         existingFile = False
      if fDict is None: # Check Listview for changes and update internal dictionary
         fDict = {}
         removeList = self.currFileDict.keys() # start list of keys to remove. 
         _i = self.listView.firstChild()
         while _i:
            _iStr = str( _i.text( 0 ) ).strip()
            if existingFile:
               if _iStr and not os.path.exists( os.path.expandvars( _iStr ) ):
                  raise Ganga_Errors.UpdateException( "%s does not exist" % _iStr )
            self.currFileDict.setdefault( _iStr, self.__Str2File( _iStr ) )
            try:
               removeList.remove( _iStr )
            except ValueError:
               pass 
            _i = _i.nextSibling()
         for _i in removeList: # remove keys not found in the Listview
            del self.currFileDict[ _i ]
         return self.currFileDict
      # fDict was provided. Add only valid entries.
      for f in fDict.keys():
         if existingFile:
            if not os.path.isfile( os.path.expandvars( f ) ):
               del fDict[ f ]
               continue
      self.currFileDict.update( fDict )
      return fDict

   def __Str2File( self, fname ):
      """
      Converts a filename to a File object.
      This function assumes knowledge of the structure of the File object.
      """
      if isinstance( fname, inspector.File ):
         return fname
      fObj = inspector.File()
      if fname:
         fObj.name = fname
      return fObj

   def __File2Str( self, fObj ):
      """
      Reverse function of __Str2File().
      """
      if isinstance( fObj, str ):
         return fObj
      if not fObj.name:
         return ''
      return fObj.name


class InputDict_Widget( InputList_Widget ):
   def __init__( self,
                 parent = None, 
                 name = None, 
                 textLabel = 'File List',
                 default = {},
                 defvalue = {},
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 newEntryFormat = None,
                 customgui = None,
                 fl = 0 ):
      if newEntryFormat is None:
         newEntryFormat = 'key : value'
      self.newEntryFormat = newEntryFormat
      InputList_Widget.__init__( self, parent, name, textLabel, default, defvalue, protected, tooltip, command, displayLevel, modifiedCallback, typeValidator, customgui, fl )

   def get( self, lvItem = None, validateOnly = False ):
      _dict = {}
      if lvItem: # specific item requested
         _i = lvItem
      else:
         _i = self.listView.firstChild()
      while _i:
         _str = str( _i.text( 0 ) ).strip()
         if _str:
            try:
               _pos = _str.index( ':' )
            except ValueError:
               raise Ganga_Errors.TypeException( 'Expecting entries in [x:y] format for %s.' % str( self.listView.header().label(0) ) )
            _dKey = _str[ :_pos ].strip()
            _dVal = _str[ _pos+1: ].strip()
         else: # empty entry
            _i = _i.nextSibling()
            continue

         try:
            float( _dKey ) 
         except:
            if not _dKey or _dKey[0] not in ["'",'"']:
               _dKey = '"%s"' % _dKey

         try:
            float( _dVal ) 
         except:
            if _dVal.lower() in [ 'true', 'false', 'none' ]:
               _dVal =  _dVal.capitalize()
            elif not _dVal or _dVal[0] not in ["'",'"']:
               _dVal = '"%s"' % _dVal

         # Pre-validation test
         try:
            _key = eval( _dKey )
            _val = eval( _dVal )
         except:
            if validateOnly:
               log.error( "%s: Pre-validation eval failed. %s or %s not valid Python syntax." % ( str( self.listView.header().label( 0 ) ), _dKey, _dVal ) )
               return False # First error will stop validation.
            else:
               raise Ganga_Errors.TypeException( "%s: Pre-validation eval failed. %s or %s not valid Python syntax." % ( str( self.listView.header().label( 0 ) ), _dKey, _dVal ) )

         if self.typeValidator.validate( _key ) and self.typeValidator.validate( _val ):
            if validateOnly:
               if lvItem:
                  return True
            else:
               if lvItem:
                  return ( _key, _val )
               _dict[ _key ] = _val
         else:
            if validateOnly:
               log.error( "%s: Type validation failed.\n%s or %s not one of the following types: %s" % ( str( self.listView.header().label( 0 ) ), _key, _val, self.typeValidator.typeList ) ) 
               return False # First error will stop validation.
            else:
               raise Ganga_Errors.TypeException( "%s: Type validation failed.\n%s or %s not one of the following types: %s" % ( str( self.listView.header().label( 0 ) ), _key, _val, self.typeValidator.typeList ) )
         _i = _i.nextSibling()

      # No need to test for single lvItem as all cases dealt with above.
      if validateOnly:
         return True
      else:
         return _dict
      
   def fillWithDefault( self ):
      self.listView.clear()
      for _f in self.default:
         _kQuote = ''
         _vQuote = ''
         val = self.default[ _f ]

         if isinstance( val, str ):
            try:
               float( val )
            except:
               pass
            else:
               _vQuote = '"'

         if isinstance( _f, str ):
            try:
               float( _f )
            except:
               pass
            else:
               _kQuote = '"'

         self.slotInsert( '%s%s%s : %s%s%s' % ( _kQuote, _f, _kQuote, _vQuote, val, _vQuote ) )

   def slotInsert( self, itemText = None ):
      editNow = False
      if itemText is None:
         editNow = True
         itemText = self.newEntryFormat
      if isinstance( itemText, list ) or isinstance( itemText, GangaList ):
         for itemStr in itemText:
            item = QListViewItem( self.listView )
            item.setText( 0, str( itemStr ) )
      else:
         item = QListViewItem( self.listView )
         item.setText( 0, str( itemText ) )
         item.setRenameEnabled( 0, True )
      if editNow:
         item.startRename( 0 )
      self.modifiedCallback()


class InputChoice_Widget( InputChoice_Widget_BASE ):
   def __init__( self, 
                 parent = None, 
                 name = None, 
                 textLabel = '',
                 choices = [],
                 default = None,
                 defvalue = None,
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputChoice_Widget_BASE.__init__( self, parent, name, fl )
      if modifiedCallback is None:
         modifiedCallback = lambda:None
      self.modifiedCallback = modifiedCallback
      if typeValidator is None:
         typeValidator = TypeValidator()
      self.typeValidator = typeValidator
      self.textLabel.setText( textLabel )
      self.choices = choices
      self.default = default
      self.defvalue = defvalue
      self.command = command
      self.protected = protected
      self.customgui = customgui
      self.fillWithChoices()
      self.slotRevert()
      QToolTip.add( self.comboBox, tooltip )
      self.setButtons( not protected )

      self.connect( self.comboBox, SIGNAL( "activated(int)" ), self.modifiedCallback )
#      self.connect( self.comboBox, SIGNAL( "activated(int)" ), self.typeValidator )

   def minimumSizeHint( self ):
      return QSize( 170, 80 )

   def setButtons( self, enabled ):
      self.comboBox.setEnabled( enabled )
      self.pushButton_Revert.setEnabled( enabled )
      if enabled:
         self.pushButton_Revert.show()
      else:
         self.pushButton_Revert.hide()
   
   def fillWithChoices( self ):
      self.comboBox.clear()
      for stringItem in self.choices:
         self.comboBox.insertItem( QString( str( stringItem ) ) )
   
   def slotRevert( self ):
      if hasattr( self.default, '_impl' ):
         dEntry = self.default._impl._name
      else:
         dEntry = self.default
      try:
         self.comboBox.setCurrentItem( self.choices.index( dEntry ) )
      except ValueError:
         pass

   def get( self, validateOnly = False ):
      _str = str( self.comboBox.currentText() ).strip()
      try:
         float( _str ) 
      except:
         if _str.lower() in [ 'true', 'false', 'none' ]:
            _str =  _str.capitalize()
         elif not _str or _str[0] not in ["'",'"']:
            _str = '"%s"' % _str
      
      try:
         _val = eval( _str )
      except:
         if validateOnly:
            log.error( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.textLabel.text() ), _str ) )
            return False
         else:
            raise Ganga_Errors.TypeException( "%s: Pre-validation eval failed. %s is not valid Python syntax." % ( str( self.textLabel.text() ), _str ) )

      if self.typeValidator.validate( _val ):
         if validateOnly:
            return True
         else:
            return _val
      if validateOnly:
         log.error( "%s: Type validation failed. Valid types are: %s" % ( str( self.textLabel.text() ), self.typeValidator.typeList ) )
         return False
      raise Ganga_Errors.TypeException( "%s: Type validation failed. Valid types are: %s" % ( str( self.textLabel.text() ), self.typeValidator.typeList ) )


class ItemChoice_Widget( InputChoice_Widget ):
   def __init__( self, 
                 parent = None, 
                 name = None, 
                 textLabel = '',
                 choices = [],
                 default = None,
                 defvalue = None,
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputChoice_Widget.__init__( self, parent, name, textLabel, choices, default, defvalue, protected, tooltip, command, displayLevel, modifiedCallback, typeValidator, customgui, fl )
      self.currentItemChoice = str( self.comboBox.currentText() )
      self.pushButton_Revert.hide()
      self.disconnect( self.comboBox, SIGNAL( "activated(int)" ), self.modifiedCallback )
      self.disconnect( self.pushButton_Revert, SIGNAL( "clicked()" ), self.slotRevert )
      self.connect( self.comboBox, SIGNAL( "activated(int)" ), self.slotSwap )

   def slotSwap( self ):
      newChoice = str( self.comboBox.currentText() )
      if self.currentItemChoice != newChoice:
         self.modifiedCallback( self.command, newChoice, 'swap' )
         self.currentItemChoice = newChoice

   def get( self, validateOnly = False ):
      return str( self.comboBox.currentText() ).strip()


class ItemChoiceAdd_Widget( InputChoice_Widget ):
   def __init__( self, 
                 parent = None, 
                 name = None, 
                 textLabel = '',
                 choices = [],
                 default = None,
                 defvalue = None,
                 protected = 0,
                 tooltip = '',
                 command = '',
                 displayLevel = 0,
                 modifiedCallback = None,
                 typeValidator = None,
                 customgui = None,
                 fl = 0 ):
      InputChoice_Widget.__init__( self, parent, name, textLabel, choices, default, defvalue, protected, tooltip, command, displayLevel, modifiedCallback, typeValidator, customgui, fl )
      self.pushButton_Revert.hide()
      self.pushButton_Add = QPushButton( self, "pushButton_Add" )
      self.pushButton_Add.setText( 'Add' )
      self.layout().addWidget( self.pushButton_Add, 2, 2 )
      self.disconnect( self.comboBox, SIGNAL( "activated(int)" ), self.modifiedCallback )
      self.disconnect( self.pushButton_Revert, SIGNAL( "clicked()" ), self.slotRevert )
      self.connect( self.pushButton_Add, SIGNAL( "clicked()" ), self.slotAdd )

   def slotAdd( self ):
      self.modifiedCallback( self.command, str( self.comboBox.currentText() ), 'add' )

   def slotRevert( self ):
      pass

   def get( self, validateOnly = False ):
      return str( self.comboBox.currentText() ).strip()
      

class File_Association( File_Association_BASE ):
   def __init__( self, 
                 parent = None, 
                 name = None, 
                 default = {},
                 modifiedCallback = None,
                 fl = 0 ):
      File_Association_BASE.__init__( self, parent, name, fl )
      if modifiedCallback is None:
         modifiedCallback = lambda:None
      self.default = default
      self.modifiedCallback = modifiedCallback
      self.fillWithDefault()
      self.listView.setSorting( 1, False )
      self.listView.setDefaultRenameAction( QListView.Accept )
      self.setButtons( True )
      self.connect( self.listView, SIGNAL( "itemRenamed(QListViewItem*,int)" ), modifiedCallback )
      self.connect( self.pushButton_Add, SIGNAL( "clicked()" ), self.slotInsert )
      self.connect( self.pushButton_Del, SIGNAL( "clicked()" ), self.slotDelete )

   def minimumSizeHint( self ):
      return QSize( 420,160 )

   def setButtons( self, enabled ):
      self.listView.setEnabled( enabled )
      self.pushButton_Del.setEnabled( enabled )
      self.pushButton_Add.setEnabled( enabled )
      if enabled:
         self.pushButton_Del.show()
         self.pushButton_Add.show()
      else:
         self.pushButton_Del.hide()
         self.pushButton_Add.hide()
   
   def get( self ):
      lvIterator = QListViewItemIterator( self.listView )
      _dict = {}
      while lvIterator.current():
         _i = lvIterator.current()
         _dict[ str( _i.text( 1 ) ) ] = [ str( _i.text( 2 ) ), bool( _i.isOn() ) ]
         lvIterator += 1
      return _dict
   
   def fillWithDefault( self ):
      self.listView.clear()
      for _f in self.default:
         fa_list = [ _f ]
         if isinstance( self.default[ _f ], tuple ) or isinstance( self.default[ _f ], list ):
            fa_list.extend( self.default[ _f ] )
         else: # possibly a default from an older config file.
            fa_list.append( self.default[ _f ] )
            fa_list.append( False )
         self.slotInsert( fa_list )

   def slotInsert( self, fa_list = None ):
      editNow = False
      if fa_list is None:
         fa_list = [ '', '', False ]
         editNow = True
      _ext, _app, _gui = fa_list
      item = QCheckListItem( self.listView, "", QCheckListItem.CheckBox )
      item.setOn( _gui )
      item.setText( 1, _ext )
      item.setText( 2, _app )
      item.setRenameEnabled( 1, True )
      item.setRenameEnabled( 2, True )
      if editNow:
         item.startRename( 1 )
         self.modifiedCallback()

   def slotDelete( self ):
      lvIterator = QListViewItemIterator( self.listView, QListViewItemIterator.Selected )
      while lvIterator.current():
         _i = lvIterator.current()
         self.listView.takeItem( _i )
         del _i
         lvIterator += 1
      self.modifiedCallback()


WIDGETS_AVAILABLE = { 'Int' : InputLine_Widget,
                      'String' : InputLine_Widget,
                      'Bool' : InputChoice_Widget,
                      'DictOfString' : InputDict_Widget,
                      'Dict' : InputDict_Widget,
                      'File' : InputFile_Widget,
                      'FileOrString' : InputFile_Widget,
                      'List' : InputList_Widget,
                      'Int_List' : InputList_Widget,
                      'String_List' : InputList_Widget,
                      'File_List' : InputFileList_Widget,
                      'FileOrString_List' : InputFileList_Widget,
                      'Choice' : InputChoice_Widget,
                      'String_Choice' : InputChoice_Widget,
                      'Int_Choice' : InputChoice_Widget,
                      'Item_Choice' : ItemChoice_Widget,
                      'ItemChoice_Add' : ItemChoiceAdd_Widget }
