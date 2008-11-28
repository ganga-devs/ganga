from GangaGUI.Ganga_Job_BASE import Ganga_Job_BASE
from GangaGUI.widget_set import *
from GangaGUI.customEvents import *
from GangaGUI import miscDialogs, inspector, Ganga_Errors
from GangaGUI.customTables import JobNavListView, JobNavLVItem
from Ganga.Core.exceptions import TypeMismatchError
import qt

class Item2PageMap( object ):
   def __init__( self, item, page, widgetList ):
      self.item = item
      self.page = page
      self.widgetList = widgetList


class Ganga_Job( Ganga_Job_BASE ):
   def __init__( self, parent = None, name = None, caption = '', fl = 0, template_id = None, existingJob = None, makeCopy = None, advancedView = None ):
      Ganga_Job_BASE.__init__( self, parent, name, fl )
      self._parent = parent
      if template_id is None:
         if existingJob:
            if makeCopy:
               self.job = existingJob.copy()
            else: # Open an existing job
               if existingJob.status != 'new':
                  self.job = existingJob.copy()
                  miscDialogs.infoMessage( None, "Job %s already submitted.\nCreated a copy - Job %s" % ( existingJob.id, self.job.id ) )
               else:
                  self.job = existingJob
         else:
            self.job = inspector.Job()
      else: # Ignores if existingJob is specified
         self.job = inspector.Job( inspector.templates( template_id ) )
      self.jobNavLV = JobNavListView( self.frame_JobNavigator )
      self.frame_JobNavigator.layout().addWidget( self.jobNavLV )
      self.__pageIndex = {}

      # Flags
      if advancedView is None:
         advancedView = False
      self.advancedView = advancedView
      self.__modified = False
      self.__submitted = False
      self.__actionInProgress = ""
      
      # Connections
      self.connect( self.jobNavLV, qt.SIGNAL( 'selectionChanged( QListViewItem* )' ), self.raiseWidget )
      self.connect( self.jobNavLV.attrDelAction, qt.SIGNAL( "activated()" ), self.slotDelAttribute )

      # Display to screen
      self.populate()
      self.show()

   def minimumSizeHint( self ):
      return qt.QSize( 600, 480 )

   def isActionInProgress( self ):
      if self.__actionInProgress:
         # This should not happen. Job Tab Widget buttons should be disabled until
         # current action has completed.
         miscDialogs.infoMessage( None, "Job %s is currently executing a '%s' action. Try again later." % ( self.job.id, self.__actionInProgress ) )
         return True
      return False

   def slotDelAttribute( self ):
      self.modifyItem( self.jobNavLV.currentItem().objStr, None, 'del' )

   def getChildLists( self, item, indexRemoveList, takeList = None, includeRoot = False ):
      recursiveCall = True
      if takeList is None: # in recursive call
         indexRemoveList.append( item.objStr )
      else:
         recursiveCall = False
         if includeRoot:
            takeList.append( item )
            indexRemoveList.append( item.objStr )
      childItem = item.firstChild()
      while childItem:
         self.getChildLists( childItem, indexRemoveList )
         if not recursiveCall and not includeRoot:
            # Only top level items need to be 'taken' since child items are 
            # automatically taken when parent items are taken.
            # To retain the top level items only, the item in question is 
            # only added if it's not in a recursive call.
            takeList.append( childItem )
         childItem = childItem.nextSibling()

   def modifyItem( self, command, choice, action ):
      actualCommand = "self.job%s" % command[ command.find( '.' ): ]
      currentItem = self.__pageIndex[ command ].item
      if action == 'swap':
         
         bracketPos = command.rfind( '[' )
         if bracketPos >= 0:
            # Modification of ComponentItem sequence. Immutable! Invoking workaround.
            parentCommand = command[ :bracketPos ]
            actualParentCommand = "self.job%s" % parentCommand[ parentCommand.find( '.' ): ]
            parentObj = eval( actualParentCommand )
            newSequence = []
            iPos = int( command[ bracketPos + 1: -1 ] )
            for objPos in range( len( parentObj ) ):
               if objPos == iPos:
                  newSequence.append( eval( "inspector.%s()" % choice ) )
               else:
                  newSequence.append( parentObj[ objPos ] )
            exec( "%s = newSequence" % actualParentCommand )
         else: # Modification of ComponentItem
            if choice == 'None':
               exec( "%s = None" % actualCommand )
            else:
               exec( "%s = inspector.%s()" % ( actualCommand, choice ) )
         attr = eval( actualCommand )
         currentItem.setDisplay( attr )

         takeList = [] # top level children
         indexRemoveList = [] # entries to remove from self.__pageIndex
         self.getChildLists( currentItem, indexRemoveList, takeList )

         while takeList:
            currentItem.takeItem( takeList.pop() )

         while indexRemoveList:
            del self.__pageIndex[ indexRemoveList.pop() ]

         if attr: # in case reset to default of None
            self.populate( attr, command )

      elif action == 'add': # Add
         newSequence = []
         newSequence.extend( eval( actualCommand ) )
         newSequence.append( eval( "inspector.%s()" % choice ) )
         iPos = len( newSequence ) - 1
         exec( "%s = newSequence" % actualCommand )
         # To deal with ComponentItem sequences, arguments for parents are given to populate() instead.
         self.populate( eval( actualCommand[: actualCommand.rfind( '.' ) ] ), command[ : command.rfind( '.' ) ] )

      elif action == 'del': # Delete
         bracketPos = command.rfind( '[' )
         if bracketPos < 0:
            return
         parentCommand = command[ :bracketPos ]
         actualParentCommand = "self.job%s" % parentCommand[ parentCommand.find( '.' ): ]
         parentObj = eval( actualParentCommand )
         newSequence = []
         iPos = int( command[ bracketPos + 1: -1 ] )
         for objPos in range( len( parentObj ) ):
            if objPos == iPos:
               continue
            newSequence.append( parentObj[ objPos ] )
         exec( "%s = newSequence" % actualParentCommand )
         parentItem = currentItem.parent()

         takeList = [] # top level children
         indexRemoveList = [] # entries to remove from self.__pageIndex

         self.getChildLists( parentItem, indexRemoveList, takeList )

         while takeList:
            parentItem.takeItem( takeList.pop() )

         while indexRemoveList:
            del self.__pageIndex[ indexRemoveList.pop() ]

         self.raiseWidget( parentItem )
         # To deal with ComponentItem sequences, arguments for parents are given to populate() instead.
         self.populate( eval( actualCommand[: actualCommand.rfind( '.' ) ] ), command[ : command.rfind( '.' ) ] )


   def populate( self, baseObj = None, baseObjPrefix = None, includeRoot = None ):
      if baseObj is None:
         baseObj = self.job
      if baseObjPrefix is None:
         baseObjPrefix = 'Job'
      if includeRoot is None:
         includeRoot = False
      
      guiDisplayList = getGUIConfig( baseObj, baseObjPrefix, self.advancedView )
      
      for guiDisplayItem in guiDisplayList:
         tempObj = baseObj
         tempObjPrefix = baseObjPrefix
         try:
            parentItem = self.__pageIndex[ tempObjPrefix ].item
         except:
            parentItem = self.jobNavLV

         # ASSUMPTION: 
         # guiDisplayList (created by getGUIConfig()) is ordered such that 
         # higher level items always come first.
         # i.e. there should not be a situation where Job.backend.id comes 
         # before Job.backend or Job.backend is absent when Job.backend.id
         # exists. Running the for/loop over the split objPrefix 
         # is purely to create the hierarchical structure.
         componentStartPos = baseObjPrefix.count( '.' ) + 1
         for component in guiDisplayItem[ 'objPrefix' ].split( '.' )[ componentStartPos: ]:
            tempObjPrefix = "%s.%s" % ( tempObjPrefix, component )
            # Check for list entries
            pos = component.rfind( '[' )
            if pos >= 0:
               inList = True
               tempType = tempObj._impl._schema.datadict[ component[ :pos ] ]
               tempObj = getattr( tempObj, component[ :pos ] )[ int( component[ pos + 1 : -1 ] ) ]
            else:
               inList = False
               tempType = tempObj._impl._schema.datadict[ component ]
               tempObj = getattr( tempObj, component )
            if tempType._meta[ 'protected' ]:
               continue
            # Check in the pageIndex for an existing parent/entry
            if tempObjPrefix not in self.__pageIndex:
               if inList:
                  parentItem = self.__pageIndex[ tempObjPrefix[ : tempObjPrefix.rfind( '[' ) ] ].item
               page = qt.QWidget( self.widgetStack_JobAttributes,"WStackPage_%s" % tempObjPrefix )
               qt.QVBoxLayout( page,  11, 6, "WStackPage_Layout_%s" % tempObjPrefix )
               if isinstance( tempType, inspector.Ganga.GPIDev.Schema.ComponentItem ):
                  parentItem = JobNavLVItem( parentItem, tempObj, tempObjPrefix, not tempType._meta[ 'protected' ] )
                  if isinstance( tempType, inspector.Ganga.GPIDev.Schema.FileItem ):
                     # assumes end of for/loop. FileItem and FileItem lists are
                     # special cases with specialised widgets available
                     widgetList = buildGUI( page, [ guiDisplayItem ], self.slotModified )
                  else: # Other Component Items
                     widgetList = buildGUI( page, [ guiDisplayItem ], self.modifyItem )
               elif isinstance( tempType, inspector.Ganga.GPIDev.Schema.SimpleItem ): 
                  # assumes end of for/loop
                  parentItem = JobNavLVItem( parentItem, tempObj, tempObjPrefix, False )
                  widgetList = buildGUI( page, [ guiDisplayItem ], self.slotModified )
               self.__pageIndex[ tempObjPrefix ] = Item2PageMap( parentItem, page, widgetList )
               self.widgetStack_JobAttributes.addWidget( page, -1 )
            else: # progressing down the tree i.e. existing parent
               parentItem = self.__pageIndex[ tempObjPrefix ].item

   def getExportMethods( self ):
      def validExportMethods( obj ):
         try:
            return filter( lambda x:hasattr( obj, x ), obj._impl._exportmethods )
         except:
            return []
      exportMethods = {}# self.job._impl._name : validExportMethods( self.job ) }
      item = self.jobNavLV.firstChild()
      while item:
         attr = eval( "self.job%s" % item.objStr[ item.objStr.find( '.' ): ] )
         itemEMList = validExportMethods( attr )
         if itemEMList:
            exportMethods[ item.objStr ] = itemEMList
         item = item.nextSibling()
      return exportMethods

   def raiseWidget( self, item ):
      if item is not None:
         self.widgetStack_JobAttributes.raiseWidget( self.__pageIndex[ item.objStr ].page )

   def isModified( self ):
      return self.__modified

   def setModified( self, state):
      self.__modified = bool( state )

   def slotModified( self, *args ):
      self.setModified( True )

   def isSubmitted( self ):
      return self.__submitted

   def kill( self, callback_Success = lambda:None, callback_Failure = lambda:None ):
      if self.isActionInProgress():
         return False
      else:
         self.__actionInProgress = "kill"

      def cb_Success():
         self.__actionInProgress = ""
         callback_Success()
      
      def cb_Failure():
         self.__actionInProgress = ""
         callback_Failure()

      _action = inspector.JobAction( function = self.job.kill,
                                     callback_Success = cb_Success,
                                     callback_Failure = cb_Failure )
      try:
         inspector.queueJobAction( _action )
      except:
         cb_Failure()

   def remove( self, callback_Success = lambda:None, callback_Failure = lambda:None ):
      if self.isActionInProgress():
         return False
      else:
         self.__actionInProgress = "remove"

      def cb_Success():
         self.__actionInProgress = ""
         callback_Success()
      
      def cb_Failure():
         self.__actionInProgress = ""
         callback_Failure()

      _action = inspector.JobAction( function = self.job.remove,
                                     callback_Success = cb_Success,
                                     callback_Failure = cb_Failure )
      try:
         inspector.queueJobAction( _action )
      except:
         cb_Failure()

   def makeTemplate( self ):
      if self.saveChanges() and not self.job.name:
         _name, _ok = qt.QInputDialog.getText( "Creating new template from Job %s" % self.job.id, "Name of new template", qt.QLineEdit.Normal, "New Job %s Template" % self.job.id, self._parent, '' )
         if _ok and _name:
            j = inspector.JobTemplate( self.job )
            j.name = str( _name )
            return j
      return None

   def saveChanges( self, item = None ):
      """Update attributes with settings on screen"""
      if not self.isModified() or self.isSubmitted():
         return True
      if item is None:
         item = self.jobNavLV
         stopItem = None
      else:
         stopItem = item.nextSibling()
      attributeIterator = qt.QListViewItemIterator( item )
      while attributeIterator.current():
         currentAttribute = attributeIterator.current()
         if currentAttribute == stopItem:
            break
         m = self.__pageIndex[ currentAttribute.objStr ]
         for _w in m.widgetList:
            if _w.protected or isinstance( _w, ItemChoice_Widget ) or isinstance( _w, ItemChoiceAdd_Widget ):
               continue
            _attrStr = "self.job%s" % _w.command[ _w.command.index('.'): ]
            try:
               exec( "%s = _w.get()" % _attrStr )
            except ( TypeMismatchError,
                     Ganga_Errors.UpdateException, 
                     Ganga_Errors.TypeException, 
                     inspector.Ganga.GPIDev.Base.Proxy.ReadOnlyObjectError ), msg:
               self.__actionInProgress = ""
               miscDialogs.warningMessage( None, "Error trying to save Job %s.\n%s" % ( self.job.id, msg ) )
               return False
         attributeIterator += 1
      self.__modified = False
      return True
   
   def submit( self, callback_Success = lambda:None, callback_Failure = lambda:None ):
      if self.isActionInProgress():
         return False
      else:
         self.__actionInProgress = "submit"

      def cb_Success():
         self.__submitted = True
         self.__actionInProgress = ""
         callback_Success()
      
      def cb_Failure():
         self.__submitted = False
         self.__actionInProgress = ""
         self.setWidgets_FromThread( True )
         callback_Failure()

      if self.saveChanges():
         _action = inspector.JobAction( function = self.job.submit,
                                        callback_Success = cb_Success,
                                        callback_Failure = cb_Failure )
         try:
            inspector.queueJobAction( _action )
         except:
            cb_Failure()
         else:
            self.__submitted = True
            self.setWidgets( False )
      return self.__submitted

   def customEvent( self, myEvent ):
      if myEvent.type() == JOB_SETWIDGETS_EVENT:
         self.setWidgets( myEvent.enable, myEvent.widgetList )

   def setWidgets_FromThread( self, enable, widgetList = None ):
      qt.QApplication.postEvent( self, Job_SetWidgets_CustomEvent( enable, widgetList ) )

   def setWidgets( self, enable, widgetList = None ):
      if widgetList:
         for widget in widgetList:
            widget.setEnabled( enable )
      else:
         for objStr in self.__pageIndex:
            for widget in self.__pageIndex[ objStr ].widgetList:
               widget.setEnabled( enable )
