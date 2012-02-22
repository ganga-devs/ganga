from Ganga.Utility.Runtime import importName, allRuntimes
import qt

import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger('Ganga.customGUIManager')

customGUI_Dict = {}
connectionSources = {}

class CustomGUI_Entry( object ):
   def __init__( self, guiclass, arglist, label = None, menubar = None, icon = None, action = None, recipient = None ):
      self.guiclass = guiclass
      self.arglist = arglist
      if label is None:
         try:
            label = self.guiclass.__name__
         except:
            label = 'Unknown plugin'
      self.label = label
      self.menubar = menubar
      self.icon = icon
      self.recipient = recipient
      self.guiobj = self.guiclass( *self.arglist )
      if not action:
         action = qt.QAction( connectionSources[ 'main' ], "%s_Action" % self.guiclass.__name__ )
         action.setEnabled(1)
         action.setText( self.label )
      self.action = action
      if self.menubar:
         self.action.setMenuText( self.label )
         self.action.addTo( connectionSources[ self.menubar ] )
      if self.icon:
         self.action.setIconSet( self.icon )
         self.action.addTo( connectionSources[ 'toolbar' ] )
      qt.QObject.connect( self.action, qt.SIGNAL( "activated()" ), self.showNonModal )
   
   def showNonModal( self ):
      self.show( False )
   
   def show( self, modal = True ):
      self.guiobj.setModal( modal )
      if modal:
         if self.guiobj.exec_loop() == qt.QDialog.Accepted:
            if self.recipient:
               self.recipient( self.guiobj.get() )
               self.recipient = None
      else:
         self.guiobj.show()


def insertConnectionSources( cSrcDict ):
   connectionSources.update( cSrcDict )
   log.debug( "connectionServices = %s" % connectionSources  )


def loadCustomGUIs():
   for rtName, rtPackage in allRuntimes.iteritems():
      if rtName == 'GangaGUI':
         continue
      guiPackage = importName( rtPackage.name, 'gui' )
      if guiPackage is None or not hasattr( guiPackage, '__all__'):
         continue

      log.debug( "guiPackage.__name__ = %s, guiPackage.__all__ = %s" % ( guiPackage.__name__, guiPackage.__all__ ) )

      for customGUI_moduleName in guiPackage.__all__:
         customGUI_module = importName( guiPackage.__name__, customGUI_moduleName )
         if customGUI_module is None:
            continue
         log.debug( "customGUI_module = %s" % customGUI_module )

         for gpsDictEntry in customGUI_module.GPS_LIST:
            customGUI_Dict[ "%s.%s" % ( customGUI_moduleName, gpsDictEntry[ 'guiclass' ].__name__ ) ] = CustomGUI_Entry( **gpsDictEntry )
            log.debug( "Adding %s.%s to customGUI_Dict" % ( customGUI_moduleName, gpsDictEntry[ 'guiclass' ].__name__ ) )

   log.debug( "customGUI_Dict = %s" % customGUI_Dict )