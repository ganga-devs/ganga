# File: cfg_manager.py
# Author: Alvin Tan (Birmingham, clat@hep.ph.bham.ac.uk)
# Created: 09 November 2005
#
# Last modified: 10 July 2006

import ConfigParser
import os, sys
from shutil import copytree, rmtree
import tempfile
from GangaGUI import Ganga_Errors
import Ganga.Utility.Config as GangaConfig


class GangaGUI_configuration:
   class __impl( ConfigParser.RawConfigParser):
      def __init__( self ):
         ConfigParser.RawConfigParser.__init__( self )
         self.GangaGUI_CONFIG_NAME = 'GangaGUI.cfg'
         self.GangaGUI_CONFIG_DIR = self.__getConfigDir()
         self.tmpFileUsed = False
         self.GangaGUI_CONFIG_NAME = os.path.join( self.GangaGUI_CONFIG_DIR, self.GangaGUI_CONFIG_NAME )
         if not os.path.exists( self.GangaGUI_CONFIG_NAME ):
            self.create_GangaGUI_config()
         self.loadConfig()

      def optionxform( self, name ):
         return str( name )

      def __getConfigDir( self ):
         _gangaDir = os.path.expanduser( os.path.expandvars( os.path.join( os.path.dirname( GangaConfig.getConfig( 'Configuration' ).getEffectiveOption( 'gangadir' ) ), 'gui' ) ) )
         _gangaDir1 = self.mkDir( _gangaDir, True )
         if _gangaDir1 == _gangaDir:
            self.tmpFileUsed = False
            return _gangaDir
         else:
            self.tmpFileUsed = True
            return _gangaDir1

      def __upgradeConfig( self ):
         pass

      def defineDefaults( self ):
         try:
            self.add_section( 'SESSION' )
         except ConfigParser.DuplicateSectionError:
            pass
         _editor = os.getenv( 'VISUAL' ) or os.getenv( 'EDITOR' )
         _gui = False
         if not _editor: # define default editors
            if sys.platform == 'windows':
               _editor = 'Notepad.exe'
               _gui = True
            elif sys.platform == 'darwin':
               _editor = '/Applications/TextEdit.app'
               _gui = True
            elif 'linux' in sys.platform:
               _editor = '/usr/bin/pico'
            else:
               _editor = None
         if _editor:
            _fa = { '' : [ _editor, _gui ], 
                    'txt' : [ _editor, _gui ],
                    'log' : [ _editor, _gui ] }
         else:
            _fa = {}
         self.setDict( _fa, 'File_Association', 'DEFAULT' )
         if sys.platform.startswith( 'linux' ):
            _sc = 'xterm -e' 
         elif sys.platform == 'darwin':
            _sc = 'open'
         else:
            _sc = ''
         self.setString( _sc, 'Shell_Command', 'DEFAULT' )
         self.setDict( { 'Ganga_Log': ((True, 3, 0, True, 0), (0, 0, 865, 145)),
                         'Job_Builder': ((True, 6, 0, True, 0), (380, 175, 525, 380)), 
                         'Ganga_Scriptor': ((True, 6, 0, True, 0), (150, 355, 465, 345)), 
                         'toolBar': ((True, 2, 0, False, 0), (0, 0, 520, 45)), 
                         'MainWindow': (0, 20, 860, 580)
                       }, 'Window_Geometry', 'DEFAULT' )
         _jf = [ ( 'id', 'id', 'Job.id' ), 
                 ( 'status', 'status', 'Job.status' ), 
                 ( 'name', 'name', 'Job.name' ),
                 ( 'application', 'application', 'Job.application' ), 
                 ( 'exe filename', 'application.exe', 'Job.Local.exe' ), 
                 ( 'backend', 'backend', 'Job.backend' ), 
                 ( 'backend id', 'backend.id', 'Job.Local.id' ), 
                 ( 'backend status', 'backend.status', 'Job.Local.status' ) ]
         self.setList( _jf, 'Job_Monitor_Fields', 'DEFAULT' )
         _statusColourDict = { 'new' : ( 'black', 'cyan' ),
                               'submitting' : ( 'gray', 'white' ),
                               'submitted' : ( 'white', 'blue' ),
                               'running' : ( 'black', 'green' ),
                               'completed' : ( 'white', 'darkGreen' ),
                               'completing' : ( 'white', 'darkGreen' ),
                               'killed' : ( 'black', 'red' ),
                               'unknown' : ( 'black', 'gray' ),
                               'incomplete' : ( 'black', 'magenta' ),
                               'failed' : ( 'yellow', 'red' ),
                               '*subjob*' : ( 255, 125 ) } # subjob intensity colour (FG,BG) tuple of value 0-255.
         self.setDict( _statusColourDict, 'Status_Colour_Schema', 'DEFAULT' )
         self.setString( 'False', 'Use_Big_Pixmaps', 'DEFAULT' )
         del _editor, _gui, _fa, _sc, _jf, _statusColourDict
      
      def loadConfig( self ):
         self.readfp( file( self.GangaGUI_CONFIG_NAME ) )
         # Bring 'DEFAULTS' section in line with the current version.
         self.defineDefaults()

      def getString( self, option, section = None ):
         if section is None:
            section = 'SESSION'
         try:
            return self.get( section, option )
         except ( ConfigParser.NoSectionError, ConfigParser.NoOptionError ):
            return ''

      def getBool( self, option, section = None ):
         if section is None:
            section = 'SESSION'
         v = str( self.get( section, option ) )
         if v.lower() not in self._boolean_states:
            raise ValueError, 'Not a boolean: %s' % v
         return ConfigParser.RawConfigParser._boolean_states[ v.lower() ]

      def getInt( self, option, section = None ):
         if section is None:
            section = 'SESSION'
         return self.getint( section, option )

      def getFloat( self, option, section = None ):
         if section is None:
            section = 'SESSION'
         return self.getfloat( section, option )

      def getDict( self, option, section = None ):
         _dict = self.getString( option, section )
         if _dict == '':
            return {}
         _dict = _dict.strip()
         if _dict[0] != '{':
            _dict = '{' + _dict
         if _dict[-1] != '}':
            _dict += '}'
         try:
            return eval( _dict )
         except:
            return {}

      def setAuto( self, newEntry, option, section = None, force = None, updateNow = None ):
         if isinstance( newEntry, dict ):
            self.setDict( newEntry, option, section, force, updateNow )
         elif isinstance( newEntry, list ):
            self.setList( newEntry, option, section, force, updateNow )
         elif isinstance( newEntry, str ) or \
              isinstance( newEntry, int ) or \
              isinstance( newEntry, float ) or \
              isinstance( newEntry, bool ):
            self.setString( str( newEntry ), option, section, force, updateNow )

      def setDict( self, newdict, option, section = None, force = None, updateNow = None ):
         if not isinstance( newdict, dict ):
            raise Ganga_Errors.TypeException( "%s not a dictionary!" % newdict )
         self.setString( newdict.__str__(), option, section, force, updateNow )

      def getList( self, option, section = None ):
         _list = self.getString( option, section )
         if _list == '':
            return []
         _list = _list.strip()
         if _list[0] != '[':
            _list = '[' + _list
         if _list[-1] != ']':
            _list += ']'
         try:
            return eval( _list )
         except:
            return []
   
      def setList( self, newlist, option, section = None, force = None, updateNow = None ):
         if not isinstance( newlist, list ):
            raise Ganga_Errors.TypeException( "%s not a list!" % newlist )
         self.setString( newlist.__str__(), option, section, force, updateNow )

      def setString( self, value, option, section = None, force = None, updateNow = None ):
         if force is None:
            force = False
         if updateNow is None:
            updateNow = False
         if section is None:
            section = 'SESSION'
         if force and not self.has_section( section ):
            self.add_section( section )
         try:
            self.set( section, option, value )
         except ConfigParser.NoSectionError:
            raise
         if updateNow:
            self.updateNow()

      def updateNow( self, configPath = None ):
         if configPath is None:
            configPath = self.GangaGUI_CONFIG_NAME
         try:
            _cfg = file( configPath, 'w' )
         except IOError:
            _cfg = file( os.path.join( self.__getConfigDir(), self.GangaGUI_CONFIG_NAME ), 'w' )
         self.write( _cfg )
         _cfg.close()
            
      def create_GangaGUI_config( self ):
         self.defineDefaults()
         self.updateNow()

      def add_to_dict( self, item, option, section = None, **kwargs ):
         _dict = self.getDict( option, section )
         if isinstance( item, dict ):
            _dict.update( item )
            self.setDict( _dict, option, section, **kwargs )
         else:
            raise Ganga_Errors.TypeException( "A dictionary is expected!" )
      
      def remove_from_dict( self, item, option, section = None ):
         _dict = self.getDict( option, section )
         if isinstance( item, list ):
            iter_item = item
         elif isinstance( item, dict ):
            iter_item = item.keys()
         else:
            iter_item  = list( item )
         for i in iter_item:
            try:
               del _dict[ i ]
            except ValueError:
               continue
         self.setDict( _dict, option, section )

      def add_to_list( self, item, option, section = None, **kwargs ):
         _list = self.getList( option, section )
         if isinstance( item, list ):
            _list.extend( item )
         else:
            _list.append( item )
         self.setList( _list, option, section, **kwargs )
      
      def remove_from_list( self, item, option, section = None ):
         _list = self.getList( option, section )
         if isinstance( item, list ):
            for i in item:
               try:
                  _list.remove( i )
               except ValueError:
                  continue
         else:
            _list.remove( item )
         self.setList( _list, option, section )

      def revert( self, option, section = None ):
         if section is None:
            section = 'SESSION'
         if self.has_section( section ):
            if self.has_option( 'DEFAULT', option ):
               _s = self.getString( option, 'DEFAULT' )
               self.setString( _s, option, section )

      def mkDir( self, directory = None, tmpEnabled = None, tmpPrefix = None ):
         if tmpEnabled is None:
            tmpEnabled = False
         if directory is None:
            directory = self.GangaGUI_CONFIG_DIR
         if tmpPrefix is None:
            tmpPrefix = 'GangaGUI_Config_'
         if not os.path.exists( directory ):
            try:
               os.makedirs( directory, 0755 )
            except error, msg:
               if tmpEnabled:
                  return tempfile.mkdtemp( prefix = tmpPrefix )
               raise
         return directory
         
      def rmDir( self, directory = None, inclusive = None ):
         if directory is None:
            directory = self.GangaGUI_CONFIG_DIR
         if inclusive is None:
            inclusive = False
         for r, ds, fs in os.walk( directory, topdown = False ):
            for d in ds:
               os.rmdir( os.path.join( r, d ) )
            for f in fs:
               os.remove( os.path.join( r, f ) )
         if inclusive:
            os.rmdir( directory )

      def backupDirContents( self, oldDir, newDir, delOriginal = None ):
         if delOriginal is None:
            delOriginal = False
         if os.path.exists( newDir ):
            if delOriginal:
               for f in os.listdir( oldDir ):
                  self.rnDir( os.path.join( oldDir, f ), os.path.join( newDir, f ) )
               rmtree( oldDir, True )
            else:
               newDir1 = os.path.join( newDir, os.path.basename( oldDir ) )
               copytree( oldDir, newDir1 )
               for f in os.listdir( newDir1 ):
                  self.rnDir( os.path.join( newDir1, f ), os.path.join( newDir, f ) )
               rmtree( newDir1, True )
         else:
            if delOriginal:
               self.rnDir( oldDir, newDir )
            else:
               copytree( oldDir, newDir )

   __instance = __impl()
   
   def __getattr__( self, attr ):
      return getattr( self.__instance, attr )
   
   def __setattr__( self, attr, value ):
      return setattr( self.__instance, attr, value )

GUIConfig = GangaGUI_configuration()
