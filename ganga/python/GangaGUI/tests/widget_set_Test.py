import unittest
from qt import *
from GangaGUI.widget_set import *


class InputLine_Widget_TestCase( unittest.TestCase ):
   def setUp(self):
      self.widget = InputLine_Widget( name = 'test_InputLine_Widget',
                                      textLabel = 'Testing InputLine_Widget',
                                      default = 'Default string' )
      qApp.setMainWidget( self.widget )

   def tearDown( self ):
      del self.widget
      self.widget = None

   def test_clear( self ):
      self.widget.lineEdit.clear()
      self.assertEqual( self.widget.lineEdit.text(), QString( '' ), 'lineEdit did not clear properly' )
   
   def test_revert( self ):
      self.test_clear()
      self.widget.slotRevert()
      self.assertEqual( self.widget.lineEdit.text(), QString( 'Default string' ), 'lineEdit did not revert to default value.' )


class InputList_Widget_TestCase( unittest.TestCase ):
   def setUp(self):
      self.widget = InputList_Widget( name = 'test_InputList_Widget',
                                      textLabel = 'Testing InputList_Widget',
                                      default = [ 'test1', 'test2' ] )
      qApp.setMainWidget( self.widget )

   def tearDown( self ):
      del self.widget
      self.widget = None

   def test_clear( self ):
      self.assertNotEqual( self.widget.listView.firstChild(), None, 'listView not supposed to be clear!' )
      self.widget.listView.clear()
      self.assertEqual( self.widget.listView.childCount(), 0, 'listView supposed to be clear!' )

   def test_insert( self ):
      self.widget.listView.clear()
      self.widget.slotInsert( 'Test Item' )
      self.assertEqual( self.widget.listView.firstChild().text( 0 ), QString( 'Test Item' ), 'Insertion problem.' )
   
   def test_delete( self ):
      _cCount = self.widget.listView.childCount()
      self.widget.listView.setSelected( self.widget.listView.firstChild(), True )
      self.widget.slotDelete()
      self.assertEqual( self.widget.listView.childCount() + 1, _cCount, 'Deletion problem.' )


class InputChoice_Widget_TestCase( unittest.TestCase ):
   def setUp(self):
      self.widget = InputChoice_Widget( name = 'test_InputList_Widget',
                                        textLabel = 'Testing InputList_Widget',
                                        choices = [ 'test1', 'test2' ],
                                        default = 1 )
      qApp.setMainWidget( self.widget )

   def tearDown( self ):
      del self.widget
      self.widget = None

   def test_revert( self ):
      self.widget.comboBox.setCurrentItem( 0 )
      self.widget.slotRevert()
      self.assertEqual( self.widget.comboBox.currentItem(), 1, 'Revert problem.' )

def run():
   suite = unittest.TestSuite()
   suite.addTest( unittest.makeSuite( InputLine_Widget_TestCase ) )
   suite.addTest( unittest.makeSuite( InputList_Widget_TestCase ) )
   suite.addTest( unittest.makeSuite( InputChoice_Widget_TestCase ) )
   unittest.TextTestRunner( verbosity = 2 ).run( suite )

if __name__ == '__main__':
   from sys import argv
   qApp = QApplication( argv )

run()
