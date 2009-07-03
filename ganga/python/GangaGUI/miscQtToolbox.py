import qt

class ExtProcess( qt.QProcess ):
   def __init__( self, cmdList, stdout, stderr ):
      qt.QProcess.__init__( self )
      self.stdout = stdout
      self.stderr = stderr
      self.cmdList = cmdList
      map( self.addArgument, cmdList )
      self.connect( self, qt.SIGNAL( 'readyReadStdout()' ), self.__readExtProcStdout )
      self.connect( self, qt.SIGNAL( 'readyReadStderr()' ), self.__readExtProcStderr )
      #self.connect( self, qt.SIGNAL( 'launchFinished()' ), self.__proc.deleteLater )
      self.connect( self, qt.SIGNAL( 'processExited()' ), self.__procDone )

   def __procDone( self ):
      self.log.info( 'Process [%s] exited.' % str( self.processIdentifier() ) )
      self.deleteLater()

   def __readExtProcStderr( self ):
      if self.canReadLineStderr():
         self.stderr.write( str( self.readLineStderr() ) )

   def __readExtProcStdout( self ):
      if self.canReadLineStdout():
         self.stdout.write( str( self.readLineStdout() ) )

   def launch( self, *args ):
      # Setup logging ---------------
      import Ganga.Utility.logging
      self.log = Ganga.Utility.logging.getLogger()
      _fullCmdStr = ' '.join( self.cmdList )
      if qt.QProcess.launch( self, *args ):
         self.log.info( '[%s] started with pid=%s' % ( _fullCmdStr, str( self.processIdentifier() ) ) )
         return True
      else:
         self.log.warning( "Failed to start %s" % _fullCmdStr )
         return False         
