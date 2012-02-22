class GenericError( Exception ):
   """
   Simple wrapper for errors.
   """
   def __init__( self, msg=None ):
      if msg is None:
         self.errMsg = ''
      else:
         self.errMsg = msg

   def __repr__( self ):
      return self.errMsg

   def __str__( self ):
      return self.errMsg
      
class InspectionError( GenericError ):
   pass

class TypeException( GenericError ):
   pass

class UpdateException( GenericError ):
   pass

class ArgumentException( GenericError ):
   pass



