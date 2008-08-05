# NOTICE:
# this module is used by config system to configure logging therefore the logger
# requires special handling
# this module must be importable without any side-effect, especially without
# creation of loggers by surrounding packages...

from Ganga.Utility.util import importName

def _valueTypeAllowed( val, valTypeList,logger=None):
   for _t in valTypeList:
       _dotMod = _t.split('.')
       if len(_dotMod) > 1:
           if not hasattr( val, '_proxyClass' ):
               # val is probably a normal Python object.
               continue
           _type = importName( '.'.join(_dotMod[0:-1]), _dotMod[-1] )
           _val = val
       else: # Native Python type
           try:
               _type = eval( _t ) # '_type' is actually the class name
           except NameError:
               logger.error( "Invalid native Python type: '%s'" % _t )
               continue
           _val = val
       # Deal with the case where type is None.
       if _type is None:
           if _val is None:
               return True
           else:
               continue
       elif isinstance( _val, _type ):
          return True
   return False
