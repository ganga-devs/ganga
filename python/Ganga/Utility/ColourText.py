################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ColourText.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
################################################################################

# File: ColourText.py
# Author: K. Harrison
# Created: 050824
# Last modified: 050824

"""Module containing classes to help with creating colour text.

   Classes for text markup:

      ANSIMarkup - apply ANSI codes to the text
      NoMarkup - ingore colour codes and leave the text unchanged

   ANSI code classes defined are:

      Foreground - create object carrying ANSI codes for
                   changing text foreground colour;

      Background - create object carrying ANSI codes for
                   changing text background colour;

      Effects    - create object carrying ANSI codes for
                   changing text effects.

   Example usage is as follows:

      fg = Foreground()
      bg = Background()
      fx = Effects()

      if coloring_enabled:
       # text will be blue by default and colour codes are applied
       m = ANSIMarkup(fg.blue)
      else:
       # colour codes are ignored
       m = NoMarkup()

       # Text will be coloured in red if coloring_enabled,
       # otherwise text will be unchanged.
       print m('Text in red',code=fg.red)

   It is a better practice to use markup objects to apply colours
   because it is easier to enable/disable the markup if desired.

   However inserting the codes directly also works:

   # Print text in specified colour.
   print fg.some_colour + 'Text in some_colour' + fx.normal

   # Print text with colour of background changed as specified.
   print bg.some_colour + 'Text with background in some_colour' + fx.normal

   # Print text with specified effect applied.
   print fx.some_effect + 'Text with some_effect applied' + fx.normal

   Note that each ANSI code overrides the previous one, and their effect
   isn't cumulative.  Terminating a string with fx.normal in the above
   examples ensures that subsequent text is output using the terminal
   defaults.

   For details of the colours and effects available, see help for
   individual classes.
"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "24 August 2005"
__version__ = "1.0"

class ANSIMarkup:
   """ Apply ANSI colouring codes.
   """
   def __init__(self,default_code=None):
      if default_code is None:
         default_code = Effects().normal
      self.default_code = default_code

   def __call__(self,text,code=None):
      if code is None:
         code = self.default_code
      return code+text+self.default_code

class NoMarkup:
   """ Leave text unchanged.
   """
   def __init__(self,default_code=None):
      pass

   def __call__(self,text,code=None):
      return text

class Background:

   """Class for creating objects carrying ANSI codes for changing
      text background colour.  The defined colours are:

      black, blue, cyan, green, orange, magenta, red, white.

      In all cases, the text foregreound colour is the terminal
      default (usually black or white)."""

   def __init__( self ):
      """Set ANSI codes for defined colours"""

      _base  = '\033[%sm'
      self.black = _base % "0;40"
      self.red = _base % "0;41"
      self.green = _base % "0;42"
      self.orange = _base % "0;43"
      self.blue = _base % "0;44"
      self.magenta = _base % "0;45"
      self.cyan = _base % "0;46"
      self.white = _base % "0;47"

class Effects:

   """Class for creating objects carrying ANSI codes for text
      effects.  The defined effects are:

      normal,

      bold, reverse, underline,
 
      nobold, noreverse, nounderline.

      All effects imply terminal defaults for the colours."""

   def __init__( self ):
      """Set ANSI codes for defined effects"""
      _base  = '\033[%sm'
      self.normal = _base % "0;0"
      self.bold = _base % "0;1"
      self.underline = _base % "0;4"
      self.reverse = _base % "0;7"
      self.nobold = _base % "0;21"
      self.nounderline = _base % "0;24"
      self.noreverse = _base % "0;27"

class Foreground:
   """Class for creating objects carrying ANSI codes for changing
      text foreground colour.  The defined colours are:

      black, blue, cyan, green, orange, magenta, red, white,

      boldblue, boldcyan, boldgreen, boldgreen, boldgrey,
      boldmagenta, boldred, boldwhite, boldyellow.

      For good visibility, the bold colours are better."""

   def __init__( self ):
      """Set ANSI codes for defined colours"""

      _base  = '\033[%sm'
      self.normal = _base % "0"
      self.black = _base % "0;30"
      self.red = _base % "0;31"
      self.green = _base % "0;32"
      self.orange = _base % "0;33"
      self.blue = _base % "0;34"
      self.magenta = _base % "0;35"
      self.cyan = _base % "0;36"
      self.white = _base % "0;37"
      self.boldgrey = _base % "1;30"
      self.boldred = _base % "1;31"
      self.boldgreen = _base % "1;32"
      self.boldyellow = _base % "1;33"
      self.boldblue = _base % "1;34"
      self.boldmagenta = _base % "1;35"
      self.boldcyan = _base % "1;36"
      self.boldwhite = _base % "1;37"


colour_objects = { 'fg' : Foreground(), 'bg' : Background(), 'fx' : Effects() }

def getColour(name):
   """ Get a colour code from the symbolic name: fg = Foreground(), bg = Background(), fx = Effects()
   The name examples fg.red, fx.normal, bg.white
   Raise ValueError if name undefined or malformed.
   """
   x,y = name.split('.')
   return getattr(colour_objects[x],y)

   try:
      x,y = name.split('.')
   except Exception:
      raise ValueError('unknown colour code %s'%str(name))

   try:
      return getattr(colour_objects[x],y)
   except Exception:
      raise ValueError('unknown colour code %s'%str(name))
   
      
   

   
