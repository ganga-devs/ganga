import re
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig

class NA62LCGRequirements(GangaObject):
   '''Helper class for NA62 specific LCG requirements.

   See also: JDL Attributes Specification at http://cern.ch/glite/documentation
   '''

   _schema = Schema(Version(1,2), {
      'memory'          : SimpleItem(defvalue=0,doc='Mininum available memory (MB)'),
      'cputime'         : SimpleItem(defvalue=0,doc='Minimum available CPU time (min)'),
      'walltime'        : SimpleItem(defvalue=0,doc='Mimimum available total time (min)'),
      'allowedSites'    : SimpleItem(defvalue=[], typelist=['str'],sequence=1, doc='list of allowed sites'),
      'excludedSites'   : SimpleItem(defvalue=[], typelist=['str'],sequence=1,doc='list of excluded sites'),
      'other'           : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='Other Requirements')
   })

   _category = 'LCGRequirements'
   _name = 'NA62LCGRequirements'

   def __init__(self):      
      super(NA62LCGRequirements,self).__init__()

   def merge(self,other):
      '''Merge requirements objects'''
      
      if not other: return self
      
      merged = NA62LCGRequirements()
      for name in [ 'memory', 'cputime', 'walltime', 'allowedSites', 'excludedSites', 'other' ]:

         attr = ''

         try:
             attr = getattr(other,name)
         except KeyError as e:
             pass

         if not attr: attr = getattr(self,name)
         setattr(merged,name,attr)
         
      return merged
   
   def convert(self):
      '''Convert the condition in a JDL specification'''
     
      import re

      ##requirements = [ 'Member("%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % sw for sw in self.software ]
      requirements = []
      if self.memory:         requirements += [ 'other.GlueHostMainMemoryRAMSize >= %s' % str(self.memory) ]
      if self.cputime:        requirements += [ '(other.GlueCEPolicyMaxCPUTime >= %s || other.GlueCEPolicyMaxCPUTime == 0)' % str(self.cputime) ]
      if self.walltime:       requirements += [ '(other.GlueCEPolicyMaxWallClockTime >= %s || other.GlueCEPolicyMaxWallClockTime == 0)' % str(self.walltime) ]
      requirements += self.other

      # map site names to CEs
      # should be done with a DB call or something...
      site_map  = { 'LIV': 'liv.ac.uk',
                    'BIR': 'bham.ac.uk',
                    'GLA': 'scotgrid.ac.uk',
                    'IC': 'ic.ac.uk',
                    'RAL': 'rl.ac.uk',
                    'UCL': 'ucl.ac.uk' }

      allowed_ces = []
      excluded_ces = []
      for s in self.allowedSites:
         allowed_ces.append( site_map[s] )

      for s in self.excludedSites:         
         excluded_ces.append( site_map[s] )

      ## composing the requirements given the list of allowed_ces and excluded_ces
      if allowed_ces:
          requirements += [ '( %s )' % ' || '.join([ 'RegExp("%s",other.GlueCEUniqueID)' % ce for ce in allowed_ces])]
          
      if excluded_ces:
          #requirements += [ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces ]
          requirements += [ '( %s )' % ' && '.join([ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces])]

      return requirements
