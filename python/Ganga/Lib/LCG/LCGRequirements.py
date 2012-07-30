from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig

class LCGRequirements(GangaObject):
   '''Helper class to group LCG requirements.

   See also: JDL Attributes Specification at http://cern.ch/glite/documentation
   '''

   _schema = Schema(Version(1,2), {
      'software'        : SimpleItem(defvalue=[], typelist=['str'],sequence=1,doc='Software Installations'),
      'nodenumber'      : SimpleItem(defvalue=1,doc='Number of Nodes for MPICH jobs'),
      'memory'          : SimpleItem(defvalue=0,doc='Mininum available memory (MB)'),
      'cputime'         : SimpleItem(defvalue=0,doc='Minimum available CPU time (min)'),
      'walltime'        : SimpleItem(defvalue=0,doc='Mimimum available total time (min)'),
      'ipconnectivity'  : SimpleItem(defvalue=False,doc='External connectivity'),
      'allowedCEs'      : SimpleItem(defvalue='', doc='allowed CEs in regular expression'),
      'excludedCEs'     : SimpleItem(defvalue='', doc='excluded CEs in regular expression'),
      'other'           : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='Other Requirements')
   })

   _category = 'LCGRequirements'
   _name = 'LCGRequirements'

   _GUIPrefs = [ { 'attribute' : 'software',       'widget' : 'String_List' },
                 { 'attribute' : 'nodenumber',     'widget' : 'Int' },
                 { 'attribute' : 'memory',         'widget' : 'Int' },
                 { 'attribute' : 'cputime',        'widget' : 'Int' },
                 { 'attribute' : 'walltime',       'widget' : 'Int' },
                 { 'attribute' : 'ipconnectivity', 'widget' : 'Bool' },
                 { 'attribute' : 'allowedCEs',     'widget' : 'String' },
                 { 'attribute' : 'excludedCEs',    'widget' : 'String' },
                 { 'attribute' : 'other',          'widget' : 'String_List' } ]

   def __init__(self):
      
      super(LCGRequirements,self).__init__()

   def merge(self,other):
      '''Merge requirements objects'''
      
      if not other: return self
      
      merged = LCGRequirements()
      for name in [ 'software', 'nodenumber', 'memory', 'cputime', 'walltime', 'ipconnectivity', 'allowedCEs', 'excludedCEs', 'other' ]:
         attr = getattr(other,name)
         if not attr: attr = getattr(self,name)
         setattr(merged,name,attr)
         
      return merged

   def convert(self):
      '''Convert the condition in a JDL specification'''
     
      import re

      requirements = [ 'Member("%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % sw for sw in self.software ]
      if self.memory:         requirements += [ 'other.GlueHostMainMemoryVirtualSize >= %s' % str(self.memory) ]
      if self.cputime:        requirements += [ '(other.GlueCEPolicyMaxCPUTime >= %s || other.GlueCEPolicyMaxCPUTime == 0)' % str(self.cputime) ]
      if self.walltime:       requirements += [ '(other.GlueCEPolicyMaxWallClockTime >= %s || other.GlueCEPolicyMaxWallClockTime == 0)' % str(self.walltime) ]
      if self.ipconnectivity: requirements += [ 'other.GlueHostNetworkAdapterOutboundIP==true' ]
      requirements += self.other

      config = getConfig('LCG')

      ## retrieve allowed_ces and excluded_ces from LCGRequirement object and the config['Allowed/ExcludedCEs']
      allowed_ces  = []
      excluded_ces = []

      if self.allowedCEs:
          allowed_ces  += re.split('\s+', self.allowedCEs)

      if self.excludedCEs:
          excluded_ces += re.split('\s+', self.excludedCEs)

      if config['AllowedCEs']:
         allowed_ces += re.split('\s+',config['AllowedCEs'])
         
      if config['ExcludedCEs']:
         excluded_ces += re.split('\s+',config['ExcludedCEs'])

      ## composing the requirements given the list of allowed_ces and excluded_ces
      if allowed_ces:
          requirements += [ '( %s )' % ' || '.join([ 'RegExp("%s",other.GlueCEUniqueID)' % ce for ce in allowed_ces])]
          
      if excluded_ces:
          #requirements += [ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces ]
          requirements += [ '( %s )' % ' && '.join([ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces])]

      return requirements
