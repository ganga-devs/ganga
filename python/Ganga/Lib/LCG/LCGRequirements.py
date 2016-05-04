import re
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.Utility.Config import getConfig


class LCGRequirements(GangaObject):

    '''Helper class to group LCG requirements.

    See also: JDL Attributes Specification at http://cern.ch/glite/documentation
    '''

    _schema = Schema(Version(1, 2), {
        'software': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='Software Installations'),
        'nodenumber': SimpleItem(defvalue=1, doc='Number of Nodes for MPICH jobs'),
        'memory': SimpleItem(defvalue=0, doc='Mininum available memory (MB)'),
        'cputime': SimpleItem(defvalue=0, doc='Minimum available CPU time (min)'),
        'walltime': SimpleItem(defvalue=0, doc='Mimimum available total time (min)'),
        'ipconnectivity': SimpleItem(defvalue=False, doc='External connectivity'),
        'allowedCEs': SimpleItem(defvalue='', doc='allowed CEs in regular expression'),
        'excludedCEs': SimpleItem(defvalue='', doc='excluded CEs in regular expression'),
        'datarequirements': SimpleItem(defvalue=[], typelist=[dict], sequence=1, doc='The DataRequirements entry for the JDL. A list of dictionaries, each with "InputData", "DataCatalogType" and optionally "DataCatalog" entries'),
        'dataaccessprotocol': SimpleItem(defvalue=['gsiftp'], typelist=[str], sequence=1, doc='A list of strings giving the available DataAccessProtocol protocols'),
        'other': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='Other Requirements')
    })

    _category = 'LCGRequirements'
    _name = 'LCGRequirements'

    def __init__(self):

        super(LCGRequirements, self).__init__()

    def merge(self, other):
        '''Merge requirements objects'''

        if not other:
            return self

        merged = LCGRequirements()
        for name in ['software', 'nodenumber', 'memory', 'cputime', 'walltime', 'ipconnectivity', 'allowedCEs', 'excludedCEs', 'datarequirements', 'dataaccessprotocol', 'other']:

            attr = ''

            try:
                attr = getattr(other, name)
            except KeyError as e:
                pass

            if not attr:
                attr = getattr(self, name)
            setattr(merged, name, attr)

        return merged

    def convert(self):
        '''Convert the condition in a JDL specification'''

        import re

        requirements = [
            'Member("%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % sw for sw in self.software]
        if self.memory:
            requirements += ['other.GlueHostMainMemoryVirtualSize >= %s' %
                             str(self.memory)]
        if self.cputime:
            requirements += [
                '(other.GlueCEPolicyMaxCPUTime >= %s || other.GlueCEPolicyMaxCPUTime == 0)' % str(self.cputime)]
        if self.walltime:
            requirements += [
                '(other.GlueCEPolicyMaxWallClockTime >= %s || other.GlueCEPolicyMaxWallClockTime == 0)' % str(self.walltime)]
        if self.ipconnectivity:
            requirements += ['other.GlueHostNetworkAdapterOutboundIP==true']
        requirements += self.other

        config = getConfig('LCG')

        # retrieve allowed_ces and excluded_ces from LCGRequirement object and
        # the config['Allowed/ExcludedCEs']
        allowed_ces = []
        excluded_ces = []

        # from Ganga configuration
        if config['AllowedCEs']:
            ce_req = config['AllowedCEs'].strip()
            allowed_ces += re.split('\s+', ce_req)

        if config['ExcludedCEs']:
            ce_req = config['ExcludedCEs'].strip()
            excluded_ces += re.split('\s+', ce_req)

        # from LCGRequirements object
        # if string starts with '+', it means the requirement to be appeneded
        re_append = re.compile('^(\++)\s*(.*)')
        try:
            ce_req = self.allowedCEs.strip()
            if ce_req:
                m = re_append.match(ce_req)
                if m:
                    allowed_ces += re.split('\s+', m.group(2))
                else:
                    allowed_ces = re.split('\s+', ce_req)
        except KeyError as e:
            pass

        try:
            ce_req = self.excludedCEs.strip()
            if ce_req:
                m = re_append.match(ce_req)
                if m:
                    excluded_ces += re.split('\s+', m.group(2))
                else:
                    excluded_ces = re.split('\s+', ce_req)
        except KeyError as e:
            pass

        # composing the requirements given the list of allowed_ces and
        # excluded_ces
        if allowed_ces:
            requirements += ['( %s )' % ' || '.join(
                ['RegExp("%s",other.GlueCEUniqueID)' % ce for ce in allowed_ces])]

        if excluded_ces:
            #requirements += [ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces ]
            requirements += ['( %s )' % ' && '.join(
                ['(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in excluded_ces])]

        return requirements
