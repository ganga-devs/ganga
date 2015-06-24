from Batch import Batch, LSF, PBS, SGE

from Ganga.Utility.Config import getConfig

c = getConfig('Configuration')

c.addOption('Batch', 'LSF', 'default batch system')
