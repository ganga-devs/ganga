from Batch import *

from Ganga.Utility.Config import getConfig

c = getConfig('Configuration')

c.addOption('Batch','LSF','default batch system')
