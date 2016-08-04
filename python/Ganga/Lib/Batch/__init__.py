from __future__ import absolute_import
from .Batch import Batch, LSF, PBS, SGE

from Ganga.Utility.Config import getConfig

c = getConfig('Configuration')


