from __future__ import absolute_import
from .Batch import Batch
from .Batch import LSF
from .Batch import PBS
from .Batch import SGE

from Ganga.Utility.Config import getConfig

c = getConfig('Configuration')


