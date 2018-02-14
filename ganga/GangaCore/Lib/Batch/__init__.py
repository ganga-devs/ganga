from __future__ import absolute_import
from .Batch import Batch, LSF, PBS, SGE, Slurm

from GangaCore.Utility.Config import getConfig

c = getConfig('Configuration')


