import warnings
from ganga.GangaCore.Utility.job_monitoring import *

warnings.warn(
    "The module 'GangaTest.Framework.util' is deprecated and will be removed in a future release. "
    "Please use 'GangaCore.Utility.job_monitoring' instead.",
    DeprecationWarning,
    stacklevel=2
)
