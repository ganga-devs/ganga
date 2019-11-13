import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

logger.debug("Loading IApplication")
import GangaCore.GPIDev.Adapters.IApplication
logger.debug("Loading IBackend")
import GangaCore.GPIDev.Adapters.IBackend
logger.debug("Loading ISplitter")
import GangaCore.GPIDev.Adapters.ISplitter
logger.debug("Loading IMerger")
import GangaCore.GPIDev.Adapters.IMerger

logger.debug("Loading GangaList")
import GangaCore.GPIDev.Lib.GangaList
logger.debug("Loading File")
import GangaCore.GPIDev.Lib.File
logger.debug("Loading Job")
import GangaCore.GPIDev.Lib.Job

logger.debug("Loading Mergers")
import GangaCore.Lib.Mergers
logger.debug("Loading Splitters")
import GangaCore.Lib.Splitters

logger.debug("Loading Executable")
import GangaCore.Lib.Executable
logger.debug("Loading Root")
import GangaCore.Lib.Root
logger.debug("Loading Notebook")
import GangaCore.Lib.Notebook

logger.debug("Loading LocalHost")
import GangaCore.Lib.Localhost
logger.debug("Loading LCG")
import GangaCore.Lib.LCG
logger.debug("Loading Condor")
import GangaCore.Lib.Condor
logger.debug("Loading Interactive")
import GangaCore.Lib.Interactive
logger.debug("Loading Batch")
import GangaCore.Lib.Batch
logger.debug("Loading Remote")
import GangaCore.Lib.Remote

logger.debug("Loading Tasks")
import GangaCore.GPIDev.Lib.Tasks


logger.debug("Loading Checkers")
import GangaCore.Lib.Checkers
logger.debug("Loading Notifier")
import GangaCore.Lib.Notifier

logger.debug("Loading virtualization classes")
import GangaCore.Lib.Virtualization

logger.debug("Finished Runtime.plugins")
