"""Set RUNTIME_PATH = GangaGUI into order to export the following function the GPI"""

from GangaCore.Runtime.GPIexport import exportToGPI
from GangaGUI.start import start_gui, stop_gui

exportToGPI("start_gui", start_gui, "Functions")
exportToGPI("stop_gui", stop_gui, "Functions")