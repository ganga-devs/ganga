import os.path, time
from qt import *
from GangaGUI.customEvents import *
from GangaGUI.miscDialogs import *
from GangaGUI import inspector, Ganga_Errors
from GangaGUI.cfg_manager import GUIConfig

# Setup logging ---------------
import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger("Ganga.customTables")

DEFAULT_SORT_KEY = 'id'
DEFAULT_ORDER_ASCENDING = True
DEFAULT_FOLDER_AUTO_OPEN_TIME = 500

image_FolderClosed_data = \
    "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d" \
    "\x49\x48\x44\x52\x00\x00\x00\x18\x00\x00\x00\x18" \
    "\x08\x06\x00\x00\x00\xe0\x77\x3d\xf8\x00\x00\x04" \
    "\xb2\x49\x44\x41\x54\x78\x9c\xad\x95\x4b\x6c\x54" \
    "\x55\x18\xc7\x7f\xf3\xb8\xf7\xce\x4c\x67\xa6\x9d" \
    "\xce\xb4\x33\x43\x07\xfa\xa4\xad\x2d\x95\x42\x11" \
    "\x4a\xb5\x52\x20\x68\x68\x83\x0b\x31\x51\x13\x77" \
    "\x04\x16\xb0\x30\x26\x26\x26\xba\xd1\x05\x89\x2b" \
    "\x13\x13\x56\xc4\xe0\x42\x8d\x21\x82\x24\x1a\x35" \
    "\x21\x6a\x8c\x0a\x89\xc4\xd0\x02\xa6\xb5\x45\x3a" \
    "\x42\x1f\xf4\x35\xcf\x4e\x67\xee\x9d\x3b\x73\x8e" \
    "\x8b\xa1\x50\x5e\x22\xc2\x7f\x75\xef\x97\x7b\x7e" \
    "\xbf\x73\xf2\xe5\x7c\x17\x1e\x3d\x16\x60\x8f\x43" \
    "\xb1\x7f\x61\xb5\x5a\x5e\x7f\x0c\xbc\xdb\xb2\xce" \
    "\xe1\x72\x1d\x6c\xea\xea\xfe\x65\xdb\x2b\xfb\x64" \
    "\x73\x57\xf7\x14\x50\x7e\xa7\xfd\x61\xb3\x46\xd1" \
    "\xb4\xbd\xe1\xb6\xce\x37\xfc\x2d\xeb\x57\xb7\xec" \
    "\x18\xc0\xe6\xf1\x23\x15\x95\xf5\x8d\x75\x1c\xde" \
    "\xf9\xc4\x47\xe9\x78\x6c\xff\xf2\xc7\xf6\x87\x00" \
    "\xf7\x79\x43\x91\xb7\xab\x9e\xdc\xbc\xab\xa1\x6f" \
    "\x00\x77\xa4\x81\x4c\x3a\xcd\xe8\x95\x28\xfa\xe2" \
    "\x10\xe9\x74\x92\xcc\x33\x3b\x88\xb4\x76\xb4\x0d" \
    "\x9f\xfd\xe9\xe6\xa2\xff\x22\x78\xd1\x5d\x53\x77" \
    "\xb8\xe6\xe9\xe7\x5b\x23\x3d\x3b\xc1\xa6\x30\x7f" \
    "\x6d\x9c\x89\xbf\xbf\xc7\xae\xaa\x60\xb3\x23\x4d" \
    "\x1d\x5b\xd1\x24\x19\x9b\x25\xab\x1b\xfe\x95\x8b" \
    "\xff\x4d\xb0\xdb\x11\x08\x7f\x58\xd3\x37\xb0\x36" \
    "\xb8\xa9\x97\x82\x61\x30\x39\x3c\x04\x42\x60\x77" \
    "\x96\xb1\x98\x49\x13\x1f\x1f\x23\xb7\x70\x9d\xc8" \
    "\xc6\x1e\x84\x69\xb0\x98\x88\x51\x94\x54\x3f\x48" \
    "\xd0\xae\x78\x7d\x9f\x07\x7b\x9e\xeb\x08\x6c\xe8" \
    "\x41\x8a\x22\x73\xc3\x43\x58\x14\x15\x3d\x97\x25" \
    "\x7e\xed\x0a\x32\x31\x47\x53\x5b\x07\xbd\xbb\xfb" \
    "\x21\x36\xcd\xd7\x5f\x9d\xc2\x56\xdf\x86\x73\x7e" \
    "\x16\x13\x5c\x80\x06\x18\x77\x0a\xac\xc0\xd1\xca" \
    "\x8d\xbd\xfb\xaa\xb7\xec\xc0\xe6\x70\x92\x88\x8e" \
    "\x82\xd5\xc6\x52\x62\x81\xcc\xc4\x15\xfc\xfe\x4a" \
    "\xba\xd6\x77\xa1\x68\x4e\x6a\x3b\xb7\x60\x3a\xbc" \
    "\x6c\xdc\xba\x99\xb9\x54\x9a\x1f\xbf\x3c\x8e\xd3" \
    "\xe5\x02\x45\xd5\x00\x1f\x30\xb3\x52\xb0\x4d\x0b" \
    "\x84\x4e\x56\x3d\x3b\xe0\x77\x86\x22\x64\x66\x27" \
    "\x29\x1a\x06\xfa\x52\x9a\xec\xf4\x55\xc2\xf5\x6b" \
    "\xa9\xdb\xda\x87\xbf\x76\x2d\x9e\x48\x3d\x73\xb3" \
    "\x73\xfc\x7e\xe1\x02\x48\xc1\xd5\xf1\xcb\xd4\x6e" \
    "\xea\x85\xcf\x8e\x91\xb9\x7e\x15\x87\xd5\x06\xe0" \
    "\x59\x29\x78\xcb\x5e\x5e\xf9\x7e\x70\xcf\x6b\x14" \
    "\x52\x09\xe2\x23\x83\x48\x29\x30\x73\x39\x5c\x65" \
    "\x6e\x22\xdd\xdb\xf1\x35\xb4\x22\x14\x27\x53\x33" \
    "\x53\xc8\xe9\x29\xec\x8a\x02\x52\x20\xa5\x64\x26" \
    "\xfa\x27\xad\xbb\xfa\xa9\x08\xaf\x22\x79\xfe\x2c" \
    "\x4a\x67\x37\x40\x08\xb8\x0c\x60\x03\xce\x08\x23" \
    "\x37\x6a\x24\x63\x2f\x09\x55\xc3\x5c\x4c\x62\x66" \
    "\x17\x31\x17\x93\xac\x79\x6a\x1b\x6a\x20\x4c\x6c" \
    "\x7a\x82\x6c\x7c\x9e\xa2\x69\x20\x0a\x79\x4c\x43" \
    "\xa7\x60\xe4\x28\xe4\x75\xd2\x0b\x73\xf8\xeb\x9a" \
    "\x30\xa7\xa2\xcc\x47\xc7\x50\xaa\x23\x98\xb1\x99" \
    "\xf3\x08\x71\x6e\x59\x00\xf0\x47\x21\x19\xbb\x58" \
    "\xc8\x2e\xbe\x2c\x84\xc4\x98\x1c\x47\x64\x97\xd0" \
    "\x82\x6b\xc8\x67\x33\x98\x7a\x96\x82\x91\xc3\xd4" \
    "\x75\x0a\x86\x7e\xe3\x5d\xc7\xd4\x75\x84\x69\x60" \
    "\x14\x05\x16\x33\xcf\xdc\xf0\x20\x36\x4f\x05\x42" \
    "\xcf\xc6\x65\xde\x38\x05\x77\xdf\xe4\x13\xd8\xec" \
    "\x7b\x5d\x75\xcd\x78\x9a\xda\xb1\xa8\x0e\x84\x99" \
    "\x07\x24\x08\x89\x94\x12\x89\x04\x29\x91\x42\x02" \
    "\x02\x0b\x92\x42\x51\x52\xdf\xd1\xc5\x85\x23\xef" \
    "\x22\xbd\x3e\x2c\x42\x44\x45\x26\xd5\xb0\xb2\xc9" \
    "\x87\x2c\x8a\x7a\x44\x0d\x45\x70\xd5\x36\x63\xf3" \
    "\x54\x60\xea\x59\x8a\xe9\x04\x00\x52\xdc\x02\x23" \
    "\x05\xa5\x47\x51\x12\x49\x89\x9e\x8c\x93\x69\x6c" \
    "\xa1\xb2\x6d\x03\x0b\xc3\x83\x48\x45\xad\x07\x2a" \
    "\x81\xf8\xb2\x40\xb3\xfb\xab\xd1\x1a\xdb\xc9\xa7" \
    "\x62\x88\xf8\xfc\x0a\x60\x09\x82\x14\x48\x21\x6e" \
    "\xd5\x84\x04\x59\x44\x16\x4c\x44\x76\x89\xc9\x5f" \
    "\x4f\xe3\xf2\xdd\xb8\xc4\xc5\x02\x40\x37\xf0\xed" \
    "\xb2\xe0\x03\x73\x66\x72\xb6\x90\x5d\xfa\xd4\x52" \
    "\xe6\x01\x33\x0f\x37\x00\x08\x71\x53\x80\x10\x25" \
    "\xb0\x28\x40\xde\x28\xc9\xec\x76\x28\xf3\xa2\xa7" \
    "\x13\x54\xae\xeb\x42\xab\xf0\x63\x24\x63\x60\xb1" \
    "\x1c\x40\xca\xa5\xe5\x26\x03\x5c\xc2\xd0\x85\xcc" \
    "\xa4\xb6\xcb\x6c\x06\x99\x5b\x42\xea\x59\xa4\x91" \
    "\x03\x43\x2f\x01\xcd\x3c\x14\xf2\xa5\x1d\x5a\x6d" \
    "\x58\x03\x21\xb4\xd5\x8d\xb8\x6a\x9b\x71\x47\xea" \
    "\xf1\xd5\xb7\x60\x2d\x9a\x64\x26\xc6\xc1\x62\x09" \
    "\x03\xf1\x7b\x8d\xeb\x6f\x80\xfe\x7b\xd4\xc1\x62" \
    "\x05\x6f\x05\x6a\x55\x18\xd5\x1f\x44\x2b\xaf\xc4" \
    "\xe1\x2e\xc7\x5d\x15\xc6\x13\xac\xc1\x5b\x15\xa6" \
    "\xbc\x7a\x15\xdf\x1d\xdc\x83\x1e\x9b\x05\xf8\xf8" \
    "\x5e\xb3\x68\x3f\x10\x05\xd4\x9b\x15\x67\x19\x36" \
    "\x7f\x08\xb5\x2a\x54\x82\x7a\x2a\xf0\xd6\xd4\x52" \
    "\xb1\xaa\x0e\x45\x73\xa0\xcf\x4d\x93\x8b\x8e\x92" \
    "\x1d\x19\x64\x41\xd5\x74\xbb\xc3\x99\x02\xce\x00" \
    "\xa7\xef\xf7\xc3\x39\x64\xd1\x1c\x47\xac\x15\x01" \
    "\xec\xbe\x2a\xb4\x40\x10\xd5\xed\xc5\x1d\x08\x51" \
    "\x59\xd7\x8c\xea\x74\xb3\x18\x1d\x61\x69\x62\x9c" \
    "\x54\x74\xf4\xb7\xc4\xd8\xc5\x9f\x81\x61\xe0\x2c" \
    "\x30\x76\xdb\xa1\xef\x23\xc0\xe6\xf5\x0d\xfa\xb7" \
    "\xbf\xd0\x69\x95\x02\x6f\x30\x42\xa0\xa9\x1d\x23" \
    "\xb1\x40\xfc\xd2\x39\xe2\x23\x83\x43\xa9\xf1\x91" \
    "\xe3\xc0\x09\xe0\xaf\xfb\x31\x1e\x94\xbd\x91\xfe" \
    "\x57\xe5\xae\x63\x3f\xc8\xad\xef\x1d\x95\x91\x67" \
    "\xfb\xa5\xdd\xe1\xfa\x04\xd8\xf2\x7f\x81\x77\x25" \
    "\xd8\xd5\x7b\xbd\xf3\xc0\x3b\xd2\xee\x70\x9d\xa4" \
    "\x34\xc0\x1e\x6f\xac\x8a\xf6\xa6\xd5\xae\x1c\x7e" \
    "\x14\xc6\x3f\x56\x68\x2c\x61\x2c\xc7\xef\xfc\x00" \
    "\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82"
image_FolderOpen_data = \
    "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d" \
    "\x49\x48\x44\x52\x00\x00\x00\x18\x00\x00\x00\x18" \
    "\x08\x06\x00\x00\x00\xe0\x77\x3d\xf8\x00\x00\x05" \
    "\x87\x49\x44\x41\x54\x78\x9c\xa5\x95\x5b\x6c\x1c" \
    "\x77\x15\xc6\x7f\x73\xd9\x59\xef\x7d\xc7\x7b\xf3" \
    "\x3a\x6b\x67\xb3\x4e\x1c\x67\x9d\xd8\x4e\x1c\x07" \
    "\x37\x37\x22\x52\xb5\xa0\xb6\xa2\xa0\x22\x55\xad" \
    "\x90\x20\xa8\x91\xfa\x12\x09\x90\x5a\x2e\x0f\x48" \
    "\x14\xc4\x4b\x5e\x10\x12\x82\xa2\x82\x52\x08\x24" \
    "\x45\x44\x4a\x29\xa2\x88\x90\x12\x42\xa9\xd3\xd6" \
    "\x71\x69\x1c\xbb\x86\xc4\xd7\x4d\xd6\xbb\xb1\xd7" \
    "\x7b\x9b\x99\xf5\xee\xec\x0c\x0f\x6d\xd2\x10\x9c" \
    "\x16\x29\xe7\xe9\xff\xd7\xf9\xce\xf7\x7b\x3a\xe7" \
    "\x93\xb8\xf7\x0a\x00\x8f\xb9\x9d\xf2\x57\x1b\x4d" \
    "\x0b\x60\xea\xf6\xa6\x70\x0f\xc6\x29\xe0\x81\x8d" \
    "\x83\xc3\xfb\x43\xa9\x9e\x87\x92\x3d\x9b\xfd\x97" \
    "\xfe\xf2\xea\xc2\xc4\x3f\xce\xf5\x01\xc5\x7b\x01" \
    "\x7c\xa5\x3d\xdd\xff\x60\xbc\x6f\xd7\x17\x82\x5d" \
    "\x69\x3a\xb7\xdf\x47\xd9\xa8\xe1\x09\xaa\xac\x77" \
    "\x3b\x38\xfa\xf0\xce\x1f\x18\x86\xfe\xad\x9b\x62" \
    "\xf9\xff\x34\xed\x10\x45\xf1\xa9\xce\x3d\xf7\x7f" \
    "\x39\xd2\xbb\x33\x11\xdf\xb1\x07\x53\x90\x28\x64" \
    "\x33\xbc\x71\xf6\x4f\x48\x82\x4d\x59\xd3\xd8\xba" \
    "\xb5\x0f\x7f\x28\xd2\x69\x64\xe6\x6e\x0d\x7e\x1c" \
    "\x20\x29\x3a\x94\xef\xae\x3f\xf8\xd9\x2f\x46\xfa" \
    "\x87\xf1\xc4\x3b\x31\x2a\x65\xfe\x75\xf1\x02\xd8" \
    "\x16\x92\xe2\x44\x94\x44\x1a\x5a\x05\x4b\xaf\xa2" \
    "\x95\x8a\x58\xa2\xf8\x5f\x06\x77\x03\xb8\x11\x84" \
    "\x1f\xb5\xed\x79\xe0\x50\x6c\xe8\x00\xae\x70\x1b" \
    "\xfa\x52\x8e\xd2\xf8\x28\x82\xc3\x81\x56\xad\x50" \
    "\xcd\x67\xa1\x5a\xa4\xb2\x9c\x23\xd4\xdd\x87\x2d" \
    "\x40\xa5\xb4\x82\x65\x0b\xee\x8f\x03\x3c\xab\x6e" \
    "\x1d\xfa\x76\x64\xd7\x01\x9f\xa2\x46\xa9\x2e\xdf" \
    "\x20\x3f\xfd\x6f\x9a\xb6\x4d\xbd\x5c\xc4\xac\x14" \
    "\x49\xc4\xdb\x68\x0f\xab\x0c\x7c\xee\x61\xb4\xf9" \
    "\x79\x5e\x7e\xe9\xd7\x58\xed\x49\xca\x81\x20\x96" \
    "\xe2\xf4\xdd\x0d\xb0\xc7\x19\x89\xff\x22\xb6\xff" \
    "\xa1\x4d\x9e\x44\x8a\xa6\x5e\xc6\x2c\x2e\x22\x62" \
    "\x12\x8c\xa8\x88\x80\xae\xe5\xe9\xdc\x3d\x4c\xb8" \
    "\x77\x88\x5c\xcd\x62\x24\x5f\xe5\xc9\x2f\x3d\xc3" \
    "\xe2\xf5\x6b\x9c\x39\x71\x0c\x6f\x30\x88\xe8\x6c" \
    "\x51\xef\x04\xf4\xcb\x1e\xdf\x21\x6f\x77\xdf\x11" \
    "\x75\xe0\x3e\x2c\xd3\x24\x3f\x3e\x8a\x3f\x1a\x26" \
    "\x96\x5c\x47\x7c\xc3\x06\xd2\xa9\x0e\xb6\xc5\xc3" \
    "\xc4\xd4\x56\x0c\x13\x66\x66\xa6\x31\x8c\x3a\x13" \
    "\xf3\x26\xc7\x5f\x39\x85\x82\x02\xab\x1a\xc6\x8d" \
    "\x45\x44\x51\x4e\xde\x09\xb8\x5f\x89\xae\x3b\x12" \
    "\xd8\xb1\x97\x6a\x66\x1a\xd3\xd0\xb0\x4c\x93\xe5" \
    "\x99\x12\xac\x36\x40\xf0\x92\x5d\x58\xe2\x72\x34" \
    "\x40\x6f\x77\x82\xcd\x91\x20\xaa\x57\xa1\x3b\x1e" \
    "\xe6\xc1\xed\x3d\x54\xea\x02\xdf\x13\x75\x38\x06" \
    "\x95\x4b\x6f\xe1\xeb\x4a\x87\x80\xf5\xc0\x1c\x80" \
    "\x04\xbc\xd1\x28\x2e\x5d\xad\x2d\x2d\x7e\x1e\xb7" \
    "\x07\xb3\x52\xc6\x28\xe4\x71\x38\x1c\xb4\x0f\xee" \
    "\xa3\x94\xcf\x51\xbe\x51\x60\x6e\x2e\xcb\xc4\xd5" \
    "\x0c\x35\x59\x21\x16\x51\x99\xcf\xe7\x19\x99\x9e" \
    "\x25\x1d\x0d\x72\xf8\xd3\x9f\xe2\xdc\xb2\xc1\xc2" \
    "\xeb\x67\x11\x3d\x3e\xcc\x95\xa5\xf3\xc0\xe4\x4d" \
    "\x00\xc0\xbb\xcd\xf2\xca\x58\x43\xab\x7c\xc6\xd4" \
    "\x2a\x2d\xcd\xc5\x05\x24\xb7\x17\x29\x10\xc2\xac" \
    "\x69\x58\x66\x1d\xa1\x59\x47\x5f\x5e\xe6\xf2\xf8" \
    "\x14\x25\x41\x66\xfb\x96\x8d\xb4\x79\x5c\x8c\x66" \
    "\xb2\x84\x9c\x0e\x9e\x7e\xfc\x31\x5e\xfc\xfb\x28" \
    "\xc5\x8b\x23\x80\x3d\x0b\x9c\xb9\x1d\x00\x90\xb5" \
    "\x0d\xed\x09\xb9\xc5\x1d\x0e\xf4\x0e\xe2\x49\x76" \
    "\x53\xd7\x2a\x34\xf4\x2a\x8d\x9a\x8e\x59\x33\xc0" \
    "\x36\xb1\xab\x25\xa6\xde\x19\x67\xd5\xaf\xb2\xbf" \
    "\x3f\x4d\x2a\xa8\x32\x57\x58\x61\xa0\x3d\x4e\xdb" \
    "\xc0\x4e\x4e\x1f\x7b\x11\xcc\xd5\x2c\xf0\xdb\xd8" \
    "\xe0\x30\x12\xe0\x01\xfe\x2c\xab\x91\x9f\xb8\x7b" \
    "\x06\xc2\xee\x54\x0f\xa2\xcb\x4d\xbd\xb8\x4c\x43" \
    "\xab\x60\xae\xea\x34\x6b\x3a\x66\x4d\xc7\x34\x74" \
    "\x68\x9a\x34\x0a\x37\x98\x99\xcd\x90\xdc\xb1\x9d" \
    "\x5d\x9d\x6d\xf8\x5a\x5c\x68\x35\x9d\xbd\x5d\x49" \
    "\x7e\xf3\xce\x65\x8a\x13\xef\xba\x3d\xc9\x8d\x3f" \
    "\xfc\xc4\x37\x9f\x43\x02\x44\xa0\xcb\x11\x6d\xdf" \
    "\x27\x85\xa2\xac\x2e\xe7\xa8\x17\x0b\x98\xc6\xfb" \
    "\xc6\x4d\x43\xc3\x34\x74\x4c\xfd\x83\xb7\x5e\x45" \
    "\x6c\x9a\x34\xaf\x4c\x52\xf0\x06\x38\xb0\x6f\x37" \
    "\x5d\x01\x3f\x46\xdd\xa4\xc3\xe7\xe3\xf5\xe9\x79" \
    "\x26\xce\xbe\xa6\x0e\x1f\xf9\xfa\xce\x70\x2c\xee" \
    "\xbf\xfd\xd8\x1d\x16\xbc\xfe\x9f\x0a\x4e\x17\xb6" \
    "\xd9\x00\xdb\x06\xcb\x02\xab\x89\x6d\xdb\x1f\xfe" \
    "\xb1\xa0\xf1\x41\x7f\x43\x0f\xdf\x7f\xe9\x24\x4f" \
    "\xf5\x76\x51\x6f\x34\x09\xbb\x5c\x8c\x5d\xcb\x31" \
    "\x5f\x35\xd8\xb2\x2e\xc6\x77\x8e\xff\xee\xf4\xed" \
    "\x8b\xf6\xbc\x5d\x2d\xa7\xec\x6a\xf9\xd9\x35\xb6" \
    "\xfb\x7f\x4a\x4e\xa4\x70\xa5\x07\xb8\x32\x3b\x43" \
    "\x75\x63\x82\x84\x3f\x48\xa5\x56\x63\x38\x99\x60" \
    "\x18\xb8\xb8\x70\x8d\x52\xbd\x31\x76\xe7\xa9\xf8" \
    "\x06\xf0\x08\x90\x5e\xd3\x55\x69\x41\x8e\xb6\x33" \
    "\x7c\xe8\x30\x83\xfb\x77\xb3\x2d\xd2\x4a\xaf\xcf" \
    "\x8d\x47\x56\x00\xf0\x2a\x0a\xd5\xd5\x55\xbc\x4e" \
    "\x27\xf3\x2b\x25\x40\xbc\xb0\xd6\x2d\xda\x0b\x14" \
    "\x3e\x34\x75\x22\xaa\x61\x1c\xad\x51\x14\x35\x82" \
    "\x12\x68\xa5\xac\x35\xd1\x0d\x8b\xf6\x48\x2b\x43" \
    "\xb1\x30\x92\xe8\xa0\xde\x6c\xe2\x90\x65\x14\xf9" \
    "\x7d\xcb\xb1\xcc\xe2\x85\x80\xcf\xf5\xaa\xb4\x06" \
    "\xa0\x06\xd4\xc5\x60\xf8\xa0\xbc\x6e\x03\xae\xf5" \
    "\x9b\xf0\x74\x6e\x22\xd0\x91\x22\x96\xde\x41\xac" \
    "\x7b\x2b\x75\xad\xc6\xc8\x89\xe3\xbc\x35\x36\x5a" \
    "\xf1\x6d\xe9\x75\xf6\x47\x42\x48\xa2\x78\x2b\xbd" \
    "\xe6\x56\x4a\x9c\x1e\x9f\xfc\x59\x5b\xc0\x7b\xee" \
    "\xae\x89\xe6\x88\x25\x8a\xd1\x83\x8f\x06\x64\xcb" \
    "\x22\xd4\x95\xc6\xe1\x50\xd0\x32\xd3\xe4\x47\xcf" \
    "\x5f\x2f\x4f\x4f\xfe\xbe\x56\x2a\xfc\xd2\x19\x8d" \
    "\xbd\xfd\xf4\x89\x97\x9f\x1c\x52\x5d\x2f\x3c\x31" \
    "\xb0\xed\xd6\xec\xa9\x4b\x93\xe6\x6b\x53\x57\x43" \
    "\x61\x8f\xbb\x7c\xd7\xc0\x69\xe4\x32\xcf\x05\xfd" \
    "\xea\xd1\xc4\xd0\x27\xc9\xbd\xfd\x37\xde\xfb\xe3" \
    "\xc9\xf7\x4a\xb3\x53\x47\x81\x17\x6e\x6a\x5a\x63" \
    "\x71\x82\x8a\xf4\xf3\x37\x67\xb3\x39\xb7\x43\x79" \
    "\xe5\xd1\xde\xcd\xe4\xab\x1a\xe3\xd9\xc5\x1f\x6f" \
    "\x8a\x84\xca\x82\x20\x7c\x64\xa2\xfd\x6a\xe5\xe2" \
    "\xf9\xa3\xd5\x2b\x97\xb5\xb9\x33\xa7\xbe\x06\x3c" \
    "\x7f\xa7\xc0\xb2\x6d\x9a\x16\x44\xbc\xca\x1f\xfe" \
    "\x79\xed\xfa\xe3\x1d\x41\xff\x89\xa5\xaa\x8e\x43" \
    "\x74\x3e\x23\x4b\x0a\x36\xf6\x47\x02\x72\xd7\xdf" \
    "\xfc\xeb\x23\xc0\x08\xb0\xb4\x96\x40\x96\x24\x5a" \
    "\x7d\x6e\x04\xc1\x40\x12\xc5\x93\xa3\x99\x5c\x8f" \
    "\x2c\xd9\x0b\xa1\x60\xcb\xaa\x80\x00\x36\xfc\x07" \
    "\x03\xdc\x4c\x8b\xb9\x6f\x03\xb4\x00\x00\x00\x00" \
    "\x49\x45\x4e\x44\xae\x42\x60\x82"
image_Job_data = \
    "\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d" \
    "\x49\x48\x44\x52\x00\x00\x00\x10\x00\x00\x00\x10" \
    "\x08\x06\x00\x00\x00\x1f\xf3\xff\x61\x00\x00\x03" \
    "\x50\x49\x44\x41\x54\x78\x9c\x4d\xd3\xdf\x6b\x5b" \
    "\x65\x00\xc6\xf1\xef\xfb\x9e\x5f\x39\x39\x69\xd2" \
    "\xa4\x5d\x5c\x5d\x6d\xd5\x4e\xb6\x4e\xdd\x94\x8a" \
    "\x82\x88\xf4\xa6\x5e\x0c\x05\x11\xef\x86\xf3\x42" \
    "\xfc\x0b\xe6\xcf\x09\x82\x32\x18\x7a\x2d\xf3\x46" \
    "\xd1\x89\x14\x45\xb1\x30\x94\x49\xa9\x0c\x51\xa7" \
    "\x63\xa3\x9b\x93\x3a\xb1\xb6\xcb\xda\xda\x34\xc9" \
    "\x49\xd3\x24\xe7\x9c\xe4\xe4\xe4\x9c\xd7\x8b\xa2" \
    "\xf8\xf9\x03\xbe\x57\xcf\x23\x9e\x78\xf3\x18\x42" \
    "\xd8\x80\x00\x14\x42\xe8\x28\xa5\x70\x5b\x0d\x5a" \
    "\x9d\x80\x6c\x3a\x7d\xdf\xd8\x9e\x3b\x66\x7a\x51" \
    "\xab\x14\xf6\x1a\x73\x9a\x96\x43\x08\x03\x48\x50" \
    "\x4a\x43\xe7\x3f\x02\x4d\x0a\x6a\xcd\x06\x0d\xdf" \
    "\xdb\x77\xe4\xae\x7b\x8e\x3d\x3a\x39\xf5\xcc\x63" \
    "\x07\x0f\x3f\x32\x71\xfb\x18\xef\x7e\xf5\xf1\xf2" \
    "\xdc\xc5\xf9\xb9\x7d\x43\x59\xa4\xe8\x20\xa5\x86" \
    "\x14\x72\x37\xa0\x49\x49\xa7\x17\x72\xab\x56\x36" \
    "\xa6\x26\xee\x3f\x75\x72\xfa\xc9\x57\x9e\x7e\x78" \
    "\x9a\xff\x9b\x9a\x98\x1c\xfd\xe6\xca\x8f\xcf\x17" \
    "\xb2\x05\xad\x17\x05\x6d\xbf\x1b\xb8\xc8\x64\x4b" \
    "\x1c\x7d\xeb\x38\xb5\x66\x80\x1f\x86\x33\xaf\x3e" \
    "\xfb\xe2\xec\xf1\xe9\xa7\x86\x01\x14\x70\x71\x65" \
    "\x83\xba\xdf\x65\xe6\xe0\x38\xb6\x69\x70\x69\xbd" \
    "\x81\xae\xeb\xa0\x12\x6c\x4b\xf1\xd1\xf9\xcf\x96" \
    "\xf5\xea\x4e\x1d\x5d\xcf\xbd\x30\xfb\xd2\xa9\x0f" \
    "\x8e\xdc\x79\x00\x3f\x4a\xb8\xbe\x51\xa1\x17\xf7" \
    "\xb1\x0d\x83\xd1\x7c\x8e\x9a\x1f\x32\x66\x1a\x0c" \
    "\x67\x1c\xdc\x20\x62\x20\x65\x93\x1f\xd0\x09\x3a" \
    "\xcd\x79\xbd\x90\xbd\xed\xb5\xcf\x5f\x7e\xef\xf4" \
    "\xa0\x63\xf3\x4b\x69\x8b\x6e\x3f\x66\xc0\x32\xc9" \
    "\xa5\x53\x08\x20\x4a\x14\xcd\x6e\x8f\x5e\x9c\x60" \
    "\x8a\x3e\xed\x8e\x8f\x6d\xe8\xac\x6c\xee\xb0\x56" \
    "\x29\x5d\x96\x71\x12\x96\xfb\x0a\xaa\x7e\x44\x2f" \
    "\x56\x0c\xa5\x6d\x86\x1c\x9b\x28\x56\x6c\xfb\x21" \
    "\x75\xaf\x4b\xcd\xeb\x22\xa4\x24\x97\xb2\x50\x2a" \
    "\x41\x4a\x9d\xea\x4e\x85\x76\xd0\xf9\x43\x6e\xba" \
    "\x9b\x67\x5f\xff\xe4\x9d\x2f\x8a\x8e\xc1\xb0\x93" \
    "\x66\xa7\x1b\x51\xf3\x3a\x74\xa3\x18\x21\x04\xc5" \
    "\xac\x43\xde\xb1\xb9\xb6\xb1\x8d\x6d\x4a\x1c\x43" \
    "\x10\x2b\x41\xbd\x59\x57\x9d\x5e\xa7\x24\xc7\x8b" \
    "\x23\x7c\x77\x75\xfe\xb9\xaf\xaf\x5e\x51\x07\xf6" \
    "\xe6\x40\x08\x34\xdd\x44\xd7\x75\x12\x05\x6b\xf5" \
    "\x36\xe5\xa6\x4f\x18\x43\x5f\x41\xde\x36\x09\x7a" \
    "\x11\x3b\xde\x76\x25\x8e\x7b\x15\xa9\x30\x19\x1e" \
    "\xd8\x13\x7e\xf8\xed\xd9\x37\xdc\x4e\x42\xca\xcc" \
    "\xf0\xe7\x96\xcb\xb5\xf5\x0a\x7f\xb9\x2d\x2c\xd3" \
    "\xa0\x98\x1d\x40\xd7\x24\x41\x5f\x90\x77\xd2\xf4" \
    "\xe3\x3e\x5e\xd0\xae\xa1\x40\x2a\x15\x33\x9c\x1b" \
    "\xe4\xb7\xd5\xc5\xd3\xe7\x2f\x5f\xf0\x34\xc3\xc2" \
    "\x6d\xfb\x64\x4c\x03\x5b\xd7\x08\xfb\x8a\xba\xd7" \
    "\xa5\xda\xf4\xf8\x75\xc3\x25\x65\x9a\xe4\x2c\x9d" \
    "\x66\xe0\xd7\x85\xd0\x90\x60\x13\x27\x29\x8a\x83" \
    "\x63\x9c\xfb\xe9\xdc\xc9\xac\xd9\x67\xb4\x50\x20" \
    "\x8e\x13\x92\x24\xa1\xde\x6a\x71\xcb\x75\xd9\x6a" \
    "\xb6\x68\x77\x3a\x68\x9a\x60\x24\x6f\xd1\x0a\x3c" \
    "\x5f\x93\x02\x09\x11\x10\x91\xcb\xa4\xb9\xb1\xb6" \
    "\x74\xe6\xe7\xa5\x4b\xc1\xbd\xe3\xa3\xf8\x61\x88" \
    "\xdb\x6e\x63\x5b\x16\x33\x87\x27\x39\xfa\xe0\x21" \
    "\xc6\x8b\x7b\xb9\xbe\xe9\xb3\xd6\x50\x38\x96\x49" \
    "\x9c\xf4\xd1\x41\x02\xa0\x12\x81\x63\x65\xe2\x0b" \
    "\x57\x17\xde\x7f\xe8\xd0\xe3\x27\x1e\xb8\x7b\x3f" \
    "\xfb\x47\x52\xb4\x03\x58\x5c\x59\x65\xc3\xad\xe2" \
    "\x77\x3d\xc2\x28\x24\x97\xb6\x69\x79\xdb\xdd\x94" \
    "\x91\x42\x87\x98\x7f\xa7\x5b\xc8\x0e\xf2\x7b\x69" \
    "\xe9\xcc\x8d\x9b\x8b\x27\x34\x91\x78\xb3\xf3\xdf" \
    "\x7f\x79\xb3\x5c\x5e\x2e\x6f\x57\x4b\x8e\xa5\xfd" \
    "\x2d\x49\x1a\x61\x14\x35\xfa\x71\x1c\xd9\xa9\x74" \
    "\x73\xc0\xc9\xa2\xef\x5e\x73\x97\x94\x12\xdb\x12" \
    "\xab\xb3\x0b\x9f\xbe\xdd\xf2\x9a\x0b\xeb\xb5\xad" \
    "\x1f\x32\x76\x8a\x8c\x6d\x83\x12\xc4\x80\x65\xd8" \
    "\xa4\x53\x3a\xa0\x88\x93\x98\x7f\x00\xa3\x84\x96" \
    "\x7e\xca\x51\x98\x28\x00\x00\x00\x00\x49\x45\x4e" \
    "\x44\xae\x42\x60\x82"


class AttributeSelectionListView( QListView ):
   cStateDict = { True: QCheckListItem.On, False: QCheckListItem.Off }
   baseList = [ 'Job.id', 'Job.status' ]
   def __init__( self, parent = None, componentDict = None, currentSelection = None ):
      QListView.__init__( self, parent )
      if componentDict is None:
         componentDict = {}
      if currentSelection is None:
         currentSelection = [ 'Job.id', 'Job.status' ]
      self.componentDict = componentDict
      self.currentSelection = currentSelection
      self.commandMap = {}
#      self.ResizeMode( QListView.AllColumns )
      self.setRootIsDecorated( True )
      self.__populate()

   def __populate( self ):
      self.addColumn( 'Command' )
      self.addColumn( 'Displayed Text' )
      self.setAllColumnsShowFocus( True )
      
      #Job
      componentItem = 'Job'
      cLVItem_Job = QListViewItem( self, componentItem )
      cLVItem_Job.setRenameEnabled( 0, False )
      for attrDict in self.componentDict[ componentItem ]:
         if attrDict[ 'hidden' ]:
            continue
         _cmd = attrDict[ 'objPrefix' ]
         self.commandMap[ _cmd ] = attrDict[ 'objPrefix' ]
         cText = self.commandMap[ _cmd ].replace( 'Job.', '' )
         _cLVItemCommand = QCheckListItem( cLVItem_Job, cText, QCheckListItem.CheckBox )
         _cLVItemCommand.setState( self.cStateDict[ _cmd in self.currentSelection ] )
         _cLVItemCommand.setEnabled( _cmd not in self.baseList )
         _cLVItemCommand.setText( 1, cText )
         _cLVItemCommand.setRenameEnabled( 1, True )
      
      #Applications and Backends
      categories = self.componentDict.keys()
      categories.remove( 'Job' )
      for category in categories:
         cLVItem_Category = QListViewItem( self, category )
         cLVItem_Category.setRenameEnabled( 0, False )
         for component in self.componentDict[ category ]:
            cLVItemController_Component = QCheckListItem( cLVItem_Category, component, QCheckListItem.CheckBoxController )
            for attrDict in self.componentDict[ category ][ component ]:
               if attrDict[ 'hidden' ]:
                  continue
               _cmd = attrDict[ 'objPrefix' ]
               self.commandMap[ _cmd ] = attrDict[ 'objPrefix' ]
               cText = self.commandMap[ _cmd ].replace( 'Job.'+component+'.', '' )
               _cLVItemCommand = QCheckListItem( cLVItemController_Component, cText, QCheckListItem.CheckBox )
               _cLVItemCommand.setState( self.cStateDict[ _cmd in self.currentSelection ] )
               _cLVItemCommand.setEnabled( _cmd not in self.baseList )
               _cLVItemCommand.setText( 1, component + ' ' + cText )
               _cLVItemCommand.setRenameEnabled( 1, True )

   def getSelection( self, parentItem = None, cmd = None ):
      if parentItem is None:
         parentItem = self
      if cmd is None:
         cmd = ''
      _jobfields = []
      _i = parentItem.firstChild()
      while _i:
         if parentItem is self:
            cText = cmd
         else:
            if cmd is '':
               cText = str( _i.text( 0 ) )
            else:
               cText = cmd + '.' + str( _i.text( 0 ) )
         _jobfields.extend( self.getSelection( _i, cText ) )
         _i = _i.nextSibling()
      if not parentItem.firstChild() and parentItem.state() == QCheckListItem.On:
         pos = cmd.rfind( '.' )
         if pos != -1: # an application or backend attribute
            cmd_lhs = cmd[ :pos ]
            if cmd_lhs in inspector.plugins( 'applications' ):
               _type = 'application'
            elif cmd_lhs in inspector.plugins( 'backends' ):
               _type = 'backend'
            else:
               raise Ganga_Errors.TypeException( "%s not a recognised Ganga application or backend!" % cmd_lhs )
            gangaCmd = _type + cmd[ pos: ]
         else:
            gangaCmd = cmd
         _jobfields.append( ( str( parentItem.text( 1 ) ), gangaCmd, 'Job.' + cmd ) )
      return _jobfields


class LVItem( QListViewItem ):
   def __init__( self, parent = None ):
      QListViewItem.__init__( self, parent )
      self.setDragEnabled( True )
      self.fg = None
      self.bg = None

   def compare( self, other, column, ascending ):
      try:
         return int( str( self.key( column, ascending ) ) ) - int( str( other.key( column, ascending ) ) )
      except:
         return self.key( column, ascending ).compare( other.key( column, ascending ) )

   def setText( self, column, textString, colourTuple = None ):
      if colourTuple:
         self.fg, self.bg = colourTuple
      QListViewItem.setText( self, column, textString )

class ColourLVItem( LVItem ):
   def __init__( self, parent, itemText, fg, bg ):
      LVItem.__init__( self, parent )
      self.columnColour = { 0 : ( fg, bg ) }
      self.setColouredText( 0, itemText, fg, bg )

   def setColouredText( self, column, text, fg, bg ):
      self.columnColour[ column ] = ( fg, bg )
      self.setText( column, QString( text ) )
      self.repaint()

   def paintCell( self, p, cg, column, _width, _alignment ):
      _cg = QColorGroup( cg )
      _cg.setColor( QColorGroup.Text, self.columnColour[ column ][ 0 ] )
      _cg.setColor( QColorGroup.Base, self.columnColour[ column ][ 1 ] )
      LVItem.paintCell( self, p, _cg, column, _width, _alignment )


class JobLVItem( LVItem ):
   def __init__( self, parent, jid, jobDetails, colourTuple = None ):
      LVItem.__init__( self, parent )
      if colourTuple:
         self.fg, self.bg = colourTuple
      self.setText( 0, QString( jid.split( '.' )[ -1 ] ) )
      for pos in range( len( jobDetails ) ):
         self.setText( pos + 1, QString( jobDetails[ pos ] ) )

   def paintCell( self, p, cg, column, _width, _alignment ):
      _cg = QColorGroup( cg )
      old_fg = QColor( _cg.text() )
      old_bg = QColor( _cg.base() ) # background()
      if not self.fg:
         self.fg = old_fg
      if not self.bg:
         self.bg = old_bg
      _cg.setColor( QColorGroup.Text, self.fg ) # Qt.lightGray
      _cg.setColor( QColorGroup.Base, self.bg )
      LVItem.paintCell( self, p, _cg, column, _width, _alignment )
      _cg.setColor( QColorGroup.Text, old_fg )
      _cg.setColor( QColorGroup.Base, old_bg )


class SortedListView( QListView ):
   def __init__( self, parent = None, name = None ):
      QListView.__init__( self, parent, name )
      self.setShowSortIndicator( True )
      self.setRootIsDecorated( True )
      self.setAllColumnsShowFocus( True )
      self.setSelectionMode( QListView.Extended )
      self.resizeMode = QListView.AllColumns
      self.contextMenu = QPopupMenu( self )
      self.iterator = None


class Jobs_ListView( SortedListView ):
   def __init__( self, parent = None, actionsDict = {}, cmCallback = None ):
      SortedListView.__init__( self, parent )
      self.actionsDict = actionsDict
      if cmCallback is None:
         cmCallback = lambda:None
      self.__cmCallback = cmCallback
      self.fields = []
      self.indexDict = {}
      self.expandedJobs = {}
      self.__statusPos = None
      self.__currDragItem = None
      self._colourSchema = GUIConfig.getDict( 'Status_Colour_Schema' )
      # Slots
      self.connect( self, 
                    SIGNAL( 'contextMenuRequested( QListViewItem*, const QPoint&, int )' ), 
                    self.slotContextMenuRequested )

   def slotContextMenuRequested( self, qItem, point, column ):
      self.contextMenu.clear()
      self.__cmCallback()
      self.actionsDict[ 'New' ].addTo( self.contextMenu )
      self.actionsDict[ 'NewFromTemplate' ].addTo( self.contextMenu )
      self.actionsDict[ 'Open' ].addTo( self.contextMenu )
      self.contextMenu.insertSeparator()
      self.actionsDict[ 'Submit' ].addTo( self.contextMenu )
      self.actionsDict[ 'GetOutput' ].addTo( self.contextMenu )
      self.actionsDict[ 'Extras' ].addTo( self.contextMenu )
      self.contextMenu.insertSeparator()
      self.actionsDict[ 'SaveAsTemplate' ].addTo( self.contextMenu )
      self.actionsDict[ 'Copy' ].addTo( self.contextMenu )
      self.actionsDict[ 'CopySubmit' ].addTo( self.contextMenu )
      self.contextMenu.insertSeparator()
      self.actionsDict[ 'Remove' ].addTo( self.contextMenu )
      self.actionsDict[ 'Kill' ].addTo( self.contextMenu )
      self.contextMenu.insertSeparator()
      self.actionsDict[ 'HeaderChange' ].addTo( self.contextMenu )
      self.contextMenu.exec_loop( point )

   def customEvent( self, myEvent ):
      if myEvent.type() == CALLBACK_EVENT:
         self.updateLV( myEvent.jobStatusDictFunc )

   def dragObject( self ):
#      log.debug( "drag initated" )
      currItem = self.currentItem()
      self.__currDragItem = currItem
      if not currItem or currItem.parent() is not None:
         return None
      return QTextDrag( str( currItem.text( 0 ) ), self )

   # This method may be called by a child thread 
   # hence the use of the postEvent() method.
   def updateLV_Helper( self, jobStatusDictFunc ):
      QApplication.postEvent( self, Callback_CustomEvent( jobStatusDictFunc ) )

   def clear( self ):
      SortedListView.clear( self )
      for col in range( self.columns() ):
         self.removeColumn( 0 )
      self.indexDict.clear()
      self.expandedJobs.clear()

   def __getStatusColour( self, status, subjob = False ):
      try:
         colourTuple = self._colourSchema[ status ]
      except:
         colourTuple = ( 'black', None )
      fg = colourTuple[ 0 ]
      bg = colourTuple[ 1 ]
      subjobColourMask = self._colourSchema[ '*subjob*' ]
      if fg is not None:
         modifiedFG = QColor( fg )
         if subjob:
            h, s, v = modifiedFG.getHsv()
            modifiedFG.setHsv( h, s, subjobColourMask[ 0 ] )
         fg = modifiedFG
      if bg is not None:
         modifiedBG = QColor( bg )
         if subjob:
            h, s, v = modifiedBG.getHsv()
            modifiedBG.setHsv( h, s, subjobColourMask[ 1 ] )
         bg = modifiedBG
      return fg, bg

   def find( self, findText, next ):
      if next == True:
         self.iterator += 1
         if not self.iterator.current():
            self.iterator = QListViewItemIterator( self )
      else: # reset iterator
         self.iterator = QListViewItemIterator( self )
         # move the iterator to the current position i.e. current highlighed item
         while self.iterator.current() != self.currentItem():
            self.iterator += 1
      startItem = self.iterator.current()
      cycle = None
      while self.iterator.current() != cycle:
         item = self.iterator.current()
         for col in range( self.columns() ):
            if str( findText ) in str( item.text( col ) ):
               self.clearSelection()
               self.ensureItemVisible( item )
               self.setSelected( item, True )
               return True
         self.iterator += 1
         # End of list view reached, reset iterator to top of list view.
         if not self.iterator.current():
            self.iterator = QListViewItemIterator( self )
            # set the cycle flag to prevent infinite loop!
            cycle = startItem
      return False

   def refresh( self ):
      self._colourSchema = GUIConfig.getDict( 'Status_Colour_Schema' )
      self.updateLV()

   def updateLVHeaders( self, fields ):
      self.clear()
      for pos in xrange( len( fields ) ):
         if fields[ pos ][ 1 ] == 'status':
            self.__statusPos = pos - 1
         self.addColumn( fields[ pos ][ 0 ] ) # tuple position 0 contains field text that is can be user defined.
      # jobStatusDict does not have id. Hence the shift of field index by 1.
#      self.__statusPos = self.fields.index( 'status' ) - 1 

   def updateLV( self, jobStatusDictFunc ):
      jobStatusDict = jobStatusDictFunc()
      def _getParent( jid ):
         try:
            _parentID = jobStatusDict[ jid ][ 'meta' ][ 'master' ]
         except:
            _parentItem = self
         else:
            try:
               _parentItem = self.indexDict[ _parentID ]
            except:
               _parentItem = self
         return _parentItem
      if jobStatusDict == {}:
         return
      elif jobStatusDict is None: # refresh... possibly to change colours
         jobLVItemIterator = qt.QListViewItemIterator( self )
         columns = self.columns()
         while jobLVItemIterator.current():
            i = jobLVItemIterator.current()
            status = str( i.text( self.__statusPos + 1 ) ) # include id column
            _colourTuple = self.__getStatusColour( status = status, subjob = bool( i.parent() ) )
            for column in range( columns ):
               i.setText( column, i.text( column ), _colourTuple )
            i.repaint()
            jobLVItemIterator += 1
         return

      # Normal update
      _cItem = self.currentItem()
      jid = None
      _entriesToDel = []
      for jid in jobStatusDict:
         if jid in self.indexDict:
            # Existing entry
            _item = self.indexDict[ jid ]
            if not jobStatusDict[ jid ]: # Test if diff entry is set to {}. 
#               log.debug( "setting %s to be removed from self.indexDict" % jid )
               _entriesToDel.append( jid )
               continue
            else:
               _c = jobStatusDict[ jid ][ 'content' ]
               _colourTuple = self.__getStatusColour( status = _c[ self.__statusPos ], subjob = bool( _item.parent() ) )
#               log.debug( "Updating job %s." % jid )
               for pos in range( len( _c ) ):
                  _item.setText( pos + 1, QString( _c[ pos ] ), _colourTuple )
               _cItem = _item
         else: # New entry
            # Possible problem. Diff entry requires non-existent j to be removed.
            if not jobStatusDict[ jid ]:
               continue
            # Insert new item.
            _c = jobStatusDict[ jid ][ 'content' ]
            _itemParent = _getParent( jid )
            _colourTuple = self.__getStatusColour( status = _c[ self.__statusPos ], subjob = _itemParent is not self )
            _cItem = JobLVItem( _itemParent, jid, _c, _colourTuple )
            self.indexDict[ jid ] = _cItem
#            log.debug( "Adding job %s." % jid )
         try:
#            log.debug( "Job %s: %s" % ( jid, jobStatusDict[ jid ][ 'meta' ] ) )
            _cItem.setExpandable( jobStatusDict[ jid ][ 'meta' ][ 'subjobs' ] )
         except:
            continue
      # Only root items should be deleted. This allows PyQt to take care
      # of releasing memory properly to avoid the situation when the 
      # underlying C++ object is missing when garbage collection is 
      # initiated automatically.
      rootItemsToDel = []
      for entry in _entriesToDel:
         _i = self.indexDict[ entry ]
         _p = _i.parent()
         if _p == None: # master jobs
            self.takeItem( _i )
            rootItemsToDel.append( entry )
         else: # subjobs
            _p.takeItem( _i )
            # As subjobs are child items, in the case where subjobs are
            # deleted but master jobs are not (e.g. collapse rather than
            # deletion of a master job), they are inserted at the list head
            # to ensure that the underlying C++ object is deleted in the
            # right order.
            rootItemsToDel.insert( 0, entry )
#      log.debug( "Items to delete: %s." % rootItemsToDel )
      for item in rootItemsToDel:
         del self.indexDict[ item ]
#      log.debug( "After deletion, current indexDict: %s." % self.indexDict )
      self.sort()


class FolderLVItem( LVItem ):
   image_FolderClosed = QPixmap()
   image_FolderClosed.loadFromData( image_FolderClosed_data, "PNG" )
   image_FolderOpen = QPixmap()
   image_FolderOpen.loadFromData( image_FolderOpen_data, "PNG" )
   image_Job = QPixmap()
   image_Job.loadFromData( image_Job_data, "PNG" )

   def __init__( self, parent, itemName, isJob = True ):
      self.isJob = isJob
      if self.isJob:
         self.pix = self.image_Job
      else:
         self.pix = self.image_FolderClosed
      LVItem.__init__( self, parent )
      self.setDropEnabled( not self.isJob )
      self.setText( 0, itemName )

   def fullPath( self ):
      s = str( self.text( 0 ) )
      parent = self.parent()
      while parent:
         s = os.path.join( str( parent.text( 0 ) ), s )
         parent = parent.parent()
      return s

   def compare( self, other, column, ascending ):
      s_type = type( self.key( column, ascending ) )
      o_type = type( other.key( column, ascending ) )
      if s_type != o_type:
         if s_type == int:
            return -1
         else:
            return 1
      else:
         if s_type == int:
            return self.key( column, ascending ) - other.key( column, ascending )
         else:
            return self.key( column, ascending ).compare( other.key( column, ascending ) )

   def key( self, column, ascending ):
      try:
         return int( str( self.text( column ) ) )
      except:
         return self.text( column )
   
   # Overiden methods to facilitate dynamic icon change
   def setup( self ):
      if not self.isJob:
         self.setExpandable( self.childCount() )
      LVItem.setup( self )
   
   def setOpen( self, o ):
      if not self.isJob:
         if o:
            self.setPixmap( self.image_FolderOpen )
         else:
            self.setPixmap( self.image_FolderClosed )
      LVItem.setOpen( self, o )
   
   def setPixmap( self, pm ):
      self.pix = pm
      self.setup()
      self.widthChanged( 0 )
      self.invalidateHeight()
      self.repaint()

   def pixmap( self, column = 0 ):
      if column:
         return None
      return self.pix
   

class LogicalFolders_ListView( SortedListView ):
   def __init__( self, parent = None, actionsDict = {} ):
      SortedListView.__init__( self, parent )
      self.actionsDict = actionsDict
      self.setSelectionMode( QListView.Single )
      self.setDefaultRenameAction( QListView.Accept )
      self.setAcceptDrops( True )
      self.viewport().setAcceptDrops( True )
      self.__dropItem = None
      self.__oldCurrent = None
      self.__currDragItem = None
      self.addColumn( "Folders" )
      self.__flatJobTree = {}
      self.__autoOpenTimer = QTimer()
      # Slots
      self.connect( self, 
                    SIGNAL( 'contextMenuRequested( QListViewItem*, const QPoint&, int )' ), 
                    self.slotContextMenuRequested )
      self.connect( self.__autoOpenTimer, SIGNAL( 'timeout()' ), self.slotOpenFolder )
      # Connections for logical folder context menu
      __guiParent = self.actionsDict[ '__PARENT__' ]
      self.connect( __guiParent, qt.PYSIGNAL( "folderAdd()" ), self.slotAddFolder )
      self.connect( __guiParent, qt.PYSIGNAL( "folderRemove()" ), self.slotRemoveFolder )
      self.connect( __guiParent, qt.PYSIGNAL( "folderRename()" ), self.slotRenameFolder )
      self.connect( __guiParent, qt.PYSIGNAL( "folderAddJob()" ), self.slotAddJobToFolder )
      self.connect( __guiParent, qt.PYSIGNAL( "folderRemoveJob()" ), self.slotRemoveJobFromFolder )

      self.slotUpdateFolders()
      self.firstChild().setOpen( bool( self.firstChild().childCount() ) )

   def slotOpenFolder( self ):
      self.__autoOpenTimer.stop()
      if self.__dropItem:
         self.__dropItem.setOpen( bool( self.__dropItem.childCount() ) )

   def slotContextMenuRequested( self, qItem, point, column ):
      self.contextMenu.clear()
      if qItem is None:
         return
      if qItem.isJob:
         self.actionsDict[ 'FolderRemoveJob' ].addTo( self.contextMenu )
      else:
         self.actionsDict[ 'FolderAdd' ].addTo( self.contextMenu )
         if qItem.parent() is not None: # root item
            self.actionsDict[ 'FolderRemove' ].addTo( self.contextMenu )
            self.actionsDict[ 'FolderRename' ].addTo( self.contextMenu )
         self.contextMenu.insertSeparator()
         self.actionsDict[ 'FolderAddJob' ].addTo( self.contextMenu )
      self.contextMenu.exec_loop( point )

   def slotRenameFolder( self ):
      item = self.selectedItem()
      if item is None or item.isJob or item.parent() is None:
         return
      oldPath = item.fullPath()
      newName, ok = QInputDialog.getText( 'Folder rename', 'Enter new folder name', QLineEdit.Normal, item.text( 0 ), self )
      newName = str( newName )
      if not ok or \
         not newName or \
         newName in inspector.jobtree.listdirs( item.parent().fullPath() ):
         return
      item.setText( 0, newName )
      inspector.jobtree.mkdir( os.path.join( os.path.dirname( item.parent().fullPath() ), newName ) )
      itemIterator = QListViewItemIterator( item )
      while itemIterator.current():
         i = itemIterator.current()
         try:
            if i.isJob:
               inspector.jobtree.add( inspector.jobs( int( str( i.text( 0 ) ) ) ), os.path.dirname( i.fullPath() ) )
            else:
               inspector.jobtree.mkdir( i.fullPath() )
         except: # Don't proceed to remove renamed item
            return
         itemIterator += 1
      inspector.jobtree.rm( oldPath )
      self.slotUpdateFolders()
   
   def slotAddFolder( self ):
      item = self.selectedItem()
      if item is None or item.isJob:
         return
      itemPath = item.fullPath()
      newName, ok = QInputDialog.getText( 'Add new folder', 'Enter new folder name', QLineEdit.Normal, '', self )
      newName = str( newName )
      if not ok or \
         not newName or \
         newName in inspector.jobtree.listdirs( itemPath ):
         return
      inspector.jobtree.mkdir( os.path.join( itemPath, newName ) )
      self.slotUpdateFolders()
      item.setOpen( bool( item.childCount() ) )

   def slotRemoveFolder( self ):
      item = self.selectedItem()
      if item is None or item.isJob or item.parent() is None:
         return
      parent = item.parent()
      inspector.jobtree.rm( item.fullPath() )
      self.slotUpdateFolders()
      parent.setOpen( bool( parent.childCount() ) )

   def slotAddJobToFolder( self ):
      item = self.selectedItem()
      if item is None or item.isJob:
         return
      itemfullPath = item.fullPath()
      jid_str, ok = QInputDialog.getItem( 'Add a job', 'Job id to add to %s:' % itemfullPath, QStringList.fromStrList( map( str, inspector.jobs.ids() ) ), 0, False, self )
      if not ok or \
         not jid_str or \
         int( str( jid_str ) ) in inspector.jobtree.listjobs( itemfullPath ):
         return
      inspector.jobtree.add( inspector.jobs( int( str( jid_str ) ) ), itemfullPath )
      self.slotUpdateFolders()
      item.setOpen( bool( item.childCount() ) )

   def slotRemoveJobFromFolder( self ):
      item = self.selectedItem()
      if item is None or not item.isJob:
         return
      parent = item.parent()
      inspector.jobtree.rm( item.fullPath() )
      self.slotUpdateFolders()
      parent.setOpen( bool( parent.childCount() ) )

   def focusInEvent( self, evt ):
      self.slotUpdateFolders()
      SortedListView.focusInEvent( self, evt )

   def __refreshFlatJobTreeCopy( self, root = '/' ):
      # Create a flat representation of the job tree with path
      # as the key to list of jobs.
      for directory in inspector.jobtree.listdirs( root ):
         self.__refreshFlatJobTreeCopy( os.path.join( root, directory ) )
      self.__flatJobTree[ root ] = inspector.jobtree.listjobs( root )
   
   def slotUpdateFolders( self ):
      # ---- First parse ---- 
      # Prune the current list view and remove folders and jobs
      # that do not exist in the job tree anymore.
#      log.debug( "Updating folders!" )
#      log.debug( "First parse...\n" )
      self.__flatJobTree.clear()
      inspector.jobtree.cleanlinks()
      self.__refreshFlatJobTreeCopy()
      self.iterator = QListViewItemIterator( self )
      takenItems = []
      while self.iterator.current():
         _i = self.iterator.current()
#         log.debug( "Working on %s of the listview now." % _i.fullPath() )
         if inspector.jobtree.exists( _i.fullPath() ): 
#            log.debug( "The flat job tree: %s" % self.__flatJobTree )
            if _i.isJob: # current list view item is a job
               _folderName, _jid = os.path.split( _i.fullPath() )
#               log.debug( "Removing %s from list of jobs in %s" % ( _jid, _folderName ) )
               self.__flatJobTree[ _folderName ].remove( int( _jid ) )
               # remove folder if it no longer has jobs
               if not self.__flatJobTree[ _folderName ]:
#                  log.debug( "...removing the empty folder %s." % _folderName )
                  del self.__flatJobTree[ _folderName ]
#               else:
#                  log.debug( "...the folder %s still has %s." % ( _folderName, self.__flatJobTree[ _folderName ] ) )
            else: # current list view item is a folder
#               log.debug( "Full path of folder: %s" % _i.fullPath() )
               if not self.__flatJobTree[ _i.fullPath() ]:  # current job tree folder is empty (of jobs).
#                  log.debug( "This entry in the flat job tree is empty, deleting" )
                  # remove from self.__flatJobTree to prevent re-insertion in second parse.
                  del self.__flatJobTree[ _i.fullPath() ]
#               else:
#                  log.debug( "This folder in the flat job tree is not empty." )
         else: # delete list view branch
#            log.debug( "Marking %s for deletion." % _i.fullPath() )
            # items are not 'taken' directly but noted down in takenItems
            # so as to allow the iterator to work correctly.
            # Possible bug in QListViewItemIterator.
            # takenItems is a list of ( parent item, child item ).
            _p = _i.parent()
            if _p is None:
               takenItems.append( ( self, _i ) )
            else:
               takenItems.append( ( _p, _i ) )
         self.iterator += 1
      
      # The taken items are reversed to avoid deleting parents before 
      # children as takenItems (prior to reversal) will have parent
      # items before children items.
      takenItems.reverse()
      while takenItems:
#         log.debug( "Deleting %s." % takenItems[0][1].fullPath() )
         takenItems[ 0 ][ 0 ].takeItem( takenItems[ 0 ][ 1 ] )
         del takenItems[ 0 ]
      
      # ---- Second parse ----
      # Take the remaining flatJobTreeImage and insert
      # new folders and jobs into the list view.
#      log.debug( "\n\n\nSecond parse...\n" )
      flatJobTree_keys = self.__flatJobTree.keys()
      flatJobTree_keys.sort()
      _root = self.firstChild()
      if not _root:
         _root = FolderLVItem( self, '/', False )
         _root.setDragEnabled( False )
         _root.setRenameEnabled( 0, False )
      for folder in flatJobTree_keys:
         _i = _root
         currentPath = '/'
#         log.debug( "Working on folder %s..." % folder )
         for f in folder.strip( '/' ).split( '/' ):
            if not f:
               continue
            currentPath = os.path.join( currentPath, f )
#            log.debug( "The current path is %s." % currentPath )
            _j = _i.firstChild()
#            log.debug( "The first child of %s is %s." % (_i, _j) )
            while _j:
               if str( _j.text( 0 ) ) == f:
#                  log.debug( "found string %s in %s." % (f, _j) )
                  break
               else:
#                  log.debug( "Searching next sibling for %s" % f )
                  _j = _j.nextSibling()
            if _j: # Existing folder found. Procede to next lower level.
#               log.debug( "The item matching %s has been found" % _j )
               _i = _j
               continue
            else: # New folder... creating...
               _i = FolderLVItem( _i, f, False )
#               log.debug( "Created new folder %s with %s as parent." % ( _i, _i.parent() ) )
         # Adding the jobs
         for jid in self.__flatJobTree[ folder ]:
            FolderLVItem( _i, str( jid ), True )
#            log.debug( "Added job %s to %s" % ( jid, currentPath ) )
      self.sort()

   def dragObject( self ):
#      log.debug( "drag initated" )
      currItem = self.currentItem()
      self.__currDragItem = currItem
      if not currItem:
         return None
      return QTextDrag( currItem.fullPath(), self.viewport() )

   def contentsDragEnterEvent( self, evt ):
      if not evt.source() or not QTextDrag.canDecode( evt ):
         evt.ignore()
         return
      self.__oldCurrent = self.currentItem()
      i = self.itemAt( self.contentsToViewport( evt.pos() ) )
      if i:
         self.__dropItem = i
         self.__autoOpenTimer.start( DEFAULT_FOLDER_AUTO_OPEN_TIME )

   def contentsDragMoveEvent( self, evt ):
      if not evt.source() or not QTextDrag.canDecode( evt ):
         evt.ignore()
         return
      i = self.itemAt( self.contentsToViewport( evt.pos() ) )
      if i:
         self.setSelected( i, True )
         evt.accept()
         if i != self.__dropItem:
            self.__autoOpenTimer.stop()
            self.__dropItem = i
            self.__autoOpenTimer.start( DEFAULT_FOLDER_AUTO_OPEN_TIME )
         action = evt.action()
         if action == QDropEvent.Move:
            evt.acceptAction()
      else:
         evt.ignore()
         self.__autoOpenTimer.stop()
         self.__dropItem = None

   def contentsDragLeaveEvent( self, evt ):
      self.__autoOpenTimer.stop()
      self.__dropItem = None
      self.setCurrentItem( self.__oldCurrent )
      self.setSelected( self.__oldCurrent, True )

   def contentsDropEvent( self, evt ):
      self.__autoOpenTimer.stop()
      if not evt.source() or not QTextDrag.canDecode( evt ):
         evt.ignore()
         return
      i = self.itemAt( self.contentsToViewport( evt.pos() ) )
      # Check where the item in question was dropped
      if not i or i.isJob:
         evt.ignore()
         return
      if isinstance( evt.source(), Jobs_ListView ): # drag initiated from monitor
#         log.debug( "From the monitoring panel!" )
         decodedText = QString()
         if not QTextDrag.decode( evt, decodedText ) or \
            int( str( decodedText ) ) in inspector.jobtree.listjobs( i.fullPath() ):
            evt.ignore()
#            log.debug( "Drop event ignored!" )
            return
         FolderLVItem( i, str( decodedText ), True )
         i.setOpen( bool( i.childCount() ) )
         # Update Ganga jobtree
         inspector.jobtree.add( inspector.jobs( int( str( decodedText ) ) ), i.fullPath() )
         evt.accept()
#         log.debug( "DropEvent accepted!" )
         return
      else: # drag initiated from within
         # Test for source of drag
         if isinstance( self.__currDragItem, FolderLVItem ) and \
            self.__currDragItem.parent() != i and \
            self.__currDragItem.fullPath() not in i.fullPath():
            decodedText = QString()
            if not QTextDrag.decode( evt, decodedText ):
               evt.ignore()
#               log.debug( "Drop event ignored!" )
               return
            action = evt.action()
            parent = self.__currDragItem.parent()
            # ignore if the dest folder already has an identical folder.
            if self.__currDragItem.isJob:
               if int( str( self.__currDragItem.text( 0 ) ) ) in inspector.jobtree.listjobs( self.__dropItem.fullPath() ):
                  evt.ignore()
#                  log.debug( "Drop event ignored!" )
                  return
            else: # ignore if the dest folder already has an identical job.
               if str( self.__currDragItem.text( 0 ) ) in inspector.jobtree.listdirs( self.__dropItem.fullPath() ):
                  evt.ignore()
#                  log.debug( "Drop event ignored!" )
                  return
            # Move action
            if action == QDropEvent.Move:
               oldPath = self.__currDragItem.fullPath()
               # take the selected branch
               parent.takeItem( self.__currDragItem )
               parent.setOpen( bool( parent.childCount() ) )
               # re-insert the selected branch at the new location
               self.__dropItem.insertItem( self.__currDragItem )
               self.__dropItem.setOpen( True )
               # Update Ganga jobtree
               newItemIterator = QListViewItemIterator( self.__currDragItem )
               while newItemIterator.current():
                  curr_NewItem = newItemIterator.current()
                  if curr_NewItem.isJob:
                     inspector.jobtree.add( inspector.jobs( int( str( curr_NewItem.text( 0 ) ) ) ), os.path.dirname( curr_NewItem.fullPath() ) )
                  else:
                     inspector.jobtree.mkdir( curr_NewItem.fullPath() )
                  newItemIterator += 1
               # Actual removal of the folder or job from the jobtree
               inspector.jobtree.rm( oldPath )
               evt.acceptAction()
            elif action == QDropEvent.Copy:
               FolderLVItem( i, str( self.__currDragItem.text( 0 ) ), True )
               # Update Ganga jobtree
               inspector.jobtree.add( inspector.jobs( int( str( self.__currDragItem.text( 0 ) ) ) ), i.fullPath() )
               evt.acceptAction()
            evt.accept()
            return
      evt.ignore()


class JobNavLVItem( LVItem ):
   def __init__( self, parent, gangaObj = None, objStr = None, hotSwappable = None, colourTuple = None ):
      LVItem.__init__( self, parent )
#      self.gangaObj = gangaObj
      if objStr is None:
         objStr = ''
      self.objStr = objStr
      if hotSwappable is None:
         hotSwappable = False
      self.hotSwappable = hotSwappable
      self.setDisplay( gangaObj )
      if colourTuple:
         self.fg, self.bg = colourTuple

   def setDisplay( self, gangaObj ):
      if self.hotSwappable:
         if isinstance( gangaObj, list ):
            _displayName = self.objStr.split( '.' )[ -1 ]
         elif gangaObj is None:
            _displayName = "%s (%s)" % ( self.objStr.split( '.' )[ -1 ], gangaObj )
         else:
            _displayName = "%s (%s)" % ( self.objStr.split( '.' )[ -1 ], gangaObj._impl._name )
      else:
         _displayName = self.objStr.split( '.' )[ -1 ]
      self.setText( 0, _displayName )

   def paintCell( self, p, cg, column, _width, _alignment ):
      _cg = QColorGroup( cg )
      old_fg = QColor( _cg.text() )
      old_bg = QColor( _cg.base() ) # background()
      if not self.fg:
         self.fg = old_fg
      if not self.bg:
         self.bg = old_bg
      _cg.setColor( QColorGroup.Text, self.fg )
      _cg.setColor( QColorGroup.Base, self.bg )
      LVItem.paintCell( self, p, _cg, column, _width, _alignment )
      _cg.setColor( QColorGroup.Text, old_fg )
      _cg.setColor( QColorGroup.Base, old_bg )


class JobNavListView( QListView ):
   def __init__( self, parent = None ): #, actionsDict = {} ):
      QListView.__init__( self, parent )
#      self.actionsDict = actionsDict
      self.attrDelAction = qt.QAction( self, "attrDelAction" )
      self.attrDelAction.setText( 'Delete' )
      self.attrDelAction.setToolTip( "Delete the current item")
      self.setShowSortIndicator( True )
      self.setRootIsDecorated( True )
      self.contextMenu = QPopupMenu( self )
      self.iterator = None
      self.addColumn( "Job attributes" )
      self.setSizePolicy(qt.QSizePolicy(qt.QSizePolicy.Expanding,qt.QSizePolicy.Expanding,0,0,self.sizePolicy().hasHeightForWidth()))
      self.setSorting( -1 )
      # Slots
      self.connect( self, 
                    SIGNAL( 'contextMenuRequested( QListViewItem*, const QPoint&, int )' ), 
                    self.slotContextMenuRequested )

   def slotContextMenuRequested( self, item, point, column ):
      if item is None:
         return
      if item.objStr[ -1 ] == ']':
         self.contextMenu.clear()
         self.attrDelAction.addTo( self.contextMenu )
         self.contextMenu.exec_loop( point )
