# Compatibility.py: this module includes all methods using version-dep.
# python modules

import sys


def get_python_version():
    return sys.version_info


def get_md5_obj():
    m = None
    if get_python_version() < (2, 5):
        import md5
        m = md5.new()
    else:
        import hashlib
        m = hashlib.md5()

    return m
