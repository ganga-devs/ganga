# Compatibility.java
# 
# Copyright (C) 2010 Hurng-Chun Lee <hurngchunlee@gmail.com>
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

## this module includes all methods using version-dep. python modules

import sys

def get_python_version():
    return sys.version_info

def get_md5_obj():
    m = None
    if get_python_version() < (2,5):
        import md5
        m = md5.new()
    else:
        import hashlib
        m = hashlib.md5()

    return m
