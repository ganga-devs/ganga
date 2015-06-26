from __future__ import absolute_import
from .Repository import Repository, version


def factory(dir):
    return Repository(dir)
