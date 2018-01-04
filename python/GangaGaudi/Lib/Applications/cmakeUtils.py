
import GangaCore.Core.exceptions
import os


def get_user_platform(env=os.environ):
    '''
    Doesn't appear to be used within Ganga!
    This will likely still extract the code as in the CMT package using env['CMTCONFIG']
    '''
    raise NotImplementedError


def update_project_path(user_release_area, env=os.environ):
    '''
    Doesn't appear to be used within Ganga!
    '''
    return NotImplementedError


def get_user_dlls(appname, version, user_release_area, platform, env):
    '''

    '''
    return NotImplementedError


def make(self, argument=''):
    """Build the code in the release area the application object points to."""

    return NotImplementedError
