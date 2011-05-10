#!/usr/bin/env python
#-*-python-*-

################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: ganga_setup.py,v 1.2 2009-03-16 12:53:52 moscicki Exp $
#
# Copyright (C) 2003-2007 The Ganga Project
#
# This file is part of Ganga.
#
# Ganga is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Ganga is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################
"""
Base utility to setup Ganga environment on local sites.
It assumes CERN AFS release directory structure (/afs/cern.ch/sw/ganga/) but it is not difficult 
to adapt it for other places on Terra.
"""

import os.path, sys, os

def usage(error=None):
        print >> sys.stderr, ''' synopsis: ganga_setup.py --version=VERSION --interactive --experiment=EXP cmd

        cmd: 
                csh, sh : print shell setup commands (to be used with eval or source), --experiment=EXP specifies how the setup is done (atlas, lhcb, generic)
                dir     : print the release directory only
                version : print the latest production release in the installation tree (default choice at interactive prompt)

        if VERSION is not specified or is specified as "last","latest" or "default" then the latest production version is assumed

        --interactive : enters the interactive mode to confirm the version
        --show-all    : in interactive mode show all versions including development releases (-alpha, -beta or -pre)
        
        '''
        if error:
                print
                print 'ERROR:',error
                sys.exit(2)

def main():
        import getopt

        # defaults if no command line arguments...
        prefixStr = '/afs/cern.ch/sw/ganga/install'
        versionStr = None
        interactive = False
        showAll = False
        experiment = 'generic'
        
        try:
                opts, args = getopt.getopt(sys.argv[1:], "", ["version=","interactive","show-all","experiment="])
        except getopt.error, x:
                usage("command line syntax error")

        for o,a in opts:
                if o == "--version":
                        versionStr = a

                if o == "--interactive":
                        interactive = True

                if o == "--show-all":
                        showAll = True          

                if o == '--experiment':
                        if not a in ['atlas','lhcb','generic']:
                                usage('unknown experiment specified!')
                        experiment = a

        if len(args) != 1:
                usage("specify exactly one command")

        class Cmds:
                pass

        cmds = ['csh','sh','dir','version']
        
        cmd = args[0]

        if not cmd in cmds:
                usage("Unrecognized command %s. Must be one of: %s"%(cmd,str(cmds)))

        publicVersions, allVersions = get_versions(prefixStr)
        defaultVersion = publicVersions[-1]
        
        if not versionStr or versionStr.lower() in ['last','latest','default']:
                versionStr = defaultVersion

        # in the interactive mode we can override the versionStr
        if interactive:
                selected = False
                print >> sys.stderr, 'Setting Ganga environment:'               
                for d in (showAll and allVersions or publicVersions):
                        print >> sys.stderr, ' ', d

                while not selected:
                        print >> sys.stderr, 'Enter your choice [q]quit, [%s] : '%versionStr,
                        try:
                                choice = raw_input()
                        except EOFError:
                                print
                                sys.exit(1)
                        if not choice:
                                # keep the versionStr as it is
                                selected = True
                        if choice.upper() == 'Q':
                                sys.exit(1)
                        if choice in allVersions:
                                versionStr = choice
                                selected = True

        # double-check the version number
        if versionStr not in allVersions:
            print >> sys.stderr, 'Cannot set Ganga environment. Release version [%s] is not available' % versionStr
            sys.exit(1)

        # execute the specified command
                
        if cmd == 'version':
                print versionStr
                sys.exit(0)

        if cmd == 'dir':
                print '%s/%s/%s' % (prefixStr,versionStr)
                sys.exit(0)


        # OK, we provide the shell environment 
        shellStr = cmd

        print >> sys.stderr
        print >> sys.stderr,'Setting up Ganga %s (%s,%s)' % (versionStr,shellStr,experiment)


        setupcmds = { 'lhcb' : { 'sh': """export PATH=%(PATH)s ; export GANGA_CONFIG_PATH=%(INSTALL_PREFIX)s/config/GangaLHCbRoot.ini:%(INSTALL_PREFIX)s/config/GangaLHCb.ini:GangaLHCb/LHCb.ini""",
                                                                'csh': """setenv PATH %(PATH)s ; setenv GANGA_CONFIG_PATH %(INSTALL_PREFIX)s/config/GangaLHCbRoot.ini:%(INSTALL_PREFIX)s/config/GangaLHCb.ini:GangaLHCb/LHCb.ini"""
                                        },
                                        'atlas' : { 'sh': """alias ganga=%(BIN_DIR)s/ganga ; export GANGA_CONFIG_PATH=%(INSTALL_PREFIX)s/config/GangaAtlas%(NEWER44)s.ini:GangaAtlas/Atlas.ini""",
                                                        'csh': """alias ganga %(BIN_DIR)s/ganga ; setenv GANGA_CONFIG_PATH %(INSTALL_PREFIX)s/config/GangaAtlas%(NEWER44)s.ini:GangaAtlas/Atlas.ini""" },
                                        'generic' : { 'sh':"""alias ganga=%(BIN_DIR)s/ganga""",
                                                                'csh':"""alias ganga %(BIN_DIR)s/ganga""" }
                                        }


        vars = {'PATH': prepend_search_path(os.environ['PATH'],'%s/%s/bin' % (prefixStr,versionStr),prefixStr),
                        'INSTALL_PREFIX' : prefixStr,
                        'BIN_DIR' : '%s/%s/bin' % (prefixStr,versionStr)}
        
        #temporary hack for ATLAS: set a different CERN specific GangaAtlas.ini files for versions newer than 4.4.0-beta1
        #to be removed when all users migrate to Ganga 4.4
        if _relcmp(versionStr,'4.4.0-beta1')<0:         
                vars['NEWER44']='' # use %(INSTALL_PREFIX)s/config/GangaAtlas.ini
        else:           
                vars['NEWER44']='-v44' # use %(INSTALL_PREFIX)s/config/GangaAtlas-v44.ini
                
        print setupcmds[experiment][shellStr]%vars
        
        # if versionStr contains hotfix: strip the hotfix part (use public release) and replace GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini by an absolute path to your hotfix configuration file

def prepend_search_path(p,dir,prefix):
        """ Prepend dir to path removing duplicates if necessery (also from previous versions) based on the prefix.
        """
        newpath = ""
        matched = 0
        for d in p.split(':'):
                if newpath: newpath += ':'
                if d.find(prefix) != -1:
                        newpath += dir
                        matched = 1
                else:
                        newpath += d
        if not matched:
                if newpath: newpath = dir+':'+newpath
                else:
                        newpath = dir
        return newpath

# Compare release versions helper functions
def _stoi(i):
        "Cast string to integer whenever is possible"
        try:
                return int(i)
        except ValueError:
                return i

def _cmpver(a, b):
        "Compare x.y.z versions numerically"    
        import re
        a = map(_stoi, re.findall("\d+|\w+", a))
        b = map(_stoi, re.findall("\d+|\w+", b))
        return cmp(a, b) # -1 if a<b, 0 if a=b, 1 if a>b

def _devvercmp(a,b):
        import re
        # try to remove dev versions strings: beta,alpha,-pre
        p = re.compile( '(beta|alpha|-pre)')
        try:
                a1 = int(p.sub('', a))
        except ValueError:
                a1 = a

        try:
                b1 = int(p.sub('', b))
        except ValueError:
                b1 = b

        return cmp(a1,b1)       

#excluded_releases = ['beta','alpha', 'pre']
#make 4.0.0 appear AFTER 4.0.0-beta5 : 4.0.0 > 4.0.0-beta5
def _relcmp(x,y):
        xt = x.split('-')
        yt = y.split('-')
        
        if xt[0] != yt[0]:
                return _cmpver(xt[0],yt[0])

        if len(xt)==1 and len(yt) == 1: return 0

        if len(xt)==1: return 1
        if len(yt)==1: return -1

        return _devvercmp(xt[1],yt[1])
        
def get_versions(prefixStr):
        """
        returns a tuple containing (public_releases, all_releases)
            public releases are all the versions not containing -beta,-alpha or -pre suffixes
        
        """
        import fnmatch,re
        all = os.listdir('%s'%(prefixStr,))
        all = [n for n in all if fnmatch.fnmatch(n, '?.?.?*')]
        all.sort(_relcmp)
        #hide alpha|pre|beta from public releases
        public = filter(lambda x: re.compile("(alpha|pre|beta)").search(x) is None, all)        
        defaultVer = public[-1]
        return public,all

if __name__ == "__main__":
        main()

def test():
        p = '/afs/cern.ch/sw/ganga/install/rh73_gcc32/2.0.3/bin:/afs/cern.ch/sw/Gaudi/www/ganga/sw/install/bin:/home/moscicki/bin:/home/moscicki/scripts:/usr/sue/bin:/usr/bin:/bin:/usr/bin/X11:/usr/local/bin:/usr/local/bin/X11:/cern/pro/bin:.'
        print prepend_search_path(p,'x','/afs/cern.ch/sw/ganga')
        
        p = 'c:d/x:/afs/cern.ch/sw/ganga/install/rh73_gcc32/2.0.3/bin:/afs/cern.ch/sw/Gaudi/www/ganga/sw/install/bin:/home/moscicki/bin:/home/moscicki/scripts:/usr/sue/bin:/usr/bin:/bin:/usr/bin/X11:/usr/local/bin:/usr/local/bin/X11:/cern/pro/bin:.'
        print prepend_search_path(p,'x','/afs/cern.ch/sw/ganga')

        print prepend_search_path('','x','/afs/cern.ch/sw/ganga')
        print prepend_search_path('a:b:c','x','/afs/cern.ch/sw/ganga')    
