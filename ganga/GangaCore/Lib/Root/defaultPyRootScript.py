#!/usr/bin/env python

class Main(object):
    def run(self):
        '''Prints out some PyRoot debug info.'''
        print('Hello from PyRoot. Importing ROOT...')
        import ROOT
        print('Root Load Path', ROOT.gSystem.GetDynamicPath())

        from os.path import pathsep
        import string
        import sys

        print('Python Load Path', string.join([str(s) for s in sys.path],pathsep))

        print('Loading libTree:', ROOT.gSystem.Load('libTree'))

        print('Goodbye...')

if __name__ == '__main__':

    m = Main()
    m.run()

    import sys
    sys.exit(0)

