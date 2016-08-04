from __future__ import print_function
from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VPrinterOld.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################
from Ganga.GPIDev.Base.Proxy import isProxy, isType, runProxyMethod


def quoteValue(value, selection):
    """A quoting function. Used to get consistent formatting"""
    # print "Quoting",repr(value),selection
    if isType(value, type('')):
        if selection == 'copyable':

            value = value.replace('"', R'\"')
            value = value.replace("'", R"\'")

# DISABLED
##             valueList = list( value )
# for i in range( len( value ) ):
##                 c = value[ i ]
# if c in [ "'", '"' ]:
##                     valueList[ i ] = "\\" + c
##                     value = "".join( valueList )
            if 1 + value.find("\n"):
                # print 'Quote result',"'''" + value + "'''"
                return "'''" + value + "'''"
        # print 'Quote result',"'"+value+"'"
        return "'" + value + "'"
    # print 'Quote result',value
    return value


def indent(level):
    return ' ' * int((level - 1) * 3)


# A visitor to print the object tree.


class VPrinterOld(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying properties to print (default ''):
    #            'all'            - print all properties
    #            'copyable'       - print only copyable properties
    #            any other string - print unhidden properties

    def __init__(self, out=None, selection=''):
        self.level = 0
        self.nocomma = 1
        self.selection = selection
        if out:
            self.out = out
        else:
            import sys
            self.out = sys.stdout

        # detect whether the body is empty to handle the comma correctly in
        # this case too
        self.empty_body = 0

    def indent(self):
        return indent(self.level)

    def comma(self, force=0):
        if not self.nocomma or force:
            print(",", file=self.out)

        self.nocomma = 0

    def nodeBegin(self, node):
        self.level += 1
        if node._schema is not None:
            print(node._schema.name, '(', file=self.out)
        else:
            print('(', file=self.out)
        self.nocomma = 1
        self.empty_body = 1

    def nodeEnd(self, node):

        if self.empty_body:
            print(self.indent(), ' )', end=' ', file=self.out, sep='')
            self.nocomma = 0
        else:
            if self.nocomma:
                print(')', end=' ', file=self.out)
            else:
                print('\n', self.indent(), ' )', end=' ', file=self.out, sep='')

        self.level -= 1
        if self.level == 0:
            print('\n', file=self.out)

    def showAttribute(self, node, name):
        visible = False
        if self.selection == 'all':
            visible = True
        elif self.selection == 'copyable':
            if node._schema.getItem(name)['copyable']:
                if not node._schema.getItem(name)['hidden']:
                    visible = True
        elif self.selection == 'preparable':
            # the following relies on the assumption that we only ever call printPrepTree on
            # a preparable application.
            if node._schema.getItem(name)['preparable'] or self.level == 2:
                if not node._schema.getItem(name)['hidden']:
                    visible = True
        else:
            if not node._schema.getItem(name)['hidden']:
                visible = True
        return visible

    def simpleAttribute(self, node, name, value, sequence):
        if self.showAttribute(node, name):
            self.empty_body = 0
            self.comma()
            # DISABLED
            # print '*'*20,name
            # if sequence:
            #    print 'transformation:',repr(value)
            #    value = value.toString()
            #    print 'into',repr(value)
            # else:
            #    print 'no transformation'
            print(self.indent(), name, '=', self.quote(value), end=' ', file=self.out)

    def sharedAttribute(self, node, name, value, sequence):
        self.simpleAttribute(node, name, value, sequence)

    def acceptOptional(self, s):
        if s is None:
            print(None, end=' ', file=self.out)
        else:
            runProxyMethod(s, 'accept', self)

    def componentAttribute(self, node, name, subnode, sequence):
        if self.showAttribute(node, name):
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', end=' ', file=self.out)
            if sequence:
                print('[', end=' ', file=self.out)
                for s in subnode:
                    self.acceptOptional(s)
                    print(',', end=' ', file=self.out)
                print(']', end=' ', file=self.out)
            else:
                self.acceptOptional(subnode)

    def quote(self, x):
        return quoteValue(x, self.selection)


