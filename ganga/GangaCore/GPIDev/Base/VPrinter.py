
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VPrinter.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################
from GangaCore.GPIDev.Base.Proxy import isProxy, isType, runProxyMethod, stripProxy
from GangaCore.GPIDev.Base.Objects import GangaObject
from io import StringIO

from inspect import isclass

from GangaCore.Utility.logging import getLogger

logger = getLogger()


def quoteValue(value, interactive=False):
    """A quoting function. Used to get consistent formatting"""
    if isType(value, str):
        # If it's a string then use `repr` for the quoting
        if interactive is True:
            return str(value)
        else:
            return repr(value)
    if hasattr(value, "items"):
        # If it's a mapping like a dict then quote each key and value
        quoted_list = ["%s:%s"%(quoteValue(k, interactive), quoteValue(v, interactive)) for k,v in value.items()]
        string_of_list = '{' + ', '.join(quoted_list) + '}'
        return string_of_list
    try:
        # If it's an iterable like a list or a GangaList then quote each element
        quoted_list = [quoteValue(val, interactive) for val in value]
        string_of_list = '[' + ', '.join(quoted_list) + ']'
        return string_of_list
    except TypeError:
        # If it's not a string or iterable then just return it plain
        return value


def indent(level):
    return ' ' * int((level) * 2)


# A visitor to print the object tree.


class VPrinter(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying properties to print (default ''):
    #            'all'            - print all properties
    #            'copyable'       - print only copyable properties
    #            any other string - print unhidden properties

    __slots__ = ('level', 'nocomma', 'selection', 'out', 'empty_body', '_interactive')

    def __init__(self, out=None, selection='', interactive=False):
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
        self._interactive = interactive

    def indent(self):
        return indent(self.level)

    def comma(self, force=0):
        if not self.nocomma or force:
            print(",", file=self.out)

        self.nocomma = 0

    def nodeBegin(self, node):
        if node._schema is not None:
            print(self.indent(), node._schema.name, '(', file=self.out)
        else:
            print('(', file=self.out)
        self.level+=1
        self.nocomma = 1
        self.empty_body = 1

    def nodeEnd(self, node):

        self.level-=1

        if self.empty_body:
            print(self.indent(), ' )', end='', file=self.out, sep='')
            self.nocomma = 0
        else:
            if self.nocomma:
                print(')', end='', file=self.out)
            else:
                print('\n', self.indent(), ' )', end='', file=self.out, sep='')

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
            # the following relies on the assumption that we only ever call printTree on
            # a preparable application.
            if node._schema.getItem(name)['preparable']:
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
            if isclass(value):
                if self._interactive is True:
                    print(self.indent(), name, '=', str(value), end = '', file=self.out)
                else:
                    print(self.indent(), name, '=', repr(value), end = '', file=self.out)
            else:
                print(self.indent(), name, '=', self.quote(value), end='', file=self.out)

    def sharedAttribute(self, node, name, value, sequence):
        self.simpleAttribute(node, name, value, sequence)

    def acceptOptional(self, s):
        if s is None:
            print(None, end='', file=self.out)
        else:
            if isType(stripProxy(s), list):
                print(s, end='', file=self.out)
            elif hasattr(s, 'accept'):
                stripProxy(s).accept(self)
            else:
                self.quote(s)

    def componentAttribute(self, node, name, subnode, sequence):
        if self.showAttribute(node, name):
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', end='', file=self.out)
            #self.level+=1
            if sequence:
                print('[', end='', file=self.out)
                self.level+=1
                for s in subnode:
                    print(self.indent(), file=self.out)
                    self.acceptOptional(s)
                    print(',', end='', file=self.out)
                self.level-=1
                print(']', end='', file=self.out)
            else:
                self.acceptOptional(subnode)
            #self.level-=1

    def quote(self, x):
        return quoteValue(x, self._interactive)


class VSummaryPrinter(VPrinter):

    """A class for printing summeries of object properties in a customisable way."""

    def __init__(self, level, verbosity_level, whitespace_marker, out=None, selection='', interactive=False):
        super(VSummaryPrinter, self).__init__(out, selection)
        self.level = level
        self.verbosity_level = verbosity_level
        self.whitespace_marker = whitespace_marker
        self._interactive = interactive

    def _CallSummaryPrintMember(self, node, name, value):
        """Checks to see whether there is a summary_print function pointer
        available in the schema object. If so it uses it and returns True
        otherwise it returns False.
        """
        function_pointer_available = False

        # check whether a print_summary function has been defined
        print_summary = node._schema.getItem(name)['summary_print']
        if print_summary is not None:
            fp = getattr(node, print_summary)
            str_val = fp(value, self.verbosity_level, self._interactive)
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', self.quote(str_val), end=' ', file=self.out)
            function_pointer_available = True
        return function_pointer_available

    def _CallPrintSummaryTree(self, obj):
        sio = StringIO()
        if not hasattr(stripProxy(obj), 'printSummaryTree'):
            print("%s" % str(obj), file=self.out)
        else:
            runProxyMethod(obj, 'printSummaryTree', self.level, self.verbosity_level, self.indent(), sio, self.selection, self._interactive)
        result = sio.getvalue()
        if result.endswith('\n'):
            result = result[0:-1]
        print(result, end=' ', file=self.out)

    def simpleAttribute(self, node, name, value, sequence):
        """Overrides the baseclass method. Tries to print a summary of the attribute."""
        if not self.showAttribute(node, name):
            return
        if self._CallSummaryPrintMember(node, name, getattr(node, name)):
            return

        if sequence:
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', end=' ', file=self.out)
            self._CallPrintSummaryTree(value)
            return

        # just go back to default behaviour
        super(VSummaryPrinter, self).simpleAttribute(
            node, name, value, sequence)

    def sharedAttribute(self, node, name, value, sequence):
        """Overrides the baseclass method. Tries to print a summary of the attribute."""
        if not self.showAttribute(node, name):
            return
        if self._CallSummaryPrintMember(node, name, getattr(node, name)):
            return

        if sequence:
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', end=' ', file=self.out)
            self._CallPrintSummaryTree(value)
            return

        # just go back to default behaviour
        super(VSummaryPrinter, self).sharedAttribute(node, name, value, sequence)

    def componentAttribute(self, node, name, subnode, sequence):
        if not self.showAttribute(node, name):
            return
        if self._CallSummaryPrintMember(node, name, subnode):
            return
        if isType(subnode, GangaObject):
            self.empty_body = 0
            self.comma()
            print(self.indent(), name, '=', end=' ', file=self.out)
            self._CallPrintSummaryTree(subnode)
            return

        # just go back to default behaviour
        super(VSummaryPrinter, self).componentAttribute(node, name, subnode, sequence)


def full_print(obj, out=None, interactive=False):
    """Print the full contents of a GPI object without abbreviation."""
    import sys
    if out is None:
        out = sys.stdout

    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList

    _obj = stripProxy(obj)

    if isType(_obj, GangaList):
        obj_len = len(_obj)
        if obj_len == 0:
            print('[]', end=' ', file=out)
        else:
            outString = '['
            outStringList = []
            for x in _obj:
                if isType(x, GangaObject):
                    sio = StringIO()
                    stripProxy(x).printTree(sio, interactive)
                    result = sio.getvalue()
                    # remove trailing whitespace and newlines
                    outStringList.append(result.rstrip())
                else:
                    # remove trailing whitespace and newlines
                    outStringList.append(str(x).rstrip())
            outString += ', '.join(outStringList)
            outString += ']'
            print(outString, end=' ', file=out)
        return

    if isProxy(obj) and isinstance(_obj, GangaObject):
        sio = StringIO()
        runProxyMethod(obj, 'printTree', sio, interactive)
        print(sio.getvalue(), end=' ', file=out)
    else:
        print(str(_obj), end=' ', file=out)


def summary_print(obj, out=None, interactive=False):
    """Print the summary contents of a GPI object with abbreviation."""
    import sys
    if out is None:
        out = sys.stdout

    _obj = stripProxy(obj)

    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
    if isType(_obj, GangaList):
        obj_len = len(_obj)
        if obj_len == 0:
            print('[]', end=' ', file=out)
        else:
            outString = '['
            outStringList = []
            for x in obj:
                if isType(x, GangaObject):
                    sio =StringIO()
                    stripProxy(x).printSummaryTree(0, 0, '', out=sio)
                    result = sio.getvalue()
                    # remove trailing whitespace and newlines
                    outStringList.append(result.rstrip())
                else:
                    # remove trailing whitespace and newlines
                    outStringList.append(str(x).rstrip())
            outString += ', '.join(outStringList)
            outString += ']'
            print(outString, end=' ', file=out)
        return

    if isProxy(obj) and isinstance(_obj, GangaObject):
        sio = StringIO()
        runProxyMethod(obj, 'printSummaryTree', 0, 0, '', sio, interactive)
        print(sio.getvalue(), end=' ', file=out)
    else:
        print(str(_obj), end=' ', file=out)

