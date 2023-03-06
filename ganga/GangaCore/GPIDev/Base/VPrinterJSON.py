##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VPrinterJSON.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################
from GangaCore.GPIDev.Base.Proxy import isProxy, isType, runProxyMethod, stripProxy
from GangaCore.GPIDev.Base.Objects import GangaObject
from io import StringIO

from inspect import isclass

from GangaCore.Utility.logging import getLogger
import json

logger = getLogger()


def wrapStringInQuotes(s: str):
    return '"' + s + '"'


def quoteValue(value, interactive=False) -> str:
    """A quoting function. Used to get consistent formatting"""
    if isType(value, str):
        # If it's a string then use `repr` for the quoting
        if interactive is True:
            return str(value)
        else:
            return json.dumps(repr(value))
    if hasattr(value, "items"):
        # If it's a mapping like a dict then quote each key and value
        quoted_list = [
            "%s:%s" % (quoteValue(k, interactive), quoteValue(v, interactive))
            for k, v in value.items()
        ]
        string_of_list = "{" + ", ".join(quoted_list) + "}"
        return string_of_list
    try:
        # If it's an iterable like a list or a GangaList then quote each element
        quoted_list = [quoteValue(val, interactive) for val in value]
        string_of_list = "[" + ", ".join(quoted_list) + "]"
        return string_of_list
    except TypeError:
        # If it's not a string or iterable then just return it plain
        return json.dumps(value, default=str)


def indent(level):
    return " " * int((level) * 2)


def full_print_json(obj, out=None, interactive=False):
    """Print the full contents of a GPI object without abbreviation."""
    import sys

    if out is None:
        out = sys.stdout

    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList

    _obj = stripProxy(obj)

    # wrap everything in {"output" : ... }
    print("{\"output\" : ", file=out)

    if isType(_obj, GangaList):
        obj_len = len(_obj)
        if obj_len == 0:
            print("[]", end=" ", file=out)
        else:
            outString = "["
            outStringList = []
            for x in _obj:
                if isType(x, GangaObject):
                    sio = StringIO()
                    stripProxy(x).printTreeJSON(sio, interactive)
                    result = sio.getvalue()
                    # remove trailing whitespace and newlines
                    outStringList.append(result.rstrip())
                else:
                    # remove trailing whitespace and newlines
                    outStringList.append(str(x).rstrip())
            outString += ", ".join(outStringList)
            outString += "]"
            print(outString, end=" ", file=out)
    elif isProxy(obj) and isinstance(_obj, GangaObject):
        sio = StringIO()
        runProxyMethod(obj, "printTreeJSON", sio, interactive)
        print(sio.getvalue(), end=" ", file=out)
    else:
        print(wrapStringInQuotes(str(_obj)), end=" ", file=out)

    print("}", file=out)

class VPrinterJSON(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying properties to print (default ''):
    #            'all'            - print all properties
    #            'copyable'       - print only copyable properties
    #            any other string - print unhidden properties

    __slots__ = ("level", "nocomma", "selection", "out", "empty_body", "_interactive")

    def __init__(self, out=None, selection="", interactive=False):
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
            # print(self.indent(), node._schema.name, '(', file=self.out)
            print(
                '{ "type" : ',
                wrapStringInQuotes(node._schema.name),
                ',\n"attributes:" : {',
                file=self.out,
            )
        else:
            print("{", file=self.out)
        self.level += 1
        self.nocomma = 1
        self.empty_body = 1

    def nodeEnd(self, node):
        self.level -= 1

        if self.empty_body:
            print(self.indent(), "}\n}", end="", file=self.out, sep="")
            self.nocomma = 0
        else:
            if self.nocomma:
                print("}\n}", end="", file=self.out)
            else:
                print("\n", self.indent(), "}\n}", end="", file=self.out, sep="")

        if self.level == 0:
            print("\n", file=self.out)

    def showAttribute(self, node, name):
        visible = False
        if self.selection == "all":
            visible = True
        elif self.selection == "copyable":
            if node._schema.getItem(name)["copyable"]:
                if not node._schema.getItem(name)["hidden"]:
                    visible = True
        elif self.selection == "preparable":
            # the following relies on the assumption that we only ever call printTreeJSON on
            # a preparable application.
            if node._schema.getItem(name)["preparable"]:
                if not node._schema.getItem(name)["hidden"]:
                    visible = True
        else:
            if not node._schema.getItem(name)["hidden"]:
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
                    print(
                        self.indent(),
                        wrapStringInQuotes(name),
                        ":",
                        str(value),
                        end="",
                        file=self.out,
                    )
                else:
                    print(
                        self.indent(),
                        wrapStringInQuotes(name),
                        ":",
                        repr(value),
                        end="",
                        file=self.out,
                    )
            else:
                print(
                    self.indent(),
                    wrapStringInQuotes(name),
                    ":",
                    self.quote(value),
                    end="",
                    file=self.out,
                )

    def sharedAttribute(self, node, name, value, sequence):
        self.simpleAttribute(node, name, value, sequence)

    def acceptOptional(self, s):
        if s is None:
            print("null", end="", file=self.out)
        else:
            if isType(stripProxy(s), list):
                print(s, end="", file=self.out)
            elif hasattr(s, "accept"):
                stripProxy(s).accept(self)
            else:
                self.quote(s)

    def componentAttribute(self, node, name, subnode, sequence):
        if self.showAttribute(node, name):
            self.empty_body = 0
            self.comma()
            print(self.indent(), wrapStringInQuotes(name), ":", end="", file=self.out)
            # self.level+=1
            if sequence:
                print("[", end="", file=self.out)
                self.level += 1
                for s in subnode:
                    print(self.indent(), file=self.out)
                    self.acceptOptional(s)
                    print(",", end="", file=self.out)
                self.level -= 1
                print("]", end="", file=self.out)
            else:
                self.acceptOptional(subnode)
            # self.level-=1

    def quote(self, x):
        return quoteValue(x, self._interactive)
