import re
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName

from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef

# config_scope is namespace used for evaluating simple objects (e.g. File, datetime, SharedDir)
from GangaCore.Utility.Config import config_scope

from GangaCore.Utility.Plugin import PluginManagerError, allPlugins

from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Lib.GangaList.GangaList import makeGangaList

from .GangaRepository import SchemaVersionError

import xml.sax.saxutils
import copy
from io import StringIO

logger = getLogger()

_cached_eval_strings = {}

##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VStreamer.py,v 1.1.2.2 2009-07-14 09:20:22 ebke Exp $
##########################################################################

# dump object (job) to file f (or stdout) while ignoring the attribute
# 'ignore_subs'


class XMLFileError(GangaException):

    def __init__(self, excpt, message):
        GangaException.__init__(self, excpt, message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            err = '(%s:%s)' % (type(self.excpt), self.excpt)
        else:
            err = ''
        return "XMLFileError: %s %s" % (self.message, err)

def _raw_to_file(j, fobj=None, ignore_subs=[]):
    sio = StringIO()
    vstreamer = VStreamer(out=sio, selection=ignore_subs)
    vstreamer.begin_root()
    j.accept(vstreamer)
    vstreamer.end_root()
    print(sio.getvalue(), file=fobj)

def to_file(j, fobj=None, ignore_subs=[]):
    #used to debug write problems - rcurrie
    #_raw_to_file(j, fobj, ignore_subs)
    #return
    _ignore_subs = [ignore_subs] if not isinstance(ignore_subs, list) else ignore_subs
    try:
        _raw_to_file(j, fobj, _ignore_subs)
    except Exception as err:
        logger.error("XML to-file error for file:\n%s" % (err))
        raise XMLFileError(err, "to-file error")

# Faster, but experimental version of to_file without accept()
# def to_file(j,f=None,ignore_subs=''):
#    sl = ["<root>"]
#    sl.extend(fastXML(j,'   ',ignore_subs=ignore_subs))
#    sl.append("</root>\n")
#    f.write("".join(sl))

# load object (job) from file f
# if len(errors) > 0 the object was not loaded correctly.
# Typical exceptions are:
# * SchemaVersionError (incompatible schema version)
# * PluginManagerError (necessary plugin not loaded)
# * IOError (problem on file reading)
# * AssertionError (corruption: multiple objects in <root>...</root>
# * Exception (probably corrupted data problem)

def _raw_from_file(f):
    # logger.debug('----------------------------')
    ###logger.debug('Parsing file: %s',f.name)
    xml_content = f.read()
    obj, errors = Loader().parse(xml_content)
    return obj, errors

def from_file(f):
    #return _raw_from_file(f)
    try:
        return _raw_from_file(f)
    except Exception as err:
        logger.error("XML from-file error for file:\n%s" % err)
        raise XMLFileError(err, "from-file error")

##########################################################################
# utilities


def escape(s):
    # s.replace('"', '\\"').replace("'", "\\'"))
    return xml.sax.saxutils.escape(s)


def unescape(s):
    return xml.sax.saxutils.unescape(s)

# An experimental, fast way to print a tree of Ganga Objects to file
# Unused at the moment


def fastXML(obj, indent='', ignore_subs=''):
    if hasattr(obj, "__iter__") and not hasattr(obj, "iteritems"):
        sl = ["\n", indent, "<sequence>", "\n"]
        for so in obj:
            sl.append(indent)
            sl.extend(fastXML(so, indent + ' ', ignore_subs))
        sl.append(indent)
        sl.append("</sequence>")
        return sl
    elif hasattr(obj, '_data'):
        v = obj._schema.version
        sl = ['\n', indent, '<class name="%s" version="%i.%i" category="%s">\n' % (getName(obj), v.major, v.minor, obj._category)]
        for attr_name in obj._schema.allItemNames():
            k = attr_name
            o = getattr(obj, k)
            if k != ignore_subs:
                try:
                    if not obj._schema[k]._meta["transient"]:
                        sl.append(indent)
                        sl.append('<attribute name="%s">' % k)
                        sl.extend(fastXML(o, indent + '  ', ignore_subs))
                        sl.append('</attribute>\n')
                except KeyError as err:
                    logger.debug("KeyError: %s" % err)
                    pass
        sl.append(indent)
        sl.append('</class>')
        return sl
    else:
        return ["<value>", escape(repr(obj)), "</value>"]

##########################################################################
# A visitor to print the object tree into XML.
#


class VStreamer(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying the name of properties which should not be printed
    # e.g. 'subjobs' - will not print subjobs

    def __init__(self, out=None, selection=[]):
        self.level = 0
        self.selection = selection
        if out is not None:
            self.out = out
        else:
            import sys
            self.out = sys.stdout

    def begin_root(self):
        print('<root>', file=self.out)

    def end_root(self):
        print('</root>', file=self.out)

    def indent(self):
        return ' ' * (self.level - 1) * 3

    def nodeBegin(self, node):
        self.level += 1
        s = node._schema
        print(self.indent(), '<class name="%s" version="%d.%d" category="%s">' % (s.name, s.version.major, s.version.minor, s.category), file=self.out)

    def nodeEnd(self, node):
        print(self.indent(), '</class>', file=self.out)
        self.level -= 1
        return

    def print_value(self, x):
        print('\n', self.indent(), '<value>%s</value>' % escape(repr(x)), file=self.out)

    def showAttribute(self, node, name):
        return (self.level > 1 or name not in self.selection) and not node._schema.getItem(name)['transient']

    def simpleAttribute(self, node, name, value, sequence):
        if self.showAttribute(node, name):
            self.level += 1
            print(self.indent(), end=' ', file=self.out)
            print('<attribute name="%s">' % name, end=' ', file=self.out)
            if sequence:
                self.level += 1
                print(file=self.out)
                print(self.indent(), '<sequence>', file=self.out)
                for v in value:
                    self.acceptOptional(v)
                print(self.indent(), '</sequence>', file=self.out)
                self.level -= 1
                print(self.indent(), '</attribute>', file=self.out)
            else:
                self.level += 1
                if isinstance(value, GangaObject):
                    print("", file=self.out)
                    self.acceptOptional(value)
                else:
                    self.print_value(value)
                self.level -= 1
                print(self.indent(), end=' ', file=self.out)
                print('</attribute>', file=self.out)
            self.level -= 1

    def sharedAttribute(self, node, name, value, sequence):
        self.simpleAttribute(node, name, value, sequence)

    def acceptOptional(self, s):
        self.level += 1
        if s is None:
            print(self.indent(), '<value>None</value>', file=self.out)
        else:
            if isType(s, str):
                print(self.indent(), '<value>%s</value>' % escape(repr(s)), file=self.out)
            elif hasattr(s, 'accept'):
                s.accept(self)
            elif isType(s, (list, tuple, GangaList)):
                print(self.indent(), '<sequence>', file=self.out)
                for sub_s in s:
                    self.acceptOptional(sub_s)
                print(self.indent(), '</sequence>', file=self.out)
            else:
                self.print_value(s)
        self.level -= 1

    def componentAttribute(self, node, name, subnode, sequence):
        if self.showAttribute(node, name):
            self.level += 1
            print(self.indent(), '<attribute name="%s">' % name, file=self.out)
            if sequence:
                self.level += 1
                print(self.indent(), '<sequence>', file=self.out)
                for s in subnode:
                    self.acceptOptional(s)
                print(self.indent(), '</sequence>', file=self.out)
                self.level -= 1
            else:
                self.acceptOptional(subnode)
            print(self.indent(), '</attribute>', file=self.out)
            self.level -= 1


##########################################################################
# XML Parser.

# Empty Ganga Object


class EmptyGangaObject(GangaObject):

    """Empty Ganga Object. Is used to construct incomplete jobs"""
    _schema = Schema(Version(0, 0), {})
    _name = "EmptyGangaObject"
    _category = "internal"
    _hidden = 1

    def __init__(self):
        super(EmptyGangaObject, self).__init__()


class Loader(object):

    """ Job object tree loader.
    """

    def __init__(self):
        self.stack = None  # we construct object tree using this stack
        # ignore nested XML elements in case of data errors at a higher level
        self.ignore_count = 0
        self.errors = []  # list of exception objects in case of data errors
        # buffer for <value> elements (evaled as python expressions)
        self.value_construct = None
        # buffer for building sequences (FIXME: what about nested sequences?)
        self.sequence_start = []

    def parse(self, s):
        """ Parse and load object from string s using internal XML parser (expat).
        """
        import xml.parsers.expat

        # 3 handler functions
        def start_element(name, attrs):
            #logger.debug('Start element: name=%s attrs=%s', name, attrs) #FIXME: for 2.4 use CurrentColumnNumber and CurrentLineNumber
            # if higher level element had error, ignore the corresponding part
            # of the XML tree as we go down
            if self.ignore_count:
                self.ignore_count += 1
                return

            # initialize object stack
            if name == 'root':
                assert self.stack is None, "duplicated <root> element"
                self.stack = []
                return

            assert not self.stack is None, "missing <root> element"

            # load a class, make empty object and push it as the current object
            # on the stack
            if name == 'class':
                try:
                    cls = allPlugins.find(attrs['category'], attrs['name'])
                except PluginManagerError as e:
                    self.errors.append(e)
                    #self.errors.append('Unknown class: %(name)s'%attrs)
                    obj = EmptyGangaObject()
                    # ignore all elemenents until the corresponding ending
                    # element (</class>) is reached
                    self.ignore_count = 1
                else:
                    version = Version(*[int(v) for v in attrs['version'].split('.')])
                    if not cls._schema.version.isCompatible(version):
                        attrs['currversion'] = '%s.%s' % (cls._schema.version.major, cls._schema.version.minor)
                        self.errors.append(SchemaVersionError('Incompatible schema of %(name)s, repository is %(version)s currently in use is %(currversion)s' % attrs))
                        obj = EmptyGangaObject()
                        # ignore all elemenents until the corresponding ending
                        # element (</class>) is reached
                        self.ignore_count = 1
                    else:
                        # Initialize and cache a c class instance to use as a classs factory
                        obj = cls.getNew()
                self.stack.append(obj)

            # push the attribute name on the stack
            if name == 'attribute':
                self.stack.append(attrs['name'])

            # start value_contruct mode and initialize the value buffer
            if name == 'value':
                self.value_construct = ''

            # save a marker where the sequence begins on the stack
            if name == 'sequence':
                self.sequence_start.append(len(self.stack))

        def end_element(name):
            #logger.debug('End element: name=%s', name)

            # if higher level element had error, ignore the corresponding part
            # of the XML tree as we go up
            if self.ignore_count:
                self.ignore_count -= 1
                return

            # when </attribute> is seen the current object, attribute name and
            # value should be on top of the stack
            if name == 'attribute':
                value = self.stack.pop()
                aname = self.stack.pop()
                obj = self.stack[-1]
                # update the object's attribute
                try:
                    obj.setSchemaAttribute(aname, value)
                except:
                    raise GangaException("ERROR in loading XML, failed to set attribute %s for class %s" % (aname, _getName(obj)))
                #logger.info("Setting: %s = %s" % (aname, value))

            # when </value> is seen the value_construct buffer (CDATA) should
            # be a python expression (e.g. quoted string)
            if name == 'value':
                try:
                    # unescape the special characters
                    s = unescape(self.value_construct)
                    s = re.sub(r'(\d)L(\})', r'\1\2', s)
                    if s not in _cached_eval_strings:
                        # This is ugly and classes which use this are bad, but this needs to be fixed in another PR
                        # TODO Make the scope of objects a lot better than whatever is in the config
                        # This is a dictionary constructed from eval-ing things in the Config. Why does should it do this?
                        # Anyway, lets save the result for speed
                        _cached_eval_strings[s] = eval(s, config_scope)
                    eval_str = _cached_eval_strings[s]
                    if not isinstance(eval_str, str):
                        val = copy.deepcopy(eval_str)
                    else:
                        val = eval_str
                    #logger.debug('evaled value: %s type=%s',repr(val),type(val))
                    self.stack.append(val)
                    self.value_construct = None
                except:
                    raise GangaException("ERROR in loading XML, failed to correctly parse attribute value: \'%s\'" % str(self.value_construct))

            # when </sequence> is seen we remove last items from stack (as indicated by sequence_start)
            # we make a GangaList from these items and put it on stack
            if name == 'sequence':
                pos = self.sequence_start.pop()
                try:
                    alist = makeGangaList(self.stack[pos:])
                except:
                    raise GangaException("ERROR in loading XML, failed to construct a sequence(list) properly")
                del self.stack[pos:]
                self.stack.append(alist)

            # when </class> is seen we finish initializing the new object
            # by setting remaining attributes to their default values
            # the object stays on the stack (will be removed by </attribute> or
            # is a root object)
            if name == 'class':
                obj = self.stack[-1]
                cls = obj.__class__
                if isinstance(cls, GangaObject):
                    for attr, item in cls._schema.allItems():
                        if attr not in obj._data:
                            if item.getProperties()['getter'] is None:
                                try:
                                    setattr(obj, attr, self._schema.getDefaultValue(attr))
                                except:
                                    raise GangaException("ERROR in loading XML, failed to set default attribute %s for class %s" % (attr, _getName(obj)))
                pass

        def char_data(data):
            # char_data may be called many times in one CDATA section so we need to build up
            # the full buffer for <value>CDATA</value> section incrementally
            if self.value_construct is not None:
                ###logger.debug('char_data: append=%s',data)
                # FIXME: decode data
                self.value_construct += data

        # start parsing using callbacks
        p = xml.parsers.expat.ParserCreate()

        p.StartElementHandler = start_element
        p.EndElementHandler = end_element
        p.CharacterDataHandler = char_data

        p.Parse(s)

        if len(self.stack) != 1:
            self.errors.append(AssertionError('multiple objects inside <root> element'))

        obj = self.stack[-1]

        #logger.info("obj.__dict__: %s" % obj.__dict__)

        # Raise Exception if object is incomplete
        for attr, item in obj._schema.allItems():
            if not hasattr(obj, attr):
                raise AssertionError("incomplete XML file")
        return obj, self.errors

