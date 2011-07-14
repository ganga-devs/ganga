################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VStreamer.py,v 1.5 2009-06-09 13:03:16 moscicki Exp $
################################################################################

# dump object (job) to file f (or stdout)
def to_file(j,f=None):
    vstreamer = VStreamer(out=f,selection='subjobs')#FIXME: hardcoded subjobs handling
    vstreamer.begin_root()
    j.accept(vstreamer)
    vstreamer.end_root()

# load object (job) from file f
def from_file(f):
    ###logger.debug('----------------------------')
    ###logger.debug('Parsing file: %s',f.name)
    obj,errors = Loader().parse(f.read())
    return obj,errors

################################################################################
# utilities

import xml.sax.saxutils
#import escape, unescape

def escape(s):
    return xml.sax.saxutils.escape(s) #s.replace('"', '\\"').replace("'", "\\'"))

def unescape(s):
    return xml.sax.saxutils.unescape(s)


from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaListByRef as makeGangaListByRef

# config_scope is namespace used for evaluating simple objects (e.g. File) 
from Ganga.Utility.Config import config_scope

#def makeGangaList(l):
#    return l[:]

################################################################################
# A visitor to print the object tree into XML.
#
class VStreamer(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying the name of properties which should not be printed
    # e.g. 'subjobs' - will not print subjobs
    def __init__(self,out=None,selection=''):
        self.level = 0
        self.nocomma = 1
        self.selection = selection
        if out:
            self.out = out
        else:
            import sys
            self.out = sys.stdout

    def begin_root(self):
        print >> self.out,'<root>'
        
    def end_root(self):
        print >> self.out,'</root>'
        
    def indent(self):
        return ' '*(self.level-1)*3
        
    def nodeBegin(self,node):
        self.level += 1
        s = node._schema
        print >> self.out,self.indent(),'<class name="%s" version="%d.%d" category="%s">'%(s.name,s.version.major,s.version.minor,s.category)
        self.nocomma = 1
        self.empty_body = 1
        
    def nodeEnd(self,node):
        print >> self.out,self.indent(),'</class>'
        self.level -= 1
        return
    
    def showAttribute( self, node, name ):
        return not node._schema.getItem(name)['transient'] and name!=self.selection 

    def simpleAttribute(self,node,name, value,sequence):
        if self.showAttribute( node, name ):
            self.level+=1
            print >> self.out, self.indent(),
            print >> self.out, '<attribute name="%s">'%name,

            def print_value(v):
                #print 'value',quote(v)
                print >> self.out,'<value>%s</value>'%self.quote(v),
                
            if sequence:
                self.level+=1
                print >> self.out
                print >> self.out, self.indent(),'<sequence>'
                for v in value:
                    self.level+=1
                    print >> self.out, self.indent(),
                    print_value(v)
                    print >> self.out
                    self.level-=1
                print >> self.out, self.indent(), '</sequence>'
                self.level-=1
                print >> self.out, self.indent(), '</attribute>'
            else:
                self.level+=1
                print_value(value)
                self.level-=1
                print >> self.out,'</attribute>'
            self.level-=1

    def sharedAttribute(self,node,name, value,sequence):
        if self.showAttribute( node, name ):
            self.level+=1
            print >> self.out, self.indent(),
            print >> self.out, '<attribute name="%s">'%name,

            def print_value(v):
                #print 'value',quote(v)
                print >> self.out,'<value>%s</value>'%self.quote(v),
                
            if sequence:
                self.level+=1
                print >> self.out
                print >> self.out, self.indent(),'<sequence>'
                for v in value:
                    self.level+=1
                    print >> self.out, self.indent(),
                    print_value(v)
                    print >> self.out
                    self.level-=1
                print >> self.out, self.indent(), '</sequence>'
                self.level-=1
                print >> self.out, self.indent(), '</attribute>'
            else:
                self.level+=1
                print_value(value)
                self.level-=1
                print >> self.out,'</attribute>'
            self.level-=1





    def acceptOptional(self,s):
        self.level+=1     
        if s is None:
            print >> self.out,self.indent(), '<value>None</value>'
        else:
            s.accept(self)
        self.level-=1 
    
    def componentAttribute(self,node,name,subnode,sequence):
        if self.showAttribute( node, name ):
            self.empty_body = 0
            self.level+=1
            print >> self.out, self.indent(), '<attribute name="%s">'%name
            if sequence:
                self.level+=1
                print >> self.out, self.indent(), '<sequence>'
                for s in subnode:
                    self.acceptOptional(s)
                print >> self.out,self.indent(), '</sequence>'
                self.level-=1
            else:
                self.acceptOptional(subnode)
            print >> self.out,self.indent(), '</attribute>'
            self.level-=1 

    def quote(self,x):
        return escape(repr(x))


################################################################################
# XML Parser.

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.Utility.Plugin import PluginManagerError, allPlugins
from Ganga.GPIDev.Schema import Schema, Version

from Ganga.Utility.logging import getLogger
logger = getLogger()

# Empty Ganga Object
class EmptyGangaObject(GangaObject):
    """Empty Ganga Object. Is used to construct incomplete jobs"""
    _schema = Schema(Version(0,0), {})
    _name   = "Unknown"
    _category = "unknownObjects"
    _hidden = 1


class SchemaVersionError(Exception):
    pass

class Loader:
    """ Job object tree loader.
    """
    
    def __init__(self):
        self.stack = None # we construct object tree using this stack
        self.ignore_count = 0 # ignore nested XML elements in case of data errors at a higher level
        self.errors = [] # list of exception objects in case of data errors
        self.value_construct = None #buffer for <value> elements (evaled as python expressions)
        self.sequence_start = [] # buffer for building sequences (FIXME: what about nested sequences?)

    def parse(self,s):
        """ Parse and load object from string s using internal XML parser (expat).
        """
        import xml.parsers.expat

        # 3 handler functions
        def start_element(name, attrs):
            ###logger.debug('Start element: name=%s attrs=%s', name, attrs) #FIXME: for 2.4 use CurrentColumnNumber and CurrentLineNumber
            # if higher level element had error, ignore the corresponding part of the XML tree as we go down
            if self.ignore_count:
                self.ignore_count += 1
                return

            # initialize object stack
            if name == 'root':
                assert self.stack is None,"duplicated <root> element"
                self.stack = []
                return

            assert not self.stack is None,"missing <root> element"

            # load a class, make empty object and push it as the current object on the stack 
            if name == 'class':
                try:
                    cls = allPlugins.find(attrs['category'],attrs['name'])
                except PluginManagerError,e:
                    self.errors.append(e)
                    #self.errors.append('Unknown class: %(name)s'%attrs)
                    obj = EmptyGangaObject()
                    self.ignore_count = 1 # ignore all elemenents until the corresponding ending element (</class>) is reached
                else:
                    version = Version(*[int(v) for v in attrs['version'].split('.')])
                    if not cls._schema.version.isCompatible(version):
                        attrs['currversion'] = '%s.%s'%(cls._schema.version.major,cls._schema.version.minor)
                        self.errors.append(SchemaVersionError('Incompatible schema of %(name)s, repository is %(version)s currently in use is %(currversion)s'%attrs))
                        obj = EmptyGangaObject()
                        self.ignore_count = 1 # ignore all elemenents until the corresponding ending element (</class>) is reached
                    else:
                        # make a new ganga object
                        obj  = super(cls, cls).__new__(cls)
                        obj._data = {}
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
            ###logger.debug('End element: name=%s', name)

            # if higher level element had error, ignore the corresponding part of the XML tree as we go up
            if self.ignore_count:
                self.ignore_count -= 1
                return

            # when </attribute> is seen the current object, attribute name and value should be on top of the stack
            if name == 'attribute':
                value = self.stack.pop()
                aname = self.stack.pop()
                obj = self.stack[-1]
                # update the object's attribute
                obj._data[aname] = value

            # when </value> is seen the value_construct buffer (CDATA) should be a python expression (e.g. quoted string)
            if name == 'value':
                # unescape the special characters
                s = unescape(self.value_construct)
                ###logger.debug('string value: %s',s)
                val = eval(s,config_scope)
                ###logger.debug('evaled value: %s type=%s',repr(val),type(val))
                self.stack.append(val)
                self.value_construct = None

            # when </sequence> is seen we remove last items from stack (as indicated by sequence_start)
            # we make a GangaList from these items and put it on stack
            if name == 'sequence':
                pos = self.sequence_start.pop()
                alist = makeGangaListByRef(self.stack[pos:])
                del self.stack[pos:]
                self.stack.append(alist)

            # when </class> is seen we finish initializing the new object
            # by setting remaining attributes to their default values
            # the object stays on the stack (will be removed by </attribute> or is a root object)
            if name == 'class':
                obj = self.stack[-1]
                for attr, item in obj._schema.allItems():
                    if not attr in obj._data:
                        obj._data[attr] = obj._schema.getDefaultValue(attr)
                obj.__setstate__(obj.__dict__) # this sets the parent
                
        def char_data(data):
            # char_data may be called many times in one CDATA section so we need to build up
            # the full buffer for <value>CDATA</value> section incrementally
            if self.value_construct is not None:
                ###logger.debug('char_data: append=%s',data)
                #FIXME: decode data
                self.value_construct += data

        #start parsing using callbacks
        p = xml.parsers.expat.ParserCreate()

        p.StartElementHandler = start_element
        p.EndElementHandler = end_element
        p.CharacterDataHandler = char_data

        p.Parse(s)

        assert len(self.stack)==1, 'multiple objects inside <root> element'

        obj = self.stack[-1]
        if obj._name == 'Unknown':
            raise Exception('Unable to create root object',self.errors)

        return obj,self.errors


################################################################################
# JUNK

#-o[Logging]Ganga.GPIDev.Lib.JobRegistry=DEBUG -o[Logging]Ganga.Core.JobRepository=DEBUG -o[Configuration]StartupGPI="from Ganga.GPIDev.Base.VStreamer import p,g; j=Job(inputsandbox=['x','y'],outputsandbox=['a','b']); p(j); jj=g(j);"


def p(j,f=None):
    vstreamer = VStreamer(out=f)
    vstreamer.begin_root()
    j._impl.accept(vstreamer)
    vstreamer.end_root()

def g(j):
    import StringIO
    s = StringIO.StringIO()
    p(j,s)
    return Parser().parse(s.getvalue())

################################################################################
