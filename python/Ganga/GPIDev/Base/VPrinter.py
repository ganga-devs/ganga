################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VPrinter.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Base.Proxy import isProxy, isType, runProxyMethod

def quoteValue(value, selection):
    """A quoting function. Used to get consistent formatting"""
    #print "Quoting",repr(value),selection
    if type(value) == type(''):
        if selection == 'copyable':

            value = value.replace('"',R'\"')
            value = value.replace("'",R"\'")
                
#DISABLED
##             valueList = list( value )
##             for i in range( len( value ) ):
##                 c = value[ i ]
##                 if c in [ "'", '"' ]:
##                     valueList[ i ] = "\\" + c
##                     value = "".join( valueList )
            if 1 + value.find( "\n" ):
                #print 'Quote result',"'''" + value + "'''"
                return "'''" + value + "'''"
        #print 'Quote result',"'"+value+"'"
        return "'"+value+"'"
    #print 'Quote result',value
    return value    

def indent(level):
    return ' '*(level-1)*3

# A visitor to print the object tree.
class VPrinter(object):
    # Arguments:
    # out: file-like output stream where to print, default sys.stdout
    # selection: string specifying properties to print (default ''):
    #            'all'            - print all properties
    #            'copyable'       - print only copyable properties
    #            any other string - print unhidden properties
    def __init__(self,out=None,selection=''):
        self.level = 0
        self.nocomma = 1
        self.selection = selection
        if out:
            self.out = out
        else:
            import sys
            self.out = sys.stdout

        self.empty_body = 0 # detect whether the body is empty to handle the comma correctly in this case too

    def indent(self):
        return indent(self.level)

    def comma(self,force=0):
        if not self.nocomma or force: 
            print >> self.out, ","

        self.nocomma = 0
        
    def nodeBegin(self,node):
        self.level += 1
        print >> self.out,node._schema.name,'('
        self.nocomma = 1
        self.empty_body = 1
        
    def nodeEnd(self,node):
        if self.empty_body:
            print >> self.out, self.indent(), ')',
            self.nocomma = 0
        else:
            if self.nocomma:
                #print >> self.out, 'NOCOMMA',
                print >> self.out, ')',
            else:
                #print >> self.out, 'COMMA',
                print >> self.out,'\n',self.indent(),')',
                
        self.level -= 1
        if self.level == 0: print >> self.out,'\n'

    def showAttribute( self, node, name ):
        visible = False
        if self.selection == 'all':
            visible = True
        elif self.selection == 'copyable':
            if node._schema.getItem(name)['copyable']:
                if not node._schema.getItem(name)['hidden']:
                    visible = True
        elif self.selection == 'preparable':
            #the following relies on the assumption that we only ever call printPrepTree on 
            #a preparable application.
            if node._schema.getItem(name)['preparable'] or self.level == 2:
                if not node._schema.getItem(name)['hidden']:
                    visible = True
        else:
            if not node._schema.getItem(name)['hidden']:
                visible = True
        return visible

    def simpleAttribute(self,node,name, value,sequence):
        if self.showAttribute( node, name ):
            self.empty_body = 0
            self.comma()
            # DISABLED
            #print '*'*20,name
            #if sequence:
            #    print 'transformation:',repr(value)
            #    value = value.toString()
            #    print 'into',repr(value)
            #else:
            #    print 'no transformation'
            print >> self.out, self.indent(), name, '=', self.quote(value),

    def sharedAttribute(self,node,name, value,sequence):
        if self.showAttribute( node, name ):
            self.empty_body = 0
            self.comma()
            # DISABLED
            #print '*'*20,name
            #if sequence:
            #    print 'transformation:',repr(value)
            #    value = value.toString()
            #    print 'into',repr(value)
            #else:
            #    print 'no transformation'
            print >> self.out, self.indent(), name, '=', self.quote(value),

    def acceptOptional(self,s):
        if s is None:
            print >> self.out, None,
        else:
            runProxyMethod(s,'accept',self)

    def componentAttribute(self,node,name,subnode,sequence):
        if self.showAttribute( node, name ):
            self.empty_body = 0
            self.comma()
            print >> self.out, self.indent(), name, '=',
            if sequence:
                print >> self.out, '[',
                for s in subnode:
                    self.acceptOptional(s)
                    print >> self.out,',',
                print >> self.out, ']',
            else:
                self.acceptOptional(subnode)

    def quote(self,x):
        return quoteValue(x, self.selection)

class VSummaryPrinter(VPrinter):
    """A class for printing summeries of object properties in a customisable way."""

    def __init__(self,level, verbosity_level, whitespace_marker, out=None,selection=''):
        super(VSummaryPrinter,self).__init__(out,selection)
        self.level = level
        self.verbosity_level = verbosity_level
        self.whitespace_marker = whitespace_marker

    def _CallSummaryPrintMember(self, node, name, value):
        """Checks to see whether there is a summary_print function pointer
        available in the schema object. If so it uses it and returns True
        otherwise it returns False.
        """
        function_pointer_available = False

        #check whether a print_summary function has been defined
        print_summary = node._schema.getItem(name)['summary_print']
        if print_summary != None:
            fp = getattr(node,print_summary)
            str_val = fp(value,self.verbosity_level)
            self.empty_body = 0
            self.comma()
            print >> self.out, self.indent(), name, '=', self.quote(str_val),
            function_pointer_available = True
        return function_pointer_available

    def _CallPrintSummaryTree(self, obj):
        import StringIO
        sio = StringIO.StringIO()
        runProxyMethod(obj, 'printSummaryTree',self.level, self.verbosity_level, self.indent(), sio, self.selection)
        result = sio.getvalue()
        if result.endswith('\n'):
            result = result[0:-1]
        print >>self.out, result,

    def simpleAttribute(self, node, name, value, sequence):
        """Overrides the baseclass method. Tries to print a summary of the attribute."""
        if not self.showAttribute( node, name ):
            return
        if self._CallSummaryPrintMember(node,name,getattr(node,name)):
            return
        
        if sequence:
            self.empty_body = 0
            self.comma()
            print >> self.out, self.indent(), name, '=',
            self._CallPrintSummaryTree(value)
            return

        #just go back to default behaviour
        super(VSummaryPrinter,self).simpleAttribute(node, name, value, sequence)

    def sharedAttribute(self, node, name, value, sequence):
        """Overrides the baseclass method. Tries to print a summary of the attribute."""
        if not self.showAttribute( node, name ):
            return
        if self._CallSummaryPrintMember(node,name,getattr(node,name)):
            return
        
        if sequence:
            self.empty_body = 0
            self.comma()
            print >> self.out, self.indent(), name, '=',
            self._CallPrintSummaryTree(value)
            return

        #just go back to default behaviour
        super(VSummaryPrinter,self).sharedAttribute(node, name, value, sequence)
        
    def componentAttribute(self,node,name,subnode,sequence):
        if not self.showAttribute( node, name ):
            return
        if self._CallSummaryPrintMember(node,name,subnode):
            return
        from Objects import GangaObject
        if isType(subnode,GangaObject):
            self.empty_body = 0
            self.comma()
            print >> self.out, self.indent(), name, '=',
            self._CallPrintSummaryTree(subnode)
            return

        #just go back to default behaviour
        super(VSummaryPrinter,self).componentAttribute(node, name, subnode, sequence)

def full_print(obj, out = None):
    """Print the full contents of a GPI object without abbreviation."""
    import sys
    if out == None:
        out = sys.stdout
        
    from Ganga.GPIDev.Lib.GangaList import GangaList
    if isType(obj,GangaList):
        obj_len = len(obj)
        if obj_len == 0:
            print >>out, '[]',
        else:
            import StringIO
            outString = '['
            count = 0
            for x in obj:
                if isinstance(x, GangaObject):
                    sio = StringIO.StringIO()
                    x.printTree(sio)
                    result = sio.getvalue()
                    #remove trailing whitespace and newlines
                    outString += result.rstrip()
                else:
                    result = str(x)
                    #remove trailing whitespace and newlines
                    outString += result.rstrip()
                count += 1
                if count != obj_len: outString += ', '
            outString += ']'
            print >>out, outString, 
        return

    if isProxy(obj):
        import StringIO
        sio = StringIO.StringIO()
        runProxyMethod(obj,'printTree',sio)
        print >>out, sio.getvalue(),
    else:
        print >>out, str(obj),



def summary_print(obj, out = None):
    """Print the summary contents of a GPI object with abbreviation."""
    import sys
    if out == None:
        out = sys.stdout
        
    from Ganga.GPIDev.Lib.GangaList import GangaList
    if isType(obj,GangaList):
        obj_len = len(obj)
        if obj_len == 0:
            print >>out, '[]',
        else:
            import StringIO
            outString = '['
            count = 0
            for x in obj:
                if isinstance(x, GangaObject):
                    sio = StringIO.StringIO()
                    x.printSummaryTree(0,0,'',out = sio)
                    result = sio.getvalue()
                    #remove trailing whitespace and newlines
                    outString += result.rstrip()
                else:
                    result = str(x)
                    #remove trailing whitespace and newlines
                    outString += result.rstrip()
                count += 1
                if count != obj_len: outString += ', '
            outString += ']'
            print >>out, outString, 
        return

    if isProxy(obj):
        import StringIO
        sio = StringIO.StringIO()
        runProxyMethod(obj,'printSummaryTree',0,0,'',sio)
        print >>out, sio.getvalue(),
    else:
        print >>out, str(obj),

    
