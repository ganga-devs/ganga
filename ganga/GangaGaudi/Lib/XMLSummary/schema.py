'''Module for processing XMLSchema.
The object VTree holds a validated tree of xml.
The object Schema holds the parsed xml schema.

The Schema object is the main functional object.
The VTree object is a light-weight wrapper of the validated xml code.
The VTree can only be altered in a way which conforms to the schema.

Schema:

    Default xml can be generated from the schema: create_default(tag)
    Existing xml can be parsed/validated against the schema: parse(file)
    Most of the xml 1.0 standard is allowed/parsed
    Should not be altered once the schema has been parsed

    print the schema to see all valid types and tags
    query the constraints of a tag with Tag_constraints(tag)
    helper functions to find out what is allowed are prefixed with Tag_
    functions which print out information about the schema start with lower case
    
    inconsistent or erroneous schema will throw a NameError, TypeError or AttributeError 
    
VTree:
    
    Typical data object validated against the schema
    Usually created from a default from the Schema object,
    or by parsing existing xml from the Schema object
    Can only be altered in a way which conforms to the schema.
    
    Attributes for getting and setting of attribs, and getting of children
    are automatically generated.
    sum.an_attribute_that_is_defined_by_schema(value)
    is a shortcut for
    sum.attrib('an_attribute_that_is_defined_by_schema', value)
    
    sum.a_child_that_is_defined_by_schema(attrib,value)
    is a shortcut for
    sum.children('a_child_that_is_defined_by_schema', child)
    
    trying to change the VTree in a way which does not conform to the schema
    will throw a NameError, ValueError, TypeError or AttributeError 
'''

id = '$Id: schema.py,v 1.5 2009-10-07 13:51:18 rlambert Exp $'

__author__ = 'Rob Lambert'
__date__ = id.split()[3]
__version__ = id.split()[2]

try:
    from xml.etree import cElementTree as __ElementTree__
except ImportError:
    try:
        from xml.etree import ElementTree as __ElementTree__
    except ImportError:
        try:
            from etree import ElementTree as __ElementTree__
        except ImportError:
            import ElementTree as __ElementTree__
            #finally fail here if module cannot be found!

import os as __os__


class VTree(object):
    '''a validated tree object
    The object holds a reference to the schema used
    print a VTree to see its content
    It is unlikely this class will be created by the user,
    rather it should be returned from parsing a file or creating a default object
    using the schema.
    Trying to change the VTree in a way which does not conform to the schema
    will throw a NameError, ValueError, TypeError or AttributeError 
    '''
    #__name__='VTree'
    #__type__='VTree'
    def __init__(self,element,schema, mother=None, check=True):
        '''constructor.
        Unlikely to be called without using the schema object directly
        From an etree element, and a schema the validated object will be formed
        mother is a pointer to the mother element of the tree, to keep track of the level
        check signifies if a recursive check against the schema is required'''
    #    docstr='''
    #This particular VTree is for the tag '###TAG###'
    #which means it gets the extra methods:'''
        #the schema object is validated against
        self.__schema__=schema
        #the validated object
        self.__element__=element
        self.__mother__=mother
        if check:
            if not self.__schema__.__check__(element):
                raise ValueError("cannot validate element")
        #self.__doc__=self.__doc__+docstr.replace('###TAG###', self.tag())
        ##auto generate methods for defined children and attribs
        #for key in self.attrib().keys():
        #    try:
        #        dstr="self.###ATTRIB###".replace("###ATTRIB###",key)
        #        exec dstr
        #    except (NameError, AttributeError, ValueError, SyntaxError): pass
        #for child in self.__schema__.Tag_children(self.tag()):
        #    try:
        #        dstr="self.###ATTRIB###".replace("###ATTRIB###",child)
        #        exec dstr
        #    except (NameError, AttributeError, ValueError, SyntaxError): pass
        #except all pre-thought of exceptions here... to avoid errors in init!
    def __reimport__(self):
        global __os__
        global __ElementTree__
        
        try:
            from xml.etree import ElementTree as __ElementTree__
        except ImportError:
            try:
                from etree import ElementTree as __ElementTree__
            except ImportError:
                import ElementTree as __ElementTree__
            #finally fail here if module cannot be found!
        import os as __os__
        
    def __children__(self):
        '''list the existing children'''
        list=[]
        for c in self.__element__.getchildren():
            list.append(c.tag)
        return list
    def __repr__(self):
        '''how to print this object'''
        return self.__str__()
    def __str__(self):
        '''how to print this object'''
        ret= 'VTree-'+self.tag()+': '
        v=self.value()
        if v is not None:
            if type(v)==type([]):
                ret+=self.__schema__.__list2str__(self.value())
            else: ret+=str(v)
        if len(self.attrib()):
            ret+='\n attrib='+str(self.attrib())
        if len(self.__children__()):
            ret+='\n children='+str(self.__children__())
        return ret
    
    def __is__(self, ele, tag=None, attrib=None, value=None):
        '''internal finding function used by find and children
        the default, None, makes no requirement on the children.
        Attrib can be a single att, a list of required attribs, or a dictionary of attrib:value
        value only be a single value, what you expect the object to hold
        tag can be a single tag, or a list
        multiple tags are ORED.
        multiple attributes are ANDED
        '''        
        #check for tags
        if tag is not None:
            if type([]) == type(tag):
                if ele.tag not in tag:
                    return False
            else:
                if tag!=ele.tag:
                    return False
        #check for attributes
        if attrib is not None:
            #list of attributes which must exist
            if str(type([])) in str(type(attrib)):
                for att in attrib:
                    if att not in ele.attrib: return False
                
            #list of attribute:value pairs
            elif str(type({})) in str(type(attrib)):
                for att in attrib:
                    if att not in ele.attrib: return False
                    vc=self.__schema__.Tag_castValue(att,attrib[att])
                    vatt=self.__schema__.Tag_castValue(att, ele.attrib[att])
                    if vc is None or vatt is None: return False
                    if vc!=vatt: return False
            elif attrib not in ele.attrib: return False
        #check for value
        if value is not None:
            if not ele.text: return False
            try:
                value=self.__schema__.Tag_castValue(ele.tag,value)
            except (ValueError, TypeError, KeyError):
                return False
            if value!=self.__schema__.Tag_castValue(ele.tag,ele.text): return False
                
        return True
    
    #def __getattr__(self, name):
    #    '''append a get/set method with the name of the attribute'''
    #    docstr=""
    #    dstr='''class tmp_###ATTRIB###:
    #def __init__(self,V):
    #    self.V=V''' 
    #    type=None
    #    if name in self.attrib():
    #        docstr='''
    #    ###ATTRIB###(value=None), to get/set '###ATTRIB###' '''
    #        dstr=dstr+'''
    #def value(self, value=None):
    #    "shorcut to set/get ###ATTRIB###, sum.attrib('###ATTRIB###', value)" 
    #    return self.V.attrib('###ATTRIB###',value)'''
    #    elif name in self.__schema__.Tag_children(self.tag()):
    #        docstr='''
    #    ###ATTRIB###(attrib=None, value=None), to get a list of '###ATTRIB###' children'''
    #        dstr=dstr+'''
    #def value(self, attrib=None, value=None):
    #    "shorcut to get ###ATTRIB###, sum.children('###ATTRIB###', attrib, value)" 
    #    return self.V.children('###ATTRIB###',attrib,value)'''
    #    else:
    #        raise AttributeError, name + ' is not a valid attribute or child'
    #    
    #    docstr=docstr.replace('###ATTRIB###',name)
    #    dstr=dstr.replace('###ATTRIB###',name)
    #    #print dstr
    #    exec dstr
    #    dstr="self.###ATTRIB###=tmp_###ATTRIB###(self).value".replace('###ATTRIB###',name)
    #    #print dstr
    #    exec dstr
    #    dstr="ret=self.###ATTRIB###".replace('###ATTRIB###',name)
    #    #print dstr
    #    exec dstr
    #    #only increase the docstring if the process completes OK
    #    self.__doc__+=docstr
    #    return ret

    
        
    def __append_element__(self, child):
        '''internal method to append validated elements'''
        self.__element__.append(child.__element__)
        
        #set the level
        #if there's text there, ignore!
        if self.__element__.text is not None and self.__element__.text.strip()!='':
            return True
        #get the level from the mother
        if self.__element__.text is None:
            if (self.__mother__ is None or
                self.__mother__.__element__.text is None or
                self.__mother__.__element__.text.strip()!=''):
                self.__element__.text='\n\t'
                #self.__element__.tail='\n'
            else:
                self.__element__.text=self.__mother__.__element__.text+'\t'
                #self.__element__.tail=self.__mother__.__element__.text
        
        #if there's text there, ignore!
        #if self.__element__.text is None or self.__element__.text.strip()=='':
        self.__element__.getchildren()[-1].tail=str(self.__element__.text)[:-1]
        if len(self.__element__.getchildren())>1:
            if self.__element__.text is None or self.__element__.text.strip()=='':
                self.__element__.getchildren()[-2].tail=str(self.__element__.text)
        return True


    def __insert_element__(self,child,index):
        '''internal method to insert validated elements'''
        element_size=len(self.__element__.getchildren())

        if (element_size==0 or index>=element_size):
            return self.__append_element__(child)

        if (index<=(-element_size)):
            real_index=0
        elif (index<0):
            real_index=index + element_size
        else:
            real_index=index     
        self.__element__.insert(real_index,child.__element__)
        
        #set the level
        # now size==element_size+1
        if real_index!=element_size-1:
            self.__element__.getchildren()[real_index].tail=self.__element__.getchildren()[real_index+1].tail
        else:
            self.__element__.getchildren()[real_index].tail=self.__element__.getchildren()[real_index+1].tail[:-1]

        return True

    def __remove_element__(self,child):
        '''internal method to remove validated elements'''
        children=self.__element__.getchildren()
        element_size=len(children)
        if element_size==1:
            self.__element__.remove(child.__element__)
            # remove text if there were only \n's or \t's
            if self.__element__.text is not None and self.__element__.text.strip()=='':
                self.__element__.text=None
            return True
        elif element_size>1:
            if child.__element__==children[element_size-1]:
                # copy tail on the previous child
                children[-2].tail=children[-1].tail
            self.__element__.remove(child.__element__) 
            return True
        else:
            raise TypeError('This should never happen since child is supposed to belong to the children of the element')
            return False

    def test(self):
        '''tests that the object is OK'''
        try:
            if (__ElementTree__ is None or __os__ is None or
                self.__schema__ is None or self.__element__ is None):
                return False
            self.__element__.text
            return True
        except:
            return False
    
    def dump(self):
        '''dump the object to the screen'''
        print(self.xml())
    def xml(self):
        '''dump the object to an xml string'''
        if __os__ is None or __ElementTree__ is None:
            self.__reimport__()
            if __os__ is None or __ElementTree__ is None:
                raise ImportError("problem with modules!")
                return False #not needed
        return self.__schema__.header()+'\n'+__ElementTree__.tostring(
            self.__element__).replace(
            'ns0:',self.__schema__.__ns__.rstrip(':')+'i:').replace(
            ':ns0',':'+self.__schema__.__ns__.rstrip(':')+'i').replace(
            self.__schema__.__schemafile_long__,self.__schema__.__schemafile_short__)
    def write(self,outfile):
        ''' write xml to a file'''
        if outfile is None: return False
        if outfile=="": return False
        if __os__ is None or __ElementTree__ is None:
            self.__reimport__()
            if __os__ is None or __ElementTree__ is None:
                raise ImportError("problem with modules!")
                return False #not needed
        outfile=__os__.path.expanduser(__os__.path.expandvars(outfile))
        if not self.__schema__.Tag_isRoot(self.tag()):
            raise IOError("cannot output a file which doesn't have the root object")
            return False #not needed
        f=open(outfile,'w')
        if not f:
            raise IOError('Error opening file for writing'+str(outfile))
            return False
        f.write(self.xml())
        f.close()
        return True
    def constraints(self):
        '''get the constraints from the schema '''
        return self.__schema__.Tag_constraints(self.__element__.tag)
    def attrib(self, att=None, val=None):
        '''return a dictionary of the existing attributes
        this dictionary is not connected to the element
        if att is set, will return only the value of the given attribute
        if val is set, will set att to the given val'''
        #print all attribs
        if att is None:
            list={}
            for c in self.__element__.attrib:
                list[c]=self.__schema__.__cast_from_tag__(c,self.__element__.attrib[c])
            return list
        #print the given attrib
        if val is None:
            try:
                return self.__schema__.__cast_from_tag__(att,self.__element__.attrib[att])
            except KeyError:
                raise AttributeError(self.tag()+" has no attribute " + att)
                return None
        #set the given attrib
        if not self.__schema__.Tag_hasAttrib(self.tag(),att):
            raise AttributeError(self.tag()+" has no attribute " + att)
            return False
        
        #elif self.__schema__.Tag_canHaveValue(att,val):
        self.__element__.attrib[att]=str(self.__schema__.Tag_castValue(att,val))
        if self.__schema__.Tag_whitespace(att)=='collapse':
            self.__element__.attrib[att]=self.__element__.attrib[att].rstrip().lstrip()
        return True
        #else:
        #    raise ValueError, att+" cannot take a value " + str(val)
        #    return False
    def addto(self, mother, child):
        '''Add the child to the first mother'''
        return self.find(mother)[0].add(child)

    # ORDER IN SEQUENCES IS NOT CHECKED !!!
    def add(self, child,index=None):
        '''add a child to the tree'''
        name=''
        if 'VTree' in child.__str__():
            name=child.tag()
        elif 'str' in str(type(child)):
            name=child
        else:
            try:
                name=child.tag()
            except (AttributeError, ValueError, TypeError):
                raise TypeError('you can only add VTree objects, or use a string to add the default object')
                return False
        if name not in self.__schema__.Tag_children(self.tag()):
            raise TypeError('cannot add '+ name+ ' to ' + self.tag()+ ' as this child is not allowed in the schema,')
            return False
        if self.nChildren(name)==self.__schema__.Tag_nChild(self.tag(),name)[1]:
            raise TypeError('cannot add'+ name+ ' to ' + self.tag()+ ' as there are enough of this child already', name)
            return False
        if 'VTree' in child.__str__():
            if index is None:
                return self.__append_element__(child)
            else:
                return self.__insert_element__(child,index)
        else:
            if index is None:
                return self.__append_element__(self.__schema__.create_default(name))
            else:
                return self.__insert_element__(self.__schema__.create_default(name),index)


    def remove(self,child):
        '''remove a child from the tree'''
        if child.__element__ not in self.__element__.getchildren():
            raise TypeError('This object does not contain this child, the child cannot be removed')
            return False
        if self.nChildren(child.tag())==self.__schema__.Tag_nChild(self.tag(),child.tag())[0]:
            raise TypeError('cannot remove '+ child.tag()+ ' to ' + self.tag()+ ' as there will not be enough of this child')
            return False
        else:
            return self.__remove_element__(child)
        
    
    def value(self, val=None):
        '''return the existing value, or None
        a string is returned which is not connected to the element
        if val is given, will set the value to this'''
        #return the value
        #print 'in value using', self.__element__.text
        if val is None:
            if self.__element__.text is None:
                return None
            if len(self.__element__.text.rstrip().lstrip())==0:
                return None
            if self.__schema__.Tag_whitespace(self.tag())=='collapse':
                return self.__schema__.__cast_from_tag__(self.tag(),self.__element__.text.rstrip().lstrip())
            return self.__schema__.__cast_from_tag__(self.tag(),self.__element__.text)
        #set the value
        v=self.__schema__.Tag_castValue(self.tag(),val)
        #if self.__schema__.Tag_canHaveValue(self.tag(),val):
        #    v=self.__schema__.Tag_castValue(self.tag(),val)
        if str(type([]))==str(type(v)):
            v=self.__schema__.__list2str__(v)
        self.__element__.text=str(v)
        if self.__schema__.Tag_whitespace(self.tag())=='collapse':
            self.__element__.text=self.__element__.text.rstrip().lstrip()
        return True
        #else:
        #    raise ValueError, self.tag()+" cannot take a value " + str(val)
        #    return False
    def tag(self):
        '''return the existing tag'''
        return ''+self.__element__.tag
    def nChildren(self, tag=''):
        '''how many children of this type are there?'''
        if tag=='':
            return len(self.children())
        else:
            n=0
            for c in self.__children__():
                if c==tag:
                    n=n+1
        return n
    def mother(self):
        '''return the mother of this VTree element
        used to allow full tree navigation'''
        return self.__mother__
    def children(self, tag=None, attrib=None, value=None):
        '''return a list of the direct children with the given tags, attributes and values.
        the default, None, makes no requirement on the children.
        Attrib can be a list of required attribs, or a dictionary of attrib:value
        value only be a single value, what you expect the object to hold
        tag can be a single tag, or a list
        multiple tags are ORED.
        multiple attributes are ANDED
        '''
        it=self.__element__.getchildren()
        list=[]
        for child in it:
            if self.__is__(child,tag,attrib,value):
                list.append(VTree(child,self.__schema__,self,False))
        return list
    
    def find(self, tag=None, attrib=None, value=None):
        '''return a list of the elements with the given tags, attributes and values.
        from this level downwards.
        the default, None, makes no requirement on the daughters.
        Attrib can be a list of required attribs, or a dictionary of attrib:value
        value can be a single value, or a list of strings
        tag can be a single tag, or a list
        tags and values are ORED.
        attributes are ANDED'''
        it=None
        if tag is not None:
            if str(type([])) in str(type(tag)):
                it=self.__element__.getiterator()
            else:
                it=self.__element__.getiterator(tag)
        else:
            it=self.__element__.getiterator()
        list=[]
        for child in it:
            if self.__is__(child,tag,attrib,value):
                list.append(VTree(child,self.__schema__,self,False))
        return list
    
    def __clone_element__(self, ele):
        def_e=__ElementTree__.Element(ele.tag,ele.attrib)
        def_e.text=ele.text
        def_e.tail=ele.tail
        for c in ele.getchildren():
            def_e.append(self.__clone_element__(c))
        return def_e
    
    #def __internal_clone__(self):
    #    '''return a clone of this element, with the same schema'''
    #    e2=self.__clone_element__(self.__element__)
    #    self.__element__==e2
        
    def clone(self):
        '''return a clone of this element, with the same schema'''
        e2=self.__clone_element__(self.__element__)
        return VTree(e2,self.__schema__,None,False)

class Schema(object):
    '''details about the xml schema
    The default constructor should parse most schema
    VTrees are created by the create_default(), parse() and validate() methods
    print a parsed schema to see its content
    inconsistent or erroneous schema will throw a NameError, TypeError or AttributeError '''
    #cached default elements
    #__def_cache__={}
    def __init__(self, schemafile, ns='xs', root=''):
        '''constructor. 
        schemafile is the name of the file containing the schema, usually an xsd file
        If the namespace is not equal to xs change by setting the value of ns=
        if the root element is not the first/only defined element, set with option root='''
        self.__tree__=__ElementTree__.ElementTree()
        #that is the parsed schema
        self.__header__=''
        self.__schemafile_long__=__os__.path.expanduser(__os__.path.expandvars(schemafile))
        self.__schemafile_short__=schemafile
        self.__root__=root
        self.__rootattribs__={}
        self.__def_cache__={}
        self.__mother_cache__={}
        self.__child_cache__={}
        self.__attrib_cache__={}
        self.__type_cache__={}
        self.__enum_cache__={}
        self.__fixed_cache__={}
        self.__default_cache__={}
        self.__whitespace_cache__={}
        self.__union_cache__={}
        self.__list_cache__={}
        self.__seq_cache__={}
        self.__ns__=ns+':'
        self.__uri__="{http://www.w3.org/2001/XMLSchema}"
        #all known tags
        self.__tags__=set()
        self.__tagelement__={}
        #all known types
        self.__basetypes__=set(["integer",
                        "long",
                        "unsignedLong",
                        "double",
                        "string",
                        "normalizedString",
                        "boolean"])
        #types I can cast into
        self.__cast_types__=set([t.lower() for t in self.__basetypes__ ])
        #dictionary to remove namespace without string manipulation
        self.__type_remove_namespace__={}
        self.__func_cast__={
            'n' : lambda x: str(x),   #normalizedString
            's' : lambda x: str(x),   #string
            'f' : lambda x: float(x), #float
            'l' : lambda x: int(x),  #long
            'i' : lambda x: int(x),   #integer
            'd' : lambda x: float(x)  #double
            }
        self.__types__=set()
        self.__typelement__={}
        #all known attributes
        self.__attribs__=set()
        #type of that attribute
        self.__attribelement__={}
        #self.__attrib_rules__=[
        #                "default",
        #              r  "fixed",
        #                "minOccurs",
        #                "maxOccurs",
        #                "use" ]
        #all known rules
        #self.__rules__=["enumeration",
        #                "whiteSpace"]
        if self.__schemafile_short__:
            pf=open(self.__schemafile_long__)
            self.__header__=pf.readlines()[0]
            pf.close()
            self.__parseschema__(self.__schemafile_long__)
            self.__isconsistent__()
            #if self.__root__=='' and len(self.__tags__)>0:
            #    self.__root__=self.__tags__[0]
            if self.__root__ not in self.__tags__:
                raise TypeError('root of schema not found '+self.__root__)
                return self.__init__()
            self.__rootattribs__[
                    "xmlns:"+self.__ns__.split(':')[0]+"i"
                        ]=(
                           self.__uri__.split('}')[0].split('{')[-1]
                           +"-instance"
                           )
            self.__rootattribs__[
                    self.__ns__.split(':')[0]+"i:noNamespaceSchemaLocation"
                        ]=(
                           self.__schemafile_long__
                           )
            self.__rootattribs__[
                self.__uri__.split('}')[0]
                +"-instance}"
                +"noNamespaceSchemaLocation"
                        ]=(
                           self.__schemafile_long__
                           )
            
    # ORDER IN SEQUENCES IS NOT CHECKED !!!
    def __check__(self,element):
        '''internal method to check an element conforms to the schema'''
        #check tag
        if element.tag not in self.tags():
            raise NameError('element '+ element.tag+ ' does not exist in the schema')
            return False
        #check value
        if element.text:
            if len(element.text.rstrip().lstrip()):
                if not self.Tag_canHaveValue(element.tag, element.text):
                    raise ValueError('element '+ element.tag+ ' has the wrong entry type for the schema')
                    return False
        #check attribs
        for att,val in element.items():
            #check the root attributes
            #if self.Tag_isRoot(element.tag):
            #    for att in self.__rootattribs__.keys():
            #        if att not in element.attrib.keys():
            #            print 'the root element', element.tag, 'must have the attrib', att
            #            return False
            #        if self.__rootattribs__[att]!=element.attrib[att]:
            #           print 'the root attribute', att, 'must have the value', self.__rootattribs__[att]
            #            return False
            if not self.Tag_hasAttrib(element.tag,att):
                raise AttributeError('element '+ element.tag+ ' cannot have attribute '+att +' in the schema')
                return False
            if self.Tag_isRoot(element.tag) and 'noNamespaceSchemaLocation' in att:
                if element.attrib[att].split('/')[-1]!=self.__schemafile_long__.split('/')[-1]:
                    raise AttributeError('root element '+ element.tag+
                                           ' must be from the same schema!! '+
                                            ' attribute '+att +' is '+ val+
                                            ' versus '+self.__schemafile_long__
                                            )
                    return False
            elif not self.Tag_canHaveValue(att, val):
                raise AttributeError('element '+ element.tag + 
                                       ' cannot have attribute ' + att +
                                       ' with value '+ val+
                                       ' in the schema'
                                       )
                return False
        #check required attribs
        for att in self.Tag_attribs(element.tag):
            if self.Tag_isAttribRequired(element.tag, att):
                if att not in element.attrib:
                    raise AttributeError( 'element '+ element.tag+ 
                                            ' must have attribute '+att +' in the schema'
                                            )  
                    return False
        
        #check children
        kiddic={}
        for child in element.getchildren():
            if child.tag in kiddic:
                kiddic[child.tag]=kiddic[child.tag]+1
            else:
                kiddic[child.tag]=1
            if child.tag not in self.Tag_children(element.tag):
                raise AttributeError( 'element '+ element.tag+ 
                                        'cannot have child '+child +' in the schema'
                                        )  
                return False
            if not self.__check__(child):
                return False
        #check number of children
        for child in self.Tag_children(element.tag):
            if self.Tag_nChild(element.tag, child)[0]>0:
                #print child, kiddic, element.tag
                try:
                    if self.Tag_nChild(element.tag, child)[0]>kiddic[child]:
                        raise AttributeError( 'element '+ element.tag+ 
                                                ' has not enough copies of '+child +
                                                ' for the schema'
                                                )
                        return False
                except KeyError:
                    raise AttributeError( 'element '+ element.tag+ 
                                            ' requires child '+child +
                                            ' for the schema'
                                            )
                    
            if self.Tag_nChild(element.tag, child)[1]>0:
                try:
                    if self.Tag_nChild(element.tag, child)[1]<kiddic[child]:
                        print('element', element.tag, 'has too many copies of',child ,'for the schema')
                        return False
                except KeyError:
                    pass
        return True
    
    def __str__(self):
        '''what to print to the screen'''
        return 'tags='+str(self.__tags__)+'\ntypes='+str(self.__types__)+'\nattribs='+str(self.__attribs__)+'\nroot='+self.__root__+':'+str(self.__rootattribs__)

    def __parseschema__(self, parsefile):
        '''internal method to parse a file into the schema'''
        #parse an existing schema
        if __os__.path.exists(parsefile):
            self.__tree__=__ElementTree__.parse(parsefile)
            rt=self.__tree__.getroot()
            if rt:
                self.__uri__=rt.tag[:rt.tag.find('schema')]
                for i in list(self.__basetypes__):
                    #add namespace to basic types
                    self.__basetypes__.add(
                        self.__ns__+i)
                    self.__basetypes__.add(self.__uri__+i)
                    self.__basetypes__.remove(i)
                    self.__type_remove_namespace__[self.__uri__+i]=i.lower()
                    self.__type_remove_namespace__[self.__ns__+i]=i.lower()
                    
                    
                for e in rt.getiterator( self.__uri__+"element"):
                    try:
                        self.__tags__.add( e.attrib['name'])
                        #print 'adding tagelement'
                        self.__tagelement__[e.attrib['name']]=e
                        #print 'added tagelement'
                        #root by default is the first thing in the list
                        if self.__root__=='':
                            self.__root__=e.attrib['name']
                    except KeyError: pass
                for e in rt.getiterator( self.__uri__+"simpleType"):
                    try:
                        self.__types__.add( e.attrib['name'])
                        #print 'adding typelement'
                        self.__typelement__[e.attrib['name']]=e
                        #print 'added typelement'
                    except KeyError: pass
                for e in rt.getiterator( self.__uri__+"complexType"):
                    try:
                        self.__types__.add( e.attrib['name'])
                        #print 'adding typelement'
                        self.__typelement__[e.attrib['name']]=e
                        #print 'added typelement'
                    except KeyError: pass
                for e in rt.getiterator( self.__uri__+"attribute"):
                    try:
                        self.__attribs__.add( e.attrib['name'])
                        #print 'adding attribelement'
                        self.__attribelement__[e.attrib['name']]=e
                        #print 'added attribelement'
                    except KeyError: pass
                return True
        return False
    
    def __isconsistent__(self):
        '''internal method to check the file is a consistent schema'''
        #check schema is self-consistent
        #all schema entries must have name or ref from list
        #all entries must have type or base from list
        rt=self.__tree__.getroot()
        if rt:
            for e in rt.getiterator():
                if self.__uri__ not in e.tag:
                    __ElementTree__.dump(e)
                    raise NameError("unknown element, without namespace "+ e.tag)
                    return False
                aname=self.__ename__(e)
                #print aname
                if not aname: continue
                if (aname not in self.__tags__ and
                    aname not in self.__attribs__ and
                    aname not in self.__types__ and
                    aname not in self.__basetypes__ ):
                    __ElementTree__.dump(e)
                    raise NameError("unknown element name "+ e.tag)
                    return False
                atype=self.__etype__(e)
                if not atype: continue
                if str(type([])) not in str(type(atype)):
                    atype=[atype]
                for aatype in atype:
                    #print aatype
                    if (aatype not in self.__types__
                        and aatype not in self.__basetypes__):
                        __ElementTree__.dump(e)
                        raise TypeError("unknown element type "+ aatype)
                        return False
        #print 'schema is self-consistent'
        return True
    
    def __ename__(self, e):
        '''internal method to return the name of e'''
        try:
            aname=e.attrib['name']
            return aname
        except KeyError:
            try:
                aname=e.attrib['ref']
                return aname
            except KeyError:
                pass
            
            return None
        
    def __etype__(self, e):
        '''internal method to return the type or list of types of e'''
        try:
            atype=e.attrib['type']
            return atype
        except KeyError:
            #print 'not simple type, looking at children'
            try:
                for ec in e.getiterator():
                    if 'restriction' in ec.tag or 'extension' in ec.tag:
                        #print 'found restriction'
                        try:
                            atype=ec.attrib['base']
                            return atype
                        except KeyError: continue
                    elif 'sequence' in ec.tag:
                        #print 'found sequence'
                        atype=[]
                        for ecc in ec.getiterator():
                            if 'element' in ecc.tag:
                                atype.append(self.__etype__(ecc))
                        #print 'returning', atype
                        return atype
                    elif 'list' in  ec.tag:
                        #print 'found list'
                        try:
                            atype=ec.attrib['itemType']
                            return [atype]
                        except KeyError: continue
                    elif 'union' in  ec.tag:
                        #print 'found union'
                        try:
                            atype=ec.attrib['memberTypes']
                            return atype.split(' ')
                        except KeyError: continue
            except (KeyError, ValueError): pass
        return None
    
    def __checkcast__(self, atype,test):
        '''internal method, can this value be cast to the type of the tag?'''
        #print 'checking cast of', test ,'to', atype
        try:
            self.__cast__(atype,test)
            return True
        except (ValueError, TypeError):
            return False
    
    def __cast__(self, atype,test):
        '''internal method cast to the type of the tag'''
        #print 'cast of', test ,'to', atype
        btype=atype
        if btype in self.__type_remove_namespace__:
            btype=self.__type_remove_namespace__[atype]
        else:
            btype=btype.lower()
        
        #print 'moving to', btype
        
        a=False
        #should I make it absolute?
        if 'unsigned'==btype[:8]:
            a=True
            btype=btype[8:]
        
        if btype in self.__cast_types__:
            
            if 'b'==btype[0]:
                #print 'checking bool'
                if type('')==type(test):
                    if test.lower()=='true':
                        return True
                    elif test.lower()=='false':
                        return False
                    return bool(float(test))
                return bool(float(test)!=0)
            else:
                v=self.__func_cast__[btype[0]](test)
                if a:
                    v=abs(v)
                return v
        
        raise TypeError('do not no how to convert to type '+ atype)
        return None
    
    def __list2str__(self,list):
        '''internal method cast a list to a string separated by spaces'''
        rets=''
        for l in list:
            rets+=str(l)+' '
        return rets.rstrip()
    def __constraint__(self, tag, constr):
        '''internal method get the value for this constraint as a string'''
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                return None
            raise NameError(str(tag)+' tag is not in the schema' )
        try:
            return ele.attrib[constr]
        except KeyError: pass
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.__constraint__(atype,constr)
        
    def __default__(self, tag):
        '''internal method get the default value for this tag/attribute as a string'''
        return self.__constraint__(tag, 'default')
    
    def __fixed__(self,tag):
        '''internal method to return fixed value for this tag as a string'''
        return self.__constraint__(tag, 'fixed')
    
    def __enumeration__(self, tag):
        '''internal method to return the Enumerated values for this tag/attribute as strings'''
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                return None
            raise NameError(str(tag)+' tag is not in the schema' )
        enums=[]
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'enumeration'):
                try:
                    enums.append(ec.attrib['value'])
                except KeyError: pass
            if (len(enums)):
                return enums
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.__enumeration__(atype)
        return None
    
    def __getele__(self,tag, atag=True, aatt=True,atype=True):
        '''internal method to return the element corresponding to a tag'''
        ele=None
        try:
            if atag and tag in self.__tags__:
                ele=self.__tagelement__[tag]
            elif aatt and tag in self.__attribs__:
                ele=self.__attribelement__[tag]
            elif atype and tag in self.__types__:
                ele=self.__typelement__[tag]
        except (KeyError,AttributeError): pass
        return ele 
        
    def __compound__(self,tag,compound):
        '''sequence, list, union?'''
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                return False
            raise NameError(str(tag)+' tag is not in the schema' )
        for e in ele.getiterator(self.__ns__+compound):
            return True
        for e in ele.getiterator(self.__uri__+compound):
            #print 'iterated'
            return True
        for atype in self.__types__:
            if self.__etype__(ele)==atype:
                #print 'is of type', atype
                return self.__compound__(atype,compound)
        return False
        
    def __hasConstraint__(self,tag,constr):
        '''internal method to check for default, fixed'''
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                return False
            raise NameError(str(tag)+' tag is not in the schema' )
        try:
            ele.attrib[constr]
            return True
        except KeyError: pass
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.__hasConstraint__(atype,constr)
        return False

    def __fast_parse__(self,xmlfile):
        '''Parse without checking'''
        xmlfile=__os__.path.expanduser(__os__.path.expandvars(xmlfile))
        if not __os__.path.exists(xmlfile):
            raise IOError('file does not exist '+str(xmlfile))
            return None
        
        self.__tree__=__ElementTree__.parse(xmlfile)
        rt=self.__tree__.getroot()
        if not rt or not self.Tag_isRoot(rt.tag):
            raise TypeError('This file does not have the root of the schema')
            return None
        return VTree(rt,self,None,False)
                                                                                                
    def parse(self,xmlfile):
        '''parse an xml document and validate against this schema'''
        thetree=self.__fast_parse__(xmlfile)
        if not self.__check__(thetree.__element__):
            raise TypeError('This file could not be validated against the schema')
            return None
        #print 'file', xmlfile, 'sucessfully validated against the schema'
        return thetree
    
    def validate(self,xmlfile):
        '''parse an xml document and validate against this schema'''
        return self.parse(xmlfile)
    
    def create_default(self, tag, level=0):
        '''return the default validated object from the schema'''
        if tag not in self.__tags__:
            raise NameError('This tag, ' + tag + ' is not in the schema')
            return None
        
        if (level==0) and (tag in self.__def_cache__):
            #return a clone
            #self.__def_cache__[tag].__internal_clone__()
            return self.__def_cache__[tag].clone()
        
        def_e=__ElementTree__.Element(tag)
        #fill attributes
        for att in self.Tag_attribs(tag):
            #print 'setting default for', att
            if self.Tag_isAttribRequired(tag,att):
                if self.Tag_isFixed(att):
                    def_e.attrib[att]=self.__fixed__(att)
                elif self.Tag_hasDefault(att):
                    def_e.attrib[att]=self.__default__(att)
                elif self.Tag_hasEnumeration(att):
                    def_e.attrib[att]=self.__enumeration__(att)[0]
                else:
                    def_e.attrib[att]=''
                #print def_e.attrib[att]
        #fill children, iterate    
        for child in self.Tag_children(tag):
            cn=0
            if self.Tag_nChild(tag,child)[0]>cn:
                def_e.append(self.create_default(child,level+1).__element__)
                cn+=1
        if len(def_e.getchildren()):
            #add new line
            if def_e.text: def_e.text+='\n'
            else:
                def_e.text='\n'
            #add justification of first child
            for i in range(level+1):
                def_e.text+='\t'
            #adjust justification of last child
            def_e.getchildren()[-1].tail='\n'
            for i in range(level):
                def_e.getchildren()[-1].tail+='\t'
                    
        #fill value
        if self.__fixed__(tag) is not None:
            def_e.text=self.__fixed__(tag)
        elif self.__default__(tag) is not None:
            def_e.text=self.__default__(tag)
        elif self.__enumeration__(tag) is not None:
            def_e.text=self.__enumeration__(tag)[0]
        #tail value
        def_e.tail='\n'
        for i in range(level):
            def_e.tail+='\t'
        #check if it is root
        if self.Tag_isRoot(tag):
            def_e.attrib[list(self.__rootattribs__.keys())[1]]=self.__rootattribs__[list(self.__rootattribs__.keys())[1]]
            #for key in self.__rootattribs__.keys()[:2]:
            #    def_e.attrib[key]=self.__rootattribs__[key]
        
        retree=VTree(def_e,self,None,False)
        if (level==0): self.__def_cache__[tag]=retree
        
        return retree.clone()
    
    def header(self):
        '''return the xml header'''
        return self.__header__
    def root(self):
        '''return the xml root'''
        return self.__root__
    def setroot(self,tag):
        '''set the xml root'''
        if tag in self.__tags__:
            self.__root__=tag
            return True
        else: 
            raise NameError('This tag, ' + tag + ' is not in the schema')
            return False
    def dump(self):
        '''dump the object to an xml string'''
        print(self.xml())
    def xml(self):
        '''dump the object to an xml string'''
        return self.__header__+'\n'+__ElementTree__.tostring(self.__tree__.getroot()).replace(
            'ns0:',self.__ns__).replace(
            ':ns0',':'+self.__ns__.rstrip(':')
            ).replace(
                      self.__schemafile_long__,self.__schemafile_short__)
    def types(self):
        '''list of defined types'''
        return set(self.__types__)
    def attribs(self):
        '''list of defined attributes'''
        return set(self.__attribs__)
    def tags(self):
        '''list of defined tags'''
        return set(self.__tags__)
    def Tag_isRoot(self,tag):
        '''return true if the tag is the xml root'''
        return tag==self.__root__
    def Tag_constraints(self, tag):
        '''print schema constraints for tag'''
        if (tag not in self.__tags__ and
            tag not in self.__attribs__ and
            tag not in self.__types__):
            return {}
        dets={}
        dets['ValueTypes']=self.Tag_valueTypes(tag)
        dets['Enumeration']=self.Tag_hasEnumeration(tag)
        dets['Fixed']=self.Tag_fixed(tag)
        dets['Default']=self.Tag_default(tag)
        dets['Atts']=self.Tag_attribs(tag)
        kiddic={}
        for c in self.Tag_children(tag):
            kiddic[c]=self.Tag_nChild(tag,c)
        dets['Children']=kiddic
        return dets

    #helper functions to check the schema objects

    def Tag_canHaveValue(self, tag,val):
        '''can I set this value to the tag?'''
        try:
            self.Tag_castValue(tag,val)
            return True
        except (ValueError, TypeError):
            return False
        

    def __cast_from_tag__(self, tag, val):
        '''internal method for casting directly'''
        if tag in self.__rootattribs__:
            try:
                return str(val)
            except ValueError:
                raise ValueError('This root tag '+tag+' must be a string ')
                return None
        if (tag not in self.__tags__ and
            tag not in self.__attribs__ and
            tag not in self.__types__):
            raise NameError('This tag '+tag+' does not exist in the schema ')
            return None
        
        types=self.Tag_valueTypes(tag)
        if types is None:
            raise TypeError('No types defined for '+tag)
            #print 'no types found!'
            return None

        ret=None
        if self.Tag_isList(tag):
            #print 'recognised list'
            if type(types)==type([]):
                types=types[0]
            nval=[]
            #print 'casting', val, 'type', str(type(val))
            if type('')==type(val):
                #print 'splitting'
                nval=val.split(' ')
            elif type(val)==type([]):
                nval=val
            else: nval=[val]
            #print 'casting', val 
            ret=[]
            for aval in nval:
                try:
                    ret.append(self.__cast__(types,aval))
                except ValueError: 
                    raise ValueError('This list does not accept '+str(type(aval))+' values')
        
        elif self.Tag_isUnion(tag):
            #print 'recognised union'
            if types!=str(type([])):
                types=[types]
            for atype in types:
                try:
                    r1=self.__cast__(atype,aval)
                    ret=r1
                    break
                except ValueError:
                    continue
        else:
            #print 'basic type'
            if type(types)==type([]):
                types=types[0]
            ret=self.__cast__(types,val)
        
        if ret is None:
            raise ValueError('This tag does not accept '+str(type(aval))+' values')
        return ret
    
    def Tag_castValue(self, tag,val):
        '''can I set this value to the tag?'''
        ret=self.__cast_from_tag__(tag,val)
        
        f=self.Tag_fixed(tag)
        if f is not None:
            #print 'recognised fixed'
            if ret != f:
                raise ValueError('This value is fixed, and not equal to this one '+str(ret))
        e=self.Tag_enumeration(tag)
        if e is not None and len(e)>0:
            #print 'recognised enum'
            if ret not in e:
                raise ValueError('This value is not one of the enumerated list '+str(ret))
            
        if ret is None:
            raise ValueError('This tag '+tag+' does not accept '+str(type(val))+' values')
        else:
            return ret
    
    
    def Tag_isSequence(self, tag):
        '''does this tag define a sequence?'''
        if tag in self.__seq_cache__:
            return self.__seq_cache__[tag]
        ise=self.__compound__(tag,'sequence')
        self.__seq_cache__[tag]=ise
        return ise
    
    def Tag_isList(self, tag):
        '''does this tag take a list?'''
        if tag in self.__list_cache__:
            return self.__list_cache__[tag]
        ile=self.__compound__(tag,'list')
        self.__list_cache__[tag]=ile
        return ile
    
    
    def Tag_isUnion(self, tag):
        '''does this tag take a union?'''
        if tag in self.__union_cache__:
            return self.__union_cache__[tag]
        iu=self.__compound__(tag,'union')
        self.__union_cache__[tag]=iu
        return iu
    
    def Tag_hasDefault(self,tag):
        '''is there a default value for this tag/attribute?'''
        return self.__hasConstraint__(tag,'default')
        
    def Tag_default(self, tag):
        '''get the default value for this tag/attribute'''
        if tag in self.__default_cache__:
            return self.__default_cache__[tag]
        defa=self.__default__(tag)
        if defa is None:
            self.__default_cache__[tag]=None
            return None
        defa=self.Tag_castValue(tag,defa)
        self.__default_cache__[tag]=defa
        return defa
    
    def Tag_isFixed(self, tag):
        '''is there a fixed value for this tag/attribute?'''
        if tag in self.__fixed_cache__:
            if self.__fixed_cache__[tag] is not None:
                return True
            return False
        return self.__hasConstraint__(tag,'fixed')

    def Tag_fixed(self, tag):
        '''what is the fixed value for this tag/attribute?'''
        if tag in self.__fixed_cache__:
            return self.__fixed_cache__[tag]
        fix=self.__fixed__(tag)
        if fix is None:
            self.__fixed_cache__[tag]=None
            return None
        fix=self.Tag_castValue(tag,fix)
        self.__fixed_cache__[tag]=fix
        return fix
    
    def Tag_hasEnumeration(self, tag):
        '''are there Enumerated values for this tag/attribute?'''
        if tag in self.__enum_cache__:
            if self.__enum_cache__[tag] is not None:
                if len(self.__enum_cache__[tag]) > 0:
                    return True
            return False
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                return False
            raise NameError(str(tag)+' tag is not in the schema') 
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'enumeration'):
                return True
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.Tag_hasEnumeration(atype)
        return False
    
    def Tag_enumeration(self, tag):
        '''return the Enumerated values for this tag/attribute?'''
        if tag in self.__enum_cache__:
            return self.__enum_cache__[tag]
        enum=self.__enumeration__(tag)
        if enum is None:
            self.__enum_cache__[tag]=None
            return None
        enums=[]
        for e in enum:
            enums.append(self.__cast_from_tag__(tag,e))
        self.__enum_cache__[tag]=enums
        return enums
    
    def Tag_valueTypes(self,tag):
        '''what are the allowed value types for this tag/attribute?'''
        if tag in self.__type_cache__:
            return self.__type_cache__[tag]
##         if self.Tag_isSequence(tag):
##             self.__type_cache__[tag]=None
##             return None
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                self.__type_cache__[tag]=tag
                return tag
            raise NameError(str(tag)+' tag is not in the schema' )
        atype=self.__etype__(ele)
        if str(type([])) in str(type(atype)):
            #print 'a list or so'
            valuetypes=[]
            for aatype in atype:
                valuetypes.append(self.Tag_valueTypes(aatype))
            self.__type_cache__[tag]=valuetypes
            return valuetypes
        if atype in self.__basetypes__:
            self.__type_cache__[tag]=atype
            return atype
        for ttype in self.__types__:
            if atype==ttype:#ele.attrib['type']==ttype:
                #print 'ready to go to', ttype
                nt=self.Tag_valueTypes(atype)
                self.__type_cache__[tag]=nt
                return nt
        self.__type_cache__[tag]=None
        return None
    
    def Tag_isAttribRequired(self,tag,att):
        '''is this attribute required?'''
        try:
            if self.Tag_isRoot(tag) and att in self.__rootattribs__:
                return( list(att==self.__rootattribs__.keys())[0])
            
            ele=self.__attribelement__[att]
            if ele.attrib['use']=='required':
                return True
        except KeyError: pass
        
        ele=self.__getele__(tag,True,False,True)
        if ele is None:
            if tag in self.__basetypes__:
                return False
            raise NameError(str(tag)+' tag is not in the schema')
        #print 'looking for tag'
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'attribute'):
                for find in ['name','ref']:
                    try:
                        if ec.attrib[find]==att:
                            if ec.attrib['use']=='required':
                                return True
                    except KeyError: pass
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.Tag_isAttribRequired(atype,att)
        return False
    
    def Tag_whitespace(self,tag):
        '''how is whitespace handled for this object?'''
        if tag in self.__whitespace_cache__:
            return self.__whitespace_cache__[tag]
        ele=self.__getele__(tag)
        if ele is None:
            if tag in self.__basetypes__:
                self.__whitespace_cache__[tag]=None
                return None
            raise NameError(str(tag)+' tag is not in the schema')
        #print 'looking for tag'
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'whiteSpace'):
                #print 'iterating'
                try:
                    self.__whitespace_cache__[tag]=ec.attrib['value']
                    return ec.attrib['value']
                except KeyError: pass
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                aw=self.Tag_whitespace(atype)
                self.__whitespace_cache__[tag]=aw
                return aw
        self.__whitespace_cache__[tag]=None
        return None
    
    def Tag_hasAttrib(self,tag,att):
        '''is this an attribute of the tag?'''
        if self.Tag_isRoot(tag) and att in self.__rootattribs__:
            return True
        if tag in self.__attrib_cache__:
            return (att in self.__attrib_cache__[tag])
        if att not in self.__attribs__:
            return False
        
        ele=self.__getele__(tag,True,False,True)
        if ele is None:
            if tag in self.__basetypes__:
                return False
            raise NameError(str(tag)+' tag is not in the schema' )
        
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'attribute'):
                for find in ['name','ref']:
                    #print 'iterating'
                    try:
                        if ec.attrib[find]==att:
                            return True
                    except KeyError: pass
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.Tag_hasAttrib(atype,att)
        
        return False
    
    def Tag_attribs(self,tag):
        '''list all attributes for the tag'''
        if tag in self.__attrib_cache__:
            return self.__attrib_cache__[tag]
        
        ele=self.__getele__(tag,True,False,True)
        if ele is None:
            if tag in self.__basetypes__:
                self.__attrib_cache__[tag]=None
                return None
            raise NameError(str(tag)+' tag is not in the schema' )
        #print 'looking for tag'
        
        atts=[]
        #if self.Tag_isRoot(tag):
        #    atts=self.__rootattribs__.keys
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'attribute'):
                for find in ['name','ref']:
                    try:
                        atts.append( ec.attrib[find])
                        #print 'appended', atts
                    except KeyError: pass
        for atype in self.__types__:
            #print 'looking at daughter types'
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                #print 'found daughter types'
                atts+=self.Tag_attribs(atype)
                #print 'appended', atts
        self.__attrib_cache__[tag]=atts
        return atts
    
    def Tag_children(self, tag):
        '''list all possible children for the tag'''
        if tag in self.__child_cache__:
            return self.__child_cache__[tag]

        #if not a sequence there are no children
        if not self.Tag_isSequence(tag):
            self.__child_cache__[tag]=[]
            return []
        #retreive the element based on the tag
        ele=self.__getele__(tag,True,False,True)
        if ele is None:
            self.__child_cache__[tag]=[]
            return []
        child=[]
        #I iterate over everything looking for elements
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'element'):
                if 'name' in ec.attrib:
                    if ec.attrib['name']==tag:
                        continue
                    child.append(ec.attrib['name'])
            if len(child):
                self.__child_cache__[tag]=child
                return child
        #then I go to the type if the tag doesn't define it
        et=self.__etype__(ele)
        if et in self.__types__:
            return self.Tag_children(et)
        
        self.__child_cache__[tag]=child
        return child
        
    def Tag_mothers(self, tag):
        '''list all possible mothers for the tag'''
        if tag in self.__mother_cache__:
            return self.__mother_cache__[tag]
        mothers=[]
        if self.Tag_isRoot(tag): return None
        if tag not in self.__tags__:
            raise NameError(str(tag)+' is not a valid tag in the schema')
            return None
        for mother in self.__tags__:
            if tag in self.Tag_children(mother):
                mothers.append(mother)
        self.__mother_cache__[tag]=mothers
        return mothers
    
    def Tag_nChild(self, tag, child):
        '''give the [min,max] number of children of this type for this tag'''
        if not self.Tag_isSequence(tag):
            return [0,0]
        if child not in self.__tags__:
            return [0,0]
        ele=self.__getele__(tag,True,False,True)
        if ele is None:
            raise NameError(str(tag)+' tag is not allowed children' )
        for pref in [self.__ns__,self.__uri__]:
            for ec in ele.getiterator(pref+'element'):
                try:
                    if ec.attrib['name']==child:
                        #print 'found child'
                        min=1;
                        max=1;
                        try: 
                            if ec.attrib['minOccurs']=='unbounded':
                                min=-1
                            else:
                                min=int(ec.attrib['minOccurs'])
                        except (KeyError,ValueError): pass
                        try:
                            if ec.attrib['maxOccurs']=='unbounded':
                                max=-1
                            else:
                                max=int(ec.attrib['maxOccurs'])
                        except (KeyError,ValueError): pass
                        return [ min, max]
                except KeyError: continue
        for atype in self.__types__:
            if self.__etype__(ele)==atype:#ele.attrib['type']==atype:
                return self.Tag_nChild(atype,child)
        return [0,0]
        
