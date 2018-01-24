"""Object model for extract data as handled by base action implementations.

The Node class represents a node of extracted data containing a value and / or
subnodes.

"""

from xml.dom.minidom import getDOMImplementation, parseString


class Node(object):
    
    """A node of extracted data containing a value and / or subnodes.
    
    Methods are provided for creating a subtree of nodes, see addnode(), and for
    querying that subtree, see getnode(), getnodes(), getvalue() and
    getvalues().
    
    The value and subnodes can also be accessed directly via the value and nodes
    attributes, respectively.

    In addition methods are provided to convert to and from XML DOMs and
    strings, see todom(), fromdom(), toxml(), fromxml() and toprettyxml().
    
    For simplicity, Node supports only a subset of XML; essentially it does not
    support attributes.
    
    Example representation of Node in XML (referenced in method documentation):
    <run>
        <core>
            <id>2007-06-22_13.17.51</id>
            <start-time>2007/06/22 13:17:51</start-time>
            <extract-time>2007/06/22 13:18:55</extract-time>
        </core>
        <job>
            <core>
                <id>4</id>
                <name>Core_2007-06-22_13.17.51_0</name>
                <status>completed</status>
                <application>Executable</application>
                <backend>Local</backend>
                <backend-id>19253</backend-id>
                <backend-actualCE>lx09.hep.ph.ic.ac.uk</backend-actualCE>
            </core>
        </job>
        <job>
            <core>
                <id>5</id>
                <name>Core_2007-06-22_13.17.51_1</name>
                <status>completed</status>
                <application>Executable</application>
                <backend>Local</backend>
                <backend-id>19267</backend-id>
                <backend-actualCE>lx09.hep.ph.ic.ac.uk</backend-actualCE>
            </core>
        </job>
    </run>
    
    """
    
    def __init__(self, name, value = None):
        """Create a new node with the given name and value.

        Keyword arguments:
        name -- The node name.
        value -- Optional value, which is converted to a string using str() if
            not None.

        """
        self.name = name
        self.value = None
        if value != None:
            self.value = str(value)
        self.nodes = []

    def addnode(self, name, value = None):
        """Add a new node with the given name and value, returning the new node.
        
        Keyword arguments:
        name -- The node name.
        value -- Optional value, which is converted to a string using str() if
            not None.
        
        Example - creating a node tree (using example in class documentation):

        run = Node('run')
        run_core = run.addnode('core')
        run_core.addnode('id', '2007-06-22_13.17.51')
        run_core.addnode('start-time', '2007/06/22 13:17:51')
        run_core.addnode('extract-time', '2007/06/22 13:18:55')
        job = run.addnode('job')
        job_core = job.addnode('core')
        job_core.addnode('id', 4)
        job_core.addnode('name', 'Core_2007-06-22_13.17.51_0')
        job_core.addnode('status', 'completed')
        ...

        """
        subnode = Node(name, value)
        self.nodes.append(subnode)
        return subnode
    
    def getnode(self, path = '', sep = '.', wild='*'):
        """Return the first node matching the given path, or None if no matches.

        Keyword arguments:
        path -- Optional path to node.
        sep -- Optional path separator, normally left as default.
        wild -- Optional wildcard, normally left as default.

        This method effectively returns the first element of the list returned
        by getnodes(), or None if the list is empty. See getnodes() for path
        usage and examples.

        """
        selected = self.getnodes(path, 1, sep, wild)
        if selected:
            return selected[0]
        else:
            return None

    def getnodes(self, path = '', maxresults = -1, sep = '.', wild='*'):
        """Return a list of all nodes matching the given path, or an empty list if no matches.
        
        Keyword arguments:
        path -- Optional path to node.
        maxresults -- Optional maximum number of results, non-positive indicates
            no limit.
        sep -- Optional path separator, normally left as default.
        wild -- Optional wildcard, normally left as default.
        
        The path is, by default, '.' separated with '*' wildcard. The returned
        list is in recursive order of the subnodes; e.g. the path 'job.core'
        returns a list of all 'core' subnodes of the first 'job' subnode
        followed by all 'core' subnodes of the second 'job' subnode, etc. See
        examples below for more details.
        
        The returned list is newly created and so modifications to the list
        itself do not modify the structure of the tree. However, the elements of
        the list reference the nodes of the tree and so modifications to the
        elements of the list do modify the tree.
        
        Examples - querying a node tree (using example in class documentation):

        #return list of current node only, (not commonly used).
        run.getnodes() or run.getnodes('')
        
        #return list of direct 'job' subnodes.
        run.getnodes('job')
        
        #return list of all direct subnodes.
        run.getnodes('*')
        
        #return list of 'core' subnodes of 'job' subnodes.
        #i.e. effectively concatenates results of calling getnodes('core') on
        #each node returned by run.getnodes('job').
        run.getnodes('job.core') 

        #return list of 'id' subnodes of 'core' subnodes of 'job' subnodes.
        #i.e. effectively concatenates results of calling getnodes('id') on each
        #node returned by run.getnodes('job.core')
        run.getnodes('job.core.id') 

        #return list of all subnodes of 'job' subnodes.
        #i.e. effectively concatenates results of calling getnodes('*') on each
        #node returned by run.getnodes('job') 
        run.getnodes('job.*')
        
        #return list of all 'id' subnodes of all subnodes of 'job' subnodes. 
        #i.e. effectively concatenates results of calling getnodes('id') on each
        #node returned by run.getnodes('job.*') 
        run.getnodes('job.*.id')
        
        #return list of all 'job' subnodes where 'core.status' has value
        #'completed'.
        [n for n in runnode.getnodes('job') if n.getvalue('core.status') == 'completed']
        
        N.B. the empty string refers to the current node so the following paths
        are equivalent '', '.', '..', as are 'job', '.job', 'job.', etc.
        
        """
        selected = []
        names = path.split(sep)
        if names:
            if names[0] == '':
                subnodes = [self]
            elif names[0] == wild:
                subnodes = self.nodes[:]
            else:
                subnodes = [node for node in self.nodes if node.name == names[0]]
            if len(names) == 1:
                selected.extend(subnodes)
            else:
                subpath = sep.join(names[1:])
                for subnode in subnodes:
                    if maxresults > 0 and len(selected) >= maxresults:
                        break
                    selected.extend(subnode.getnodes(subpath))
        if maxresults > 0:
            return selected[:maxresults]
        else:
            return selected

    def getvalue(self, path = '', sep = '.', wild='*'):
        """Return the value of the first node matching the given path, or None if no matches.

        Keyword arguments:
        path -- Optional path to node.
        sep -- Optional path separator, normally left as default.
        wild -- Optional wildcard, normally left as default.

        This method effectively returns the value of the first element of the
        list returned by getnodes(), or None if the list is empty. See
        getnodes() for path usage and examples.

        """
        values = self.getvalues(path, 1, sep, wild)
        if values:
            return values[0]
        else:
            return None
    
    def getvalues(self, path = '', maxresults = -1, sep = '.', wild='*'):
        """Return a list of the values of all nodes matching the given path, or an empty list if no matches.

        Keyword arguments:
        path -- Optional path to node.
        maxresults -- Optional maximum number of results, non-positive indicates
            no limit.
        sep -- Optional path separator, normally left as default.
        wild -- Optional wildcard, normally left as default.

        This method effectively returns the values of the nodes in the list
        returned by getnodes(). The returned list can contain None, since the
        value of a node can be None. See getnodes() for path usage and examples.

        """
        return [node.value for node in self.getnodes(path, maxresults, sep, wild)]

    def __repr__(self):
        return '<Node %s:%s>' % (self.name, self.value)

    def todom(self):
        """Return an XML DOM representation of the node."""
        document = getDOMImplementation().createDocument(None, self.name, None)
        Node.__fillelement(document, document.documentElement, self)
        return document

    @staticmethod
    def __fillelement(document, element, node):
        if node.value:
            element.appendChild(document.createTextNode(node.value))
        if node.nodes:
            for subnode in node.nodes:
                subelement = document.createElement(subnode.name)
                element.appendChild(subelement)
                Node.__fillelement(document, subelement, subnode)

    @staticmethod
    def fromdom(dom):
        """Return a Node from the given XML DOM.
        
        Keyword attributes:
        dom -- The XML DOM.
        
        Only supported XML features are retained; i.e. attributes are ignored,
        multiple text nodes are stripped of white space and concatenated.
        
        """
        if dom.nodeType == dom.DOCUMENT_NODE:
            return Node.fromdom(dom.documentElement)
        elif dom.nodeType == dom.ELEMENT_NODE:
            node = Node(dom.nodeName)
            value = ''
            for subdom in dom.childNodes:
                if subdom.nodeType == subdom.TEXT_NODE:
                    value = value + subdom.data.strip()
                else:
                    node.nodes.append(Node.fromdom(subdom))
            if value:
                node.value = value
            return node           
        else:
            raise TypeError
    
    def toxml(self):
        """Return a flat XML string representation of the node."""
        return self.todom().toxml()
        
    @staticmethod
    def fromxml(xml):
        """Return a Node from the given XML string.
        
        Keyword attributes:
        xml -- The XML string.
        
        Only supported XML features are retained; i.e. attributes are ignored,
        text nodes are stripped of white space and concatenated.
        
        """
        dom = parseString(xml)
        return Node.fromdom(dom)
    
    def toprettyxml(self):
        """Return a pretty XML string representation of the node."""
        return self.todom().toprettyxml(indent='   ')

