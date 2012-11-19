from GangaTest.Framework.tests import GangaGPITestCase
#from unittest import TestCase
from xml.dom import minidom

from GangaRobot.Lib.Base.Extract import Node

def equal(node1, node2):
    """Return True if node1 equals node2, otherwise False.
    
    Handles type Node by comparing result of toxml().
    Handles list types by comparing elements.
    Handles other types by == comparison.
    
    """
    if isinstance(node1, list) and isinstance(node2, list):
        if len(node1) != len(node2):
            return False
        else:
            for n1, n2 in zip(node1, node2):
                if not equal(n1, n2):
                    return False
    if isinstance(node1, Node) and isinstance(node2, Node):
        return node1.toxml() == node2.toxml()
    else:
        return node1 == node2


class TestExtract(GangaGPITestCase):

    """Tests of extracted data object model.
    
    N.B. This test would work as a standard TestCase.
    """
    
    def setUp(self):
        """Create test extract data."""
        super(TestExtract, self).setUp()
        #testnode
        run = Node('run')
        run_core = run.addnode('core')
        run_core.addnode('id', 'Run_Id')
        job = run.addnode('job')
        job_core = job.addnode('core')
        job_core.addnode('id', 1)
        job_core.addnode('name', 'Core_1')
        job_core.addnode('status', 'completed')
        job = run.addnode('job')
        job_core = job.addnode('core')
        job_core.addnode('id', 2)
        job_core.addnode('name', 'Core_2')
        job_core.addnode('status', 'failed')
        job = run.addnode('job')
        job_core = job.addnode('core')
        job_core.addnode('id', 3)
        job_core.addnode('name', 'Core_3')
        job_core.addnode('status', 'completed')
        self.testnode = run
        #testxml
        self.testxml = """<?xml version="1.0" ?><run><core><id>Run_Id</id></core><job><core><id>1</id><name>Core_1</name><status>completed</status></core></job><job><core><id>2</id><name>Core_2</name><status>failed</status></core></job><job><core><id>3</id><name>Core_3</name><status>completed</status></core></job></run>"""
        #testdom
        self.testdom = minidom.parseString(self.testxml)
    
    def tearDown(self):
        """Dereference test extract data."""
        self.testnode = None
        self.testxml = None
        self.testdom = None
        super(TestExtract, self).tearDown()
    
    def test_fromdom(self):
        """Test fromdom() creates a node from XML DOM."""
        expected = self.testnode
        actual = Node.fromdom(self.testdom)
        assert equal(expected, actual), 'node does not convert from dom as expected'

    def test_todom(self):
        """Test todom() writes a node to XML DOM."""
        expected = self.testdom.toxml()
        actual = self.testnode.todom().toxml()
        assert expected == actual, 'node does not convert to dom as expected'

    def test_fromxml(self):
        """Test fromxml() creates a node from XML string."""
        expected = self.testnode
        actual = Node.fromxml(self.testxml)
        assert equal(expected, actual), 'node does not convert from xml as expected'
        
    def test_toxml(self):
        """Test toxml() writes a node to XML string."""
        expected = self.testxml
        actual = self.testnode.toxml().replace('\n','')
        assert expected == actual, 'node does not convert to XML as expected'

    def test_toprettyxml(self):
        """Test toprettyxml() writes a node to pretty XML string."""
        expected = self.testdom.toprettyxml(indent='   ')
        actual = self.testnode.toprettyxml()
        assert expected == actual, 'node does not convert to pretty XML as expected'
        
    def test_constructor(self):
        """Test __init__() creates a node."""
        expected = Node.fromxml('<my-node/>')
        actual = Node('my-node')
        assert equal(expected, actual), 'created node is not as expected'
        
    def test_constructor_value(self):
        """Test __init__() creates a node with a value."""
        expected = Node.fromxml('<my-node>my-value</my-node>')
        actual = Node('my-node', 'my-value')
        assert equal(expected, actual), 'created node is not as expected'
    
    def test_addnode(self):
        """Test addnode() adds a node."""
        expected = Node.fromxml('<my-node><my-sub-node/></my-node>')
        actual = Node('my-node')
        actual.addnode('my-sub-node')
        assert equal(expected, actual), 'result of adding node is not as expected'
        
    def test_addnode_value(self):
        """Test addnode() adds a node with a value."""
        expected = Node.fromxml('<my-node><my-sub-node>my-value</my-sub-node></my-node>')
        actual = Node('my-node')
        actual.addnode('my-sub-node', 'my-value')
        assert equal(expected, actual), 'result of adding node is not as expected'

    def test_getnodes(self):
        """Test getnodes() returns list of current node."""
        expected = [self.testnode]
        actual = self.testnode.getnodes()
        assert str(expected) == str(actual), 'getnodes() does not return expected list of current node.'
        actual = self.testnode.getnodes('')
        assert equal(expected, actual), "getnodes('') does not return expected list of current node."

    def test_getnodes_name(self):
        """Test getnodes('job') returns list of job subnodes."""
        expected = [n for n in self.testnode.nodes if n.name == 'job'] #job subnodes
        actual = self.testnode.getnodes('job')
        assert equal(expected, actual), "getnodes('job') does not return expected list of job subnodes."

    def test_getnodes_wild(self):
        """Test getnodes('*') returns list of all subnodes."""
        expected = self.testnode.nodes #all subnodes
        actual = self.testnode.getnodes('*')
        assert equal(expected, actual), "getnodes('*') does not return expected list of all subnodes."

    def test_getnodes_path(self):
        """Test getnodes('job.core') returns list of core subnodes of job subnodes."""
        expected = []
        for jobnode in [n for n in self.testnode.nodes if n.name == 'job']: #job subnodes
            expected.extend([n for n in jobnode.nodes if n.name =='core']) #core subnodes
        actual = self.testnode.getnodes('job.core')
        assert equal(expected, actual), "getnodes('job.core') does not return expected list of core subnodes of job subnodes."

    def test_getnodes_path_wild(self):
        """Test getnodes('job.core.*') returns list of all subnodes of job.core subnodes."""
        expected = []
        for jobnode in [n for n in self.testnode.nodes if n.name == 'job']: #job subnodes
            for corenode in [n for n in jobnode.nodes if n.name =='core']: #core subnodes
                expected.extend(corenode.nodes) #all subnodes
        actual = self.testnode.getnodes('job.core.*')
        assert equal(expected, actual), "getnodes('job.core.*') does not return expected list of all subnodes of job.core subnodes."

    def test_getnodes_path_wild_path(self):
        """Test getnodes('job.*.id') returns list of id subnodes of job.* subnodes."""
        expected = []
        for jobnode in [n for n in self.testnode.nodes if n.name == 'job']: #job subnodes
            for subnode in jobnode.nodes: #all subnodes
                expected.extend([n for n in subnode.nodes if n.name == 'id']) #id subnodes
        actual = self.testnode.getnodes('job.*.id')
        assert equal(expected, actual), "getnodes('job.*.id') does not return expected list of id subnodes of job.* subnodes."

    def test_getnodes_wild_path(self):
        """Test getnodes('*.core.id') returns list of id subnodes of job.* subnodes."""
        expected = []
        for subnode in self.testnode.nodes: #all subnodes
            for corenode in [n for n in subnode.nodes if n.name =='core']: #core subnodes
                expected.extend([n for n in corenode.nodes if n.name == 'id']) #id subnodes
        actual = self.testnode.getnodes('*.core.id')
        assert equal(expected, actual), "getnodes('*.core.id') does not return expected list of id subnodes of *.core subnodes."

    def test_getnodes_wild_wild(self):
        """Test getnodes('*.*') returns list of all subnodes of all subnodes."""
        expected = []
        for subnode in self.testnode.nodes: #all subnodes
            expected.extend(subnode.nodes)#all subnodes
        actual = self.testnode.getnodes('*.*')
        assert equal(expected, actual), "getnodes('*.*') does not return expected list of all subnodes of all subnodes."

    def test_getnode(self):
        """Test getnode() returns first node of getnodes()."""
        expected = self.testnode.getnodes('job.core.id')[0]
        actual = self.testnode.getnode('job.core.id')
        assert equal(expected, actual), "getnode('job.core.id') does not return first job.core.id subnode as expected."
        
    def test_getnode_unknown(self):
        """Test getnode() returns None for unknown name."""
        expected = None
        actual = self.testnode.getnode('xxx')
        assert expected == actual, "getnode('xxx') does not return None."

    def test_getvalues(self):
        """Test getvalues() returns values of getnodes()."""
        expected = [n.value for n in self.testnode.getnodes('job.core.id')]
        actual = self.testnode.getvalues('job.core.id')
        assert expected == actual, "getvalues('job.core.id') does not return job.core.id subnode values as expected."

    def test_getvalue(self):
        """Test getvalue() returns first value of getvalues()."""
        expected = self.testnode.getvalues('job.core.id')[0]
        actual = self.testnode.getvalue('job.core.id')
        assert expected == actual, "getvalue('job.core.id') does not return first job.core.id subnode value as expected."

    def test_getvalue_unknown(self):
        """Test getvalue() returns None for unknown name."""
        expected = None
        actual = self.testnode.getvalue('xxx')
        assert expected == actual, "getvalue('xxx') does not return None."


        
