"""
XPath utility functions.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import xml.dom

def find(context, path, maxresults=-1):
    """
    Finds all elements matching the path relative to the context. A subset of
    abbreviated xpath notation is supported.
    
    Supported:
    /    : root node
    .    : current node
    ..   : parent node
    *    : all child elements
    eg   : all child elements named 'eg'
    //eg : all descendant elements named 'eg'
    
    Unsupported:
    @      : attribute axis
    []     : filters
    text() : text node
    
    @param context: The context DOM node.
    @param path: The xpath.
    @param maxresults: The maximum number of results to return.
        Negative indicates no limit. Default -1.
    @return: A list of selected DOM nodes, or empty list if no matches.
    
    See doctests below for examples.
    
    >>> from xml.dom import minidom
    >>> x = '<a><b><c/></b><b><d><c/></d></b><e/></a>'
    >>> d = minidom.parseString(x)
    >>> rs = find(d, '/a/b');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    <b><d><c/></d></b>
    >>> rs = find(d, 'a/b');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    <b><d><c/></d></b>
    >>> rs = find(d, 'a/*');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    <b><d><c/></d></b>
    <e/>
    >>> rs = find(d, 'a/./e');
    >>> for r in rs: print r.toxml()
    <e/>
    >>> rs = find(d, 'a/b/c/..');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    >>> rs = find(d, 'a//c');
    >>> for r in rs: print r.toxml()
    <c/>
    <c/>
    >>> rs = find(d, 'a/b', maxresults=1);
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    >>> b = findnode(d, 'a/b');
    >>> rs = find(b, '/a/b');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    <b><d><c/></d></b>
    >>> rs = find(b, 'c');
    >>> for r in rs: print r.toxml()
    <c/>
    >>> rs = find(b, '*');
    >>> for r in rs: print r.toxml()
    <c/>
    >>> rs = find(b, './c');
    >>> for r in rs: print r.toxml()
    <c/>
    >>> rs = find(b, '../b');
    >>> for r in rs: print r.toxml()
    <b><c/></b>
    <b><d><c/></d></b>
    >>> rs = find(b, '..//c');
    >>> for r in rs: print r.toxml()
    <c/>
    <c/>
    """
    if path.startswith('/'):
        while context.parentNode:
            context = context.parentNode
        path = path[1:]
    return _find(context, path, maxresults)

def findnode(context, path):
    """
    Finds a single element matching the path relative to the context. A subset
    of abbreviated xpath notation is supported.
    
    @param context: The context DOM node.
    @param path: The xpath.
    @return The selected DOM node, or None if no matches.
    
    @see find for further details.
    """
    selected = find(context, path, 1)
    if len(selected) > 0:
        return selected[0]
    return None

def _children(context, name):
    """Internal children function used by _find."""
    childElements = [n for n in context.childNodes if n.nodeType == xml.dom.Node.ELEMENT_NODE]
    if name in (None, '', '*'):
        return childElements
    return [e for e in childElements if e.localName == name]

def _descendants(context, name):
    """Internal descendants function used by _find."""
    return context.getElementsByTagName(name)

def _find(context, path, maxresults=-1):
    """Internal find function used by find."""
    selected = []
    subnodes = []
    names = path.split('/')
    if names[0] == '.': # handle self
        subnodes = [context]
    elif names[0] == '..': # handle parent
        subnodes = [context.parentNode]
    elif names[0] == '': # handle //
        if len(names) > 1:
            names = names[1:]
            subnodes = _descendants(context, names[0])
    else: # handle * or <name>
        subnodes = _children(context, names[0])
    if subnodes:
        if len(names) == 1:
            selected.extend(subnodes)
        else:
            for subnode in subnodes:
                if maxresults > 0 and len(selected) >= maxresults:
                    break
                selected.extend(_find(subnode, '/'.join(names[1:]), maxresults))
    if maxresults > 0:
        return selected[:maxresults]
    else:
        return selected

def Evaluate(expr, contextNode):
    """Drop-in replacement for PyXML xpath.Evaluate function, with support for
    a subset of abbreviated xpath notation.
    
    N.B. PyXML is no longer maintained and is incompatible with Python 2.6 or
    later.
     
    If PyXML is available then its implementation is used, otherwise the find
    function defined in this module is used.
    
    @see http://pyxml.sourceforge.net/topics/ for details on PyXML
    @see find for supported xpath notation
    """
    try:
        from xml import xpath
        return xpath.Evaluate(expr, contextNode)
    except ImportError:
        return find(contextNode, expr)

if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
