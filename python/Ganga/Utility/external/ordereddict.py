# 05-07-03
# v1.0.3

# ordereddict.py
# A class that is a drop in replacement for an ordinary dictionary.
# methods that would normally return values/items in a random manner return them
# in an ordered and consistent manner.

# Copyright Michael Foord
# Not for use in commercial projects without permission. (Although permission will probably be given).
# If you use this code in a project then please credit me and include a link back.
# If you release the project then let me know (and include this message
# with my code !)

# No warranty express or implied for the accuracy, fitness to purpose or otherwise for this code....
# Use at your own risk !!!

# E-mail or michael AT foord DOT me DOT uk
# Maintained at www.voidspace.org.uk/atlantibots/pythonutils.html

import sys          # purely used for the version_info

##########################################################################


class dIter(object):

    """Implements a basic dictionary iterator with 3 modes.
    If mode=0 (default) returns the keys (by returns I mean iterates over !)
    If mode=1 returns the values
    If mode=-1 returns the items - (key, value) tuple
    mode=0 equates to the iterkeys method or __iter__ (for entry in dict)
    mode=1 equates to the itervalues method.
    mode=-1 equates to the iteritems method.
    """

    def __init__(self, indict, mode=0):
        self.thedict = indict
        self.inseq = indict.keys()
        self.index = 0
        self.mode = mode

    def next(self):
        if self.index >= len(self.inseq):
            raise StopIteration
        thekey = self.inseq[self.index]
        self.index += 1
        if not self.mode:
            return thekey
        elif self.mode == 1:
            return self.thedict[thekey]
        elif self.mode == -1:
            return (thekey, self.thedict[thekey])

    def __iter__(self):
        return self

##########################################################################


class oDict(object):

    """An ordered dictionary. ordereddict = oDict(indict, order)"""
    __doc__ = """ordereddict = oDict({'a' : 1, 'b' : 2}, True)
The dictionary can be initialised with an optional dictionary passed in as the first argument,
You can also pass in an order parameter which chooses the sort method.
order=True (default) means all ordered methods use the normal sort function.
order=False  means all ordered methods use the reverse sort function.
order=None means no sort function.

keys, items, iter and pop methods are ordered - based on the key.
The ordering is implemented in the keys() function.
The iterators are returned using the custom iterator dIter (which will work in three different ways)."""

    def __init__(self, indict={}, order=True):
        self._thedict = {}
        self._thedict.update(indict)
        self._order = order

    def __setitem__(self, item, value):
        """Setting a keyword"""
        self._thedict[item] = value

    def __getitem__(self, item):
        """Fetching a value."""
        return self._thedict[item]

    def __delitem__(self, item):
        """Deleting a keyword"""
        del self._thedict[item]

    def pop(self, item=[], default=None):
        """Emulates the pop method.
        If item is not supplied it pops the first value in the dictionary.
        This is different from the normal dict pop method."""
        if item != []:
            return self._thedict.pop(item, default)
        else:
            try:
                return self._thedict.pop(self.keys()[0])
            except IndexError:
                raise KeyError(': \'pop(): dictionary is empty\'')

    def popitem(self):
        """Emulates the popitem method - pops the first one in the list based on the chosen sort method."""
        try:
            theitem = self.keys()[0]
        except IndexError:
            raise KeyError(': \'popitem(): dictionary is empty\'')
        return (theitem, self._thedict.pop(theitem))

    def has_key(self, item):
        """Does the dictionary have this key."""
        return item in self._thedict           # does the key exist

    def __contains__(self, item):
        """Does the dictionary have this key."""
        return item in self._thedict           # does the key exist

    def setdefault(self, item, default=None):
        """Fetch an item if it exists, otherwise set the item to default and return default."""
        return self._thedict.setdefault(item, default)

    def get(self, item, default=None):
        """Fetch the item if it exists, otherwise return default."""
        return self._thedict.get(item, default)

    def update(self, indict):
        """Update the current oDdict with the dictionary supplied."""
        self._thedict.update(indict)

    def copy(self):
        """Create a new oDict object that is a copy of this one."""
        return oDict(self._thedict)

    def dict(self):
        """Create a dictionary version of this oDict."""
        return dict.copy(self._thedict)

    def clear(self):
        """Clear oDict."""
        self._thedict.clear()

    def __repr__(self):
        """An oDict version of __repr__ """
        return 'oDict(' + self._thedict.__repr__() + ')'

    def keys(self):
        """Return an ordered list of the keys of this oDict."""
        thelist = self._thedict.keys()
        if self._order == True:
            thelist.sort()
        elif self._order == False:
            thelist.sort()
            thelist.reverse()
        return thelist

    def items(self):
        """Like keys() but returns a list of (key, value)"""
        return [(key, self._thedict[key]) for key in self.keys()]

    def values(self):
        """Like keys() but returns an ordered list of values (ordered by key)"""
        return [self._thedict[key] for key in self.keys()]

    def fromkeys(cls, *args):
        """Return a new oDict initialised from the values supplied.
        If sys.version_info > 2.2 this becomes a classmethod."""
        return oDict(*args)

    if (sys.version_info[0] + sys.version_info[1] / 10.0) >= 2.2:
        fromkeys = classmethod(fromkeys)

    def __len__(self):
        return len(self._thedict)

    def __cmp__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return cmp(self._thedict, other)

    def __eq__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__eq__(other)

    def __ne__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__ne__(other)

    def __gt__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__gt__(other)

    def __ge__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__ge__(other)

    def __lt__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__lt__(other)

    def __le__(self, other):
        if hasattr(other, '_thedict'):
            other = other._thedict
        return self._thedict.__le__(other)

    def __hash__(self):
        """This just raises a TypeError."""
        self._thedict.__hash__()

    def __iter__(self):
        """Return an ordered iterator for the oDict."""
        return dIter(self)

    def iteritems(self):
        """Return an ordered iterator over the the oDict - returning (key, value) tuples."""
        return dIter(self, -1)

    def iterkeys(self):
        """Return an ordered iterator over the keys the oDict."""
        return dIter(self)

    def itervalues(self):
        """Return an ordered iterator over the the values of the oDict - ordered by key."""
        return dIter(self, 1)

    def __str__(self):
        """An oDict version of __str__ """
        return 'oDict(' + self._thedict.__str__() + ')'

##########################################################################

if __name__ == '__main__':
    dictmethods = ['__class__', '__cmp__', '__contains__', '__delattr__', '__delitem__', '__doc__', '__eq__', '__ge__', '__getattribute__', '__getitem__', '__gt__', '__hash__', '__init__', '__iter__', '__le__', '__len__', '__lt__', '__ne__', '__new__',
                   '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__setitem__', '__str__', 'clear', 'copy', 'fromkeys', 'get', 'has_key', 'items', 'iteritems', 'iterkeys', 'itervalues', 'keys', 'pop', 'popitem', 'setdefault', 'update', 'values']
    odict = oDict({'x': 'a', 'y': 'b', 'z': 'c'})
    print 'print oDict.__doc__ \n', oDict.__doc__
    print
    print
    print 'Attribute Test.\nTesting against the full attribute list for a normal dictionary. (40 attributes)'
    for entry in dictmethods:
        if not hasattr(odict, entry):
            print 'oDict doesn\'t have attribute \'%s\'' % entry
    print 'See the docs as to why those attributes are missing !!'
    print 'Method test.\nIf nothing prints below this then all the tests passed !\n'
    dlist = []
    for key in odict.iterkeys():
        dlist.append(key)
    if dlist != ['x', 'y', 'z']:
        print 'Order fail in iterkeys method.'

    dlist = []
    for value in odict.itervalues():
        dlist.append(value)
    if dlist != ['a', 'b', 'c']:
        print 'Order fail in itervalues method.'

    dlist = []
    for item in odict.iteritems():
        dlist.append(item)
    if dlist != [('x', 'a'), ('y', 'b'), ('z', 'c')]:
        print 'Order fail in iteritems method.'

    if not odict.keys() == ['x', 'y', 'z']:
        print 'Order fail in keys method.'
    if not odict.values() == ['a', 'b', 'c']:
        print 'Order fail in values method.'
    if not odict.items() == [('x', 'a'), ('y', 'b'), ('z', 'c')]:
        print 'Order fail in items method.'
    dlist = []
    while odict:
        dlist.append(odict.pop())
        if len(dlist) > 4:
            print 'Fail in pop to remove items'
            break
    if dlist != ['a', 'b', 'c']:
        print 'Order fail in pop method.'
    if not odict.fromkeys({'test': 'z', 'fish': 4}, False) == oDict({'test': 'z', 'fish': 4}, False):
        print 'Odd behaviour in fromkeys method.'


"""
oDict is an ordered dictionary.
It behaves as a drop in replacement for an ordinary dictionary in almost every circumstance.
Many dictionary methods which normally return a random value, or return values in a randomn order.
Those methods in oDict return values in an ordered and consistent manner.
The ordering is applied in the keys() method and uses the Python sort() method of lists to do the sorting.
You can additionally set it to apply the reverse method by passing in a parameter when you create the instance.
See the oDict docstring for more details.

An ordered dictinary is useful where, for example, a consistent return order for the iterators and pop methods is helpful.
I use it in FSDM markup structures (describing files and directories in a file structure) so that the markup files are built in a consistent order.

Methods which are now ordered are :

pop, popitem, keys, items, values
iteritems, iterkeys, itervalues, __iter__ ( for key in odict )


As oDict has methods defined for almost all the dictionary methods, and also has custom iterators,
it would be a good template for anyone else who wanted to create a new dictionary type with custom access methods etc.

Doesn't subclass dict or use the iter function, so I think might be compatible with versions of Python pre 2.2 ?

Extra Methods, Not in a Normal dictionary :
'dict'

'pop' is slightly different to the normal dictionary method, it can be used without a parameter.
'str' and '__repr__' are modified to indicate this is an oDict rather than just a dictionary.

A lot of the methods that would return new dictionaries (copy, fromkeys)  return new oDicts (hence the new dict method which returns an ordinary dictionary copy of the oDIct)

'Not Implemented Yet' Methods Include :
'__class__'                                         : The default is fine.
'__getattribute__', '__setattr__', '__delattr__'    : What the heck are these used for in a dict, probably raise errors. The standard 'classic' methods will be fine for us   
'__new__'                                           : not a new style class so don't need it
'__reduce__', '__reduce_ex__',                      : To do with pickling, I don't understand, hopefully Python can do something sensible anyway

The only time oDict won't act as a replacement for a dict is if the isinstance test is used.
In Python 2.2 you can fix this by making oDict a subclass of dict.

We could make oDict a bit more lightweight by overloading getattr.
Several methods that just call the same method on the underlying _thedict could all be replaced by one method that called getattr.
As one of the aims of oDict was to create a full dictionary like object, with all the methods defined, I won't do this. (__getitem__ and __setitem__ are two methods we could do away with).


TODO/ISSUES


CHANGELOG

05-07-04        Version 1.0.3
Fixed a bug in get.
Use some slightly more pythonic hasattr tests rather than isinstance.
Got rid of the slightly odd dum class.
Got rid of my suggested, nonsensical, __class__ stuff.
Changed the tests to use keys that would naturally be unordered !

20-06-04        Version 1.0.2
Slight change to the dum class, to give it a __cmp__ method.

17-06-04        Version 1.0.1
clear method is slightly better. (clears dictionary rather than rebinds)
Sorted out a few index errors where empty dictionaries are used. (raise KeyError rather than IndexError)
Made fromkeys a classmethod where Python Version > 2.2

16-06-04        Version 1.0.0
First version, appears to work fine.

"""
