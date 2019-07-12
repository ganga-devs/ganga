#
# Base concept taken from Yannick Loiseau
# http://yloiseau.net/articles/DesignPatterns/flyweight/
#

from __future__ import with_statement
from __future__ import absolute_import
import weakref
from copy import copy
import types
from collections import OrderedDict
from operator import itemgetter
import gc

# Thing to explicitly not try to flyweight
flyweight_ignored_keys = [
                          "_flyweight_cache",
                          "_flyweight_cb_func",
                          "_my_flyweight_cb_func"
                        ]
flyweight_ignored_types = (int, float, type(None), bool, str) + weakref.ProxyTypes


def proxify(value):
    """
        Wrap the value in a proxy shell if need be.
        Returns the object or the proxy object.
    """
    if type(value) is unicode:
        value = ProxyStr(value)

    elif type(value) is list:
        value = ProxyList(value)

    elif type(value) is tuple:
        value = ProxyTuple(value)

    elif type(value) is set:
        value = ProxySet(value)

    elif type(value) is dict:
        value = ProxyDict(value)

    return value


class CoW(object):

    """
        This class is a base class.
        Traditional FlyWeight pattern is useful for ensuring we
        do not duplicate memory. However, given  we also wanted
        to utilize Copy-On-Write principles for performance.
        Thus, we take the concept of FlyWeight pattern, and
        extend it by instrumenting getattr and setattr. The net
        result is that, transparent to the underlying python class object,
        every attribute is actually a pointer and memory is reduced in that
        manner. Also, I override the __copy__ method to copy the pointers
        into a new object instead of fully traversing get/set.
        This allows me to not have to implement
        custom copy methods per each class, and instead utilize the
        duck typing of python to make this all transparent.
    """

    # __slots__ = ['__weakref__','__cow_add_cb_pointer','_my_flyweight_cb_func','_flyweight_cache']

    # Using weakref to allow garbage collection
    # dict[<var_type>][__hash__]
    _flyweight_cache = {}

    def __init__(self, *args, **kwargs):
        # Contains hard ref for lambda functions
        # we are setting in other objects
        self._my_flyweight_cb_func = None
        # Contains refs to those functions that should
        # be notified when I copy update
        self._flyweight_cb_func = weakref.WeakSet()
        # print self._flyweight_cache
        super(CoW, self).__init__()

        # If we're initing from ourselves, copy over
        if len(args) >= 1 and type(args[0]) == type(self):
            obj = args[0]

            # Using setters here so we auto register
            # callbacks with things relevant
            for key, value in vars(obj).items():
                if key in flyweight_ignored_keys:
                    continue
                object.__setattr__(self, key, value)

    def __getitem__(self, key):
        # Check if we're the top level. If we are, raise exception
        getitem = super(type(self), self).__getitem__
        if getitem == self.__getitem__:
            raise TypeError("'{}' object does not support indexing"
                            .format(self.__class__.__name__))

        value = getitem(key)

        # If this was a single value, add a callback func
        if type(key) is not slice and issubclass(type(value), CoW):
            self.__cow_add_cb_pointer(value)

        return value

    def __setattr__(self, key, value):

        # Don't proxify our ignored keys
        if key not in flyweight_ignored_keys:
            value = proxify(value)

        # Non-flyweight classes
        if type(value) in flyweight_ignored_types or key \
                in flyweight_ignored_keys:
            return super(CoW, self).__setattr__(key, value)

        # Make sure the type is in our cache
        if type(value) not in self._flyweight_cache.keys():
            self._flyweight_cache[type(value)] = weakref.WeakValueDictionary()

        # If an equivalent object exists, use that
        value_hash = hash(value)
        if value_hash in self._flyweight_cache[type(value)].keys():
            return super(CoW, self).__setattr__(
                key, self._flyweight_cache[type(value)][value_hash])

        # No matching object exists, save off this one
        self._flyweight_cache[type(value)][value_hash] = value

        # Set the attr
        return super(CoW, self).__setattr__(key, value)

    def __getattribute__(self, key):
        # Grab the value
        value = super(CoW, self).__getattribute__(key)

        # Don't proxy our own stuff
        # If this is a function, don't try to copy it
        if key in flyweight_ignored_keys or type(value) in \
                [types.BuiltinFunctionType,
                 types.MethodType,
                 types.FunctionType]:
            return value

        # Writing callback value so we can be notified
        # if this object updates in place.
        if issubclass(type(value), CoW):
            self.__cow_add_cb_pointer(value)

        return value

    def __cow_add_cb_pointer(self, obj):
        # Register a callback with the object to let us know if it has updated.
        # Record it so it doesn't disappear
        self._my_flyweight_cb_func = lambda new_value: \
            self.__cow_update_object(obj, new_value)

        # Tell the object we're interested in it
        # Deciding to only use one cb function at a time.
        # Always set it at get time so we know which obj we're talking about
        obj._flyweight_cb_func.clear()
        obj._flyweight_cb_func.add(self._my_flyweight_cb_func)

    def __copy__(self):
        # Perform fast copy of attribute pointers in self.
        obj = type(self).__new__(type(self))
        CoW.__init__(obj, self)

        return obj

    def __deepcopy__(self):
        pass

    def __hash__(self):
        """
            Overriding hash function for CoW object.
        """

        # Hashing on the slots
        if hasattr(self, "__slots__"):
            return hash(tuple(self.__slots__))

        # If no slots, use vars
        return hash(tuple((x, y) for x, y in vars(self).items()
                    if x not in flyweight_ignored_keys))

    # Extending this for the sake of inheriting CoW
    def __setitem__(self, key, item):
        # Just-in-time copy
        my_copy = copy(self)

        # Set it
        super(type(my_copy), my_copy).__setitem__(key, item)

        # Notify anyone who cares
        for func in self._flyweight_cb_func:
            func(my_copy)

    def __cow_update_object(self, old, new):
        """
            Iterates through all attributes and items in current object,
            replacing any that have the id of the old object
            with the id of the new object.
        """

        # Not using __slots__
        if hasattr(self, "__dict__"):
            for key, value in vars(self).items():
                if value is old:
                    setattr(self, key, new)

        # Using __slots__
        else:
            for key in self.__slots__:
                if getattr(self, key) is old:
                    setattr(self, key, new)

        # If we expose items, update those too
        try:

            # Iterate over list
            if issubclass(type(self), list):
                for i, key in enumerate(self):
                    if key is old:
                        self[i] = new

            # Iterate over dict
            elif issubclass(type(self), dict):
                for key in self:
                    if self[key] is old:
                        self[key] = new

            # Iterate over set
            elif issubclass(type(self), set):
                if old in self:
                    self.remove(old)
                    self.add(new)

        except Exception as e:
            pass


def get_true_reference_count(obj):
    """
        Returns the true refernce count (that we're interested in)
        for the object.
        Useful in decision making of mod in place or not.
    """

    count = 0
    for ref in gc.get_referrers(obj):
        if type(ref) is dict and "_my_flyweight_cb_func" in ref:
            count += 1

    return count


def list_do_generic_call(self, method_name, *args, **kwargs):
    """
        Runs a call that we know ahead of time will
        modify the state of this object. i.e.: list.clear()
    """

    # If only one thing is watching, we don't need to copy
    copy_required = False if get_true_reference_count(self) == 1 else True

    if copy_required:
        # Just-in-time copy
        my_copy = copy(self)
        my_type = type(my_copy)
    else:
        my_copy = self
        my_type = type(self)
        my_old_hash = hash(self)

    # HACK: need to figure out a proper sloution for this...
    # Tuple __iadd__ (and likely others) causes infinite recursion.
    if my_type is ProxyTuple:
        my_type = ProxyList

    # Proxify the args if we need to
    args = [proxify(arg) for arg in args]
    kwargs = dict((key, proxify(val)) for key, val in kwargs.items())

    # Run this call in-place
    ret = getattr(super(my_type, my_copy), method_name)(*args, **kwargs)

    # Only bother calling update if we had to copy
    if copy_required:
        # Call our cb function
        for func in self._flyweight_cb_func:
            func(my_copy)
    else:
        # Update the cache with our new value

        # Remove ref to this object since hash changed
        del CoW._flyweight_cache[type(my_copy)][my_old_hash]

        # Invalidate hash cache if one exists
        if hasattr(my_copy, "_hash_cache"):
            my_copy._hash_cache = None
        # Add new ref
        CoW._flyweight_cache[type(my_copy)][hash(my_copy)] = my_copy

    # Return any value that might be returned
    return ret


# Things list class does in-place that we need to watch out for
proxy_list_inplace_dict = ['clear', 'pop', 'popitem']


class in_init(object):
    # class to handle setting and unsetting self.__in_init bool."

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        self.obj._in_init = True

    def __exit__(self, type, value, traceback):
        self.obj._in_init = False


class ProxyDict(OrderedDict, CoW):
    def __init__(self, d):
        # Recursively proxify first
        import sys
        # sys.setrecursionlimit(100000)
        d = dict((item, proxify(d[item])) for item in d)
        CoW.__init__(self)

        with in_init(self):

            self._hash_cache = None

            # If we're already a Proxy Dict, just pass through
            if type(d) in [ProxyDict, OrderedDict]:
                return OrderedDict.__init__(self, d)

            # Sorting this by default to reduce burden on hash
            super(ProxyDict, self).__init__(d)

    def copy(self):
        print ('yo')
        return self.__copy__()

    def fromkeys(self, *args, **kwargs):
        """Need to figure out how to implement this. Getting strange errors."""
        raise NotImplementedError("not implemented this yet.")

    def __copy__(self):
        return ProxyDict(self)

    def __hash__(self):
        if self._hash_cache is None:
            try:
                self._hash_cache = hash(tuple(self.items()))
            except:
                print self.items()
                assert False
        return self._hash_cache

    def __setitem__(self, *args, **kwargs):
        if not self._in_init:
            return CoW.__setitem__(self, *args, **kwargs)
        else:
            return super(ProxyDict, self).__setitem__(*args, **kwargs)

    def __getattribute__(self, key):
        # If we don't need to proxy this call, just do it.
        if key not in proxy_list_inplace_dict or key in vars(self).keys():
            return super(ProxyDict, self).__getattribute__(key)

        # Proxy this call
        return lambda *args, **kwargs: list_do_generic_call(
            self, key, *args, **kwargs)

    def __getitem__(self, key):
        # Proxy to call first, which will come back
        # to our first subclass of list.
        return CoW.__getitem__(self, key)


# Things list class does in-place that we need to watch out for
proxy_list_inplace_list = ["append", "clear", "extend",
                           "insert", "pop", "remove",
                           "reverse", "sort"]


class ProxyList(list, CoW):

    def __init__(self, iterable):

        # Recursively transform into CoW objects
        proxified = [proxify(item) for item in iterable]
        CoW.__init__(self, proxified)
        list.__init__(self, proxified)
        self._hash_cache = None

    def __hash__(self):
        if self._hash_cache is None:
            self._hash_cache = hash(tuple(self))
        return self._hash_cache

    def __copy__(self):
        return ProxyList(self)

    def __getattribute__(self, key):
        # If we don't need to proxy this call, just do it.
        if key not in proxy_list_inplace_list:
            return super(ProxyList, self).__getattribute__(key)

        # Proxy this call -- invalidate cache as we will be changing
        return lambda *args, **kwargs: list_do_generic_call(
            self, key, *args, **kwargs)

    def __getitem__(self, key):
        # Proxy to call first, which will come back
        # to our first subclass of list.
        return CoW.__getitem__(self, key)

    def __setitem__(self, *args, **kwargs):
        # Invalidate our cache
        self._hash_cache = None
        # Letting CoW get first shot at this
        return CoW.__setitem__(self, *args, **kwargs)

    def __iadd__(self, *args, **kwargs):
        # Invalidate our cache
        self._hash_cache = None

        # Can't overload special methods through getattribute,
        # so just proxying here.
        return list_do_generic_call(self, "__iadd__", *args, **kwargs)


proxy_list_inplace_set = ["add", "clear", "difference_pdate", "discard",
                          "intersection_pdate", "pop", "remove",
                          "symmetric_difference_update", "update"]


class ProxySet(set, CoW):
    def __init__(self, *args, **kwargs):
        set.__init__(self, *args, **kwargs)
        CoW.__init__(self, *args, **kwargs)
        self._hash_cache = None

    def __hash__(self):
        if self._hash_cache is None:
            self._hash_cache = hash(tuple(self))
        return self._hash_cache

    def __copy__(self):
        return ProxySet(self)

    def __getattribute__(self, key):
        # If we don't need to proxy this call, just do it.
        if key not in proxy_list_inplace_set:
            return super(ProxySet, self).__getattribute__(key)

        # Proxy this call
        return lambda *args, **kwargs: list_do_generic_call(
            self, key, *args, **kwargs)


class ProxyStr(unicode, CoW):
    def __init__(self, *args, **kwargs):
        # str keeps it's setup all in __new__
        CoW.__init__(self, *args, **kwargs)
        self._hash_cache = None

    def __hash__(self):
        if self._hash_cache is None:
            self._hash_cache = super(ProxyStr, self).__hash__()
        return self._hash_cache

    def __copy__(self):
        return ProxyStr(self)


class ProxyTuple(ProxyList, CoW):
    def __init__(self, tup):
        # Turning into a list so we can weakref
        return super(ProxyTuple, self).__init__(tup)

    def __eq__(self, obj):
        """Pretending to be a tuple..."""
        return tuple(self) == obj

    def __hash__(self):
        return super(ProxyTuple, self).__hash__()

    def __copy__(self):
        return ProxyTuple(self)

    def __setitem__(self, *args, **kwargs):
        raise TypeError("'tuple' object does not support item assignment")
