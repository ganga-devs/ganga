import collections
import fnmatch
import re
import reprlib
import sys
from inspect import isclass
import GangaCore.Utility.logging
from GangaCore.Core.exceptions import GangaException
from GangaCore.Core.GangaRepository.Registry import RegistryKeyError, RegistryIndexError, RegistryAccessError, IncompleteObject

from GangaCore.GPIDev.Schema import ComponentItem
from GangaCore.Utility.external.OrderedDict import OrderedDict as oDict
import GangaCore.Utility.Config
from GangaCore.GPIDev.Base.Proxy import isType, stripProxy, getName, addProxy
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config import makeConfig

logger = getLogger()

config = GangaCore.Utility.Config.getConfig('Display')

class RegistrySlice(object):

    def __init__(self, name, display_prefix):
        """
        Constructor for a Registry Slice object
        Args:
            name (str): Slightly descriptive name of the slice
            display_prefix (str): Prefix for slice to help modify display and other string reps of the object
        """
        super(RegistrySlice, self).__init__()
        self.objects = oDict()
        self.name = name
        self._display_prefix = display_prefix
        self._display_columns = config[self._display_prefix + '_columns']
        self._display_columns_show_empty = config[self._display_prefix + "_columns_show_empty"]
        self._display_columns_width = config[self._display_prefix + "_columns_width"]
        self._display_columns_functions = {}
        try:
            col_funcs = config[self._display_prefix + '_columns_functions']
            for this_col_func in col_funcs:
                self._display_columns_functions[this_col_func] = eval(col_funcs[this_col_func])
        except Exception as x:
            logger.error("Error on evaluating display column functions from config file: %s: %s" % (getName(x), x))

        from GangaCore.Utility.ColourText import Effects
        self._colour_normal = Effects().normal
        self._proxyClass = None

    def _getColour(self, obj):
        """ Override this function in derived slices to colorize your job/task/... list"""
        return self._colour_normal

    def do_collective_operation(self, keep_going, method, *args, **kwds):
        """
        """
        if not isinstance(keep_going, bool):
            raise GangaException("The variable 'keep_going' must be a boolean. Probably you wanted to do %s(%s).%s()" % (self.name, keep_going, method))
        result = []
        for _id in self.objects.keys():
            obj = self.objects[_id]
            try:
                if isinstance(method, str):
                    doc = method
                    result.append(getattr(obj, method)(*args, **kwds))
                else:
                    try:
                        doc = method.__doc__
                    except AttributeError:
                        doc = str(method)
                    result.append(method(obj, *args, **kwds))
            except GangaException as x:
                if not keep_going:
                    raise
            except Exception as x:
                logger.exception('%s %s %s: %s %s', doc, self.name, _id, getName(x), x)
                if not keep_going:
                    raise
        return result

    def ids(self, minid=None, maxid=None):
        "Get the list of job ids. 'minid' and 'maxid' specify optional (inclusive) slice range."
        if maxid is None:
            maxid = sys.maxsize
        if minid is None:
            minid = 0
        return [k for k in self.objects.keys() if minid <= k <= maxid]

    def clean(self, confirm=False, force=False):
        """Cleans the repository only if this slice represents the repository
        Returns True on success and False on failure"""
        if not hasattr(self.objects, "clean"):
            logger.error("'clean' only works on whole registries, e.g. 'jobs.clean()'. Use remove() to delete job slices")
            return False
        if not confirm:
            logger.warning("You are about to irreversibly delete the WHOLE '%s' registry, without properly cleaning up individual jobs." % (self.objects.name))
            if force:
                logger.warning("You will also cause any other Ganga sessions accessing this repository to shut down their operations")
                logger.warning("If you just want to remove all jobs, type '%s.remove()'. If you really want to do this, type '%s.clean(confirm=True,force=True)" % (self.objects.name, self.objects.name))
            else:
                logger.warning("If you just want to remove all jobs, type '%s.remove()'. If you really want to do this, type '%s.clean(confirm=True)" % (self.objects.name, self.objects.name))
            return False
        return self.objects.clean(force)

    def select(self, minid=None, maxid=None, **attrs):
        from GangaCore.GPIDev.Lib.Job.Job import Job

        if isType(minid, Job):
            if minid.master is not None:
                minid = minid.master.id
            else:
                minid = minid.id
            if maxid is None:
                maxid = minid

        if isType(maxid, Job):
            if maxid.master is not None:
                maxid = maxid.master.id
            else:
                maxid = maxid.id

        logger = getLogger()

        this_repr = reprlib.Repr()
        from GangaCore.GPIDev.Base.Proxy import addProxy
        attrs_str = ""
        ## Loop through all possible input combinations to constructa string representation of the attrs from possible inputs
        ## Required to flatten the additional arguments into a flat string in attrs_str
        for a in attrs:
            if isclass(attrs[a]):
                this_attr = addProxy(attrs[a]())
            else:
                from GangaCore.GPIDev.Base.Objects import GangaObject
                if isType(attrs[a], GangaObject):
                    this_attr = addProxy(attrs[a])
                else:
                    if type(attrs[a]) is str:
                        from GangaCore.GPIDev.Base.Proxy import getRuntimeGPIObject
                        this_attr = getRuntimeGPIObject(attrs[a], True)
                    else:
                        this_attr = attrs[a]
            full_str = str(this_attr)
            split_str = full_str.split('\n')
            for line in split_str:
                line = line.strip()
            flat_str = ''.join(split_str)
            attrs_str += ", %s=\"%s\"" % (a, flat_str)

        logger.debug("Attrs_Str: %s" % attrs_str)
        logger.debug("Constructing slice: %s" % ("%s.select(minid='%s', maxid='%s'%s)" % (self.name, this_repr.repr(minid), this_repr.repr(maxid), attrs_str)))
        this_slice = self.__class__("%s.select(minid='%s', maxid='%s'%s)" % (self.name, this_repr.repr(minid), this_repr.repr(maxid), attrs_str))

        def append(id, obj):
            this_slice.objects[id] = obj
        self.do_select(append, minid, maxid, **attrs)
        return this_slice

    def do_select(self, callback, minid=None, maxid=None, **attrs):
        """Get the slice of jobs. 'minid' and 'maxid' specify optional (inclusive) slice range.
        The returned slice object has the job registry interface but it is not connected to
        persistent storage. 
        """

        logger = getLogger()

        ## Loop through attrs to parse possible inputs into instances of a class where appropriate
        ## Unlike the select method we need to populate this dictionary with instance objects, not str or class
        for k, v in attrs.items():
            if isclass(v):
                attrs[k] = v()
            elif type(attrs[k]) is str:
                from GangaCore.GPIDev.Base.Proxy import getRuntimeGPIObject
                new_val = getRuntimeGPIObject(attrs[k], True)
                if new_val is None:
                    continue
                if isclass(new_val):
                    attrs[k] = new_val()
                else:
                    attrs[k] = new_val

        logger.debug("do_select: attrs: %s" % attrs)

        def select_by_list(this_id):
            return this_id in ids

        def select_by_range(this_id):
            return minid <= this_id <= maxid

        ids = None

        if isinstance(minid, collections.abc.Container):
            ids = minid
            select = select_by_list
        else:
            if minid is None:
                minid = 0
            if maxid is None:
                maxid = sys.maxsize
            select = select_by_range

        for this_id in self.objects.keys():
            obj = self.objects[this_id]
            logger.debug("id, obj: %s, %s" % (this_id, obj))
            if select(int(this_id)):
                logger.debug("Selected: %s" % this_id)
                selected = True
                if self.name == 'box':
                    name_str = obj._getRegistry()._getName(obj)
                else:
                    name_str = ''
                for a in attrs:
                    if self.name == 'box':
                        attrvalue = attrs[a]
                        if a == 'name':
                            if not fnmatch.fnmatch(name_str, attrvalue):
                                selected = False
                                break
                        elif a == 'application':
                            if hasattr(obj, 'application'):
                                if not getName(obj.application) == attrvalue:
                                    selected = False
                                    break
                            else:
                                selected = False
                                break
                        elif a == 'type':
                            if not getName(obj) == attrvalue:
                                selected = False
                                break
                        else:
                            from GangaCore.GPIDev.Base import GangaAttributeError
                            raise GangaAttributeError(
                                'undefined select attribute: %s' % a)
                    else:

                        if a == 'ids':
                            if int(this_id) not in attrs['ids']:
                                selected = False
                                break
                        else:
                            try:
                                item = obj._schema.getItem(a)
                                logger.debug("Here: %s, is item: %s" % (a, type(item)))
                            except KeyError as err:
                                from GangaCore.GPIDev.Base import GangaAttributeError
                                logger.debug("KeyError getting item: '%s' from schema" % a)
                                raise GangaAttributeError('undefined select attribute: %s' % a)
                            else:
                                attrvalue = attrs[a]

                                if item.isA(ComponentItem):
                                    ## TODO we need to distinguish between passing a Class type and a defined class instance
                                    ## If we passed a class type to select it should look only for classes which are of this type
                                    ## If we pass a class instance a compartison of the internal attributes should be performed
                                    from GangaCore.GPIDev.Base.Filters import allComponentFilters

                                    cfilter = allComponentFilters[item['category']]
                                    filtered_value = cfilter(attrs[a], item)
                                    from GangaCore.GPIDev.Base.Proxy import getName
                                    if not filtered_value is None:
                                        attrvalue = getName(filtered_value)
                                    else:
                                        attrvalue = getName(attrvalue)

                                    if getName(getattr(obj, a)) != attrvalue:
                                        selected = False
                                        break
                                else:
                                    if isinstance(attrvalue, str):
                                        regex = fnmatch.translate(attrvalue)
                                        reobj = re.compile(regex)
                                        # Compare the type of the attribute
                                        # against attrvalue
                                        if not reobj.match(str(getattr(obj, a))):
                                            selected = False
                                    else:
                                        if getattr(obj, a) != attrvalue:
                                            selected = False
                                            break
                if selected:
                    logger.debug("Actually Selected")
                    callback(this_id, obj)
                else:
                    logger.debug("NOT Actually Selected")
            else:
                logger.debug("NOT Selected: %s" % this_id)

    def copy(self, keep_going):
        this_slice = self.__class__("copy of %s" % self.name)
        for _id in self.objects.keys():
            obj = self.objects[_id]
            #obj = _unwrap(obj)
            copy = obj.clone()
            # If the copied object is not automatically registered,
            # try to register it in the old objects registry
            new_id = copy._getRegistryID()
            if new_id is None:
                reg = obj._getRegistry()
                if reg is None:
                    new_id = _id
                else:
                    reg._add(copy)
                    new_id = copy._getRegistryID()
            this_slice.objects[new_id] = copy
        return this_slice

    def __contains__(self, j):
        return j.id in self.objects.keys()

    def __call__(self, this_id):
        """ Retrieve an object by id.
        """
        if isinstance(this_id, str):
            if this_id.isdigit():
                this_id = int(this_id)
            else:
                matches = [o for o in self.objects if fnmatch.fnmatch(o._getRegistry()._getName(o), this_id)]
                if len(matches) > 1:
                    logger.error('Multiple Matches: Wildcards are allowed for ease of matching, however')
                    logger.error('                  to keep a uniform response only one item may be matched.')
                    logger.error('                  If you wanted a slice, please use the select method')
                    raise RegistryKeyError("Multiple matches for id='%s':%s" % (this_id, str([x._getRegistry()._getName(x) for x in matches])))
                if len(matches) < 1:
                    return
                return addProxy(matches[0])
        try:
            return addProxy(self.objects[this_id])
        except KeyError as err:
            logger.debug('Object id=%d not found' % this_id)
            logger.debug("%s" % err)
            raise RegistryKeyError('Object id=%d not found' % this_id)

    def __iter__(self):
        "Iterator for the objects. "
        class Iterator(object):

            def __init__(self, reg):
                self.it = list(reg.objects.values()).__iter__()

            def __iter__(self): return self

            def __next__(self):
                return next(self.it)
        return Iterator(self)

    def __len__(self):
        "Number of objects in the registry"
        return len(self.objects)

    def __getitem__(self, x):
        """Retrieve the job object from the registry: registry[x].
         If 'x' is a job id (int) then a single job object is returned or IndexError.
         If 'x' is a name (string) then a unique same name is returned, otherwise [].
         If 'x' is a job object then it is returned if it belongs to the registry, otherwise None.
         If 'x' is not of any of the types above, raise TypeError.
          or by name. If retrieved by name then the job must be unique, otherwise the RegistryKeyError is raised.
         If the input is incorrect, RegistryAccessError is raised.
        """
        if isinstance(x, int):
            try:
                if x < 0:
                    return addProxy(self.objects[self.ids()[x]])
                else:
                    return addProxy(self.objects[x])
            except IndexError:
                raise RegistryIndexError('list index out of range')

        if isinstance(x, str):
            ids = []
            for i in self.ids():
                j = self.objects[i]
                if j.name == x:
                    ids.append(j.id)
            if len(ids) > 1:
                raise RegistryKeyError('object "%s" not unique' % x)
            if len(ids) == 0:
                raise RegistryKeyError('object "%s" not found' % x)
            return addProxy(self.objects[ids[0]])

        if isinstance(x, slice):
            start = x.start if x.start is not None else ''
            stop = x.stop if x.stop is not None else ''

            returnable = self.__class__("%s[%s:%s]" % (self.name, start, stop))
            for id_ in self.ids()[x]:
                returnable.objects[id_] = self.objects[id_]

            return addProxy(returnable)

        raise RegistryAccessError('Expected int or string (job name).')

    @staticmethod
    def _getatr(obj, members):
        val = getattr(obj, members[0])
        if len(members) > 1:
            return str(RegistrySlice._getatr(val, members[1:]))
        else:
            return str(val)

    def _get_display_value(self, _obj, item):
        try:
            obj = stripProxy(_obj)
            try:
                if item in self._display_columns_functions:
                    display_func = self._display_columns_functions[item]
                    val = display_func(obj)
                else:
                    val = self._getatr(obj, item.split('.'))
            except KeyError as err:
                logger.debug("_get_display_value KeyError: %s" % err)
                logger.debug("item: \"%s\"" % item)
                #logger.debug("func: %s" % config[self._display_prefix + '_columns_functions'])
                #val = self._getatr(obj, item.split('.'))
                val = ""
            if not val and not item in self._display_columns_show_empty:
                val = ""
        except AttributeError as err:
            logger.debug("AttibErr: %s" % err)
            val = ""
        finally:
            pass
        return str(val)

    def _display(self, interactive=0):
        from GangaCore.Utility.ColourText import ANSIMarkup, NoMarkup, Effects
        if interactive:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        # default column width
        default_width = 10

        cnt = len(self)
        ds = "Registry Slice: %s (%d objects)\n" % (self.name, cnt)

        this_format = "#"
        flist = []
        for d in self._display_columns:
            width = self._display_columns_width.get(d, default_width)
            flist.append("%" + str(width) + "s ")
            #this_format += "%"+str(width)+"s  "
        this_format = "|".join(flist)
        this_format += "\n"

        if cnt > 0:
            ds += "--------------\n"
            ds += this_format % self._display_columns
            ds += "-" * len(this_format % tuple([""] * len(self._display_columns))) + "\n"


        if hasattr(self.objects, '_private_display'):
            ds += self.objects._private_display(self, this_format, default_width, markup)

        else:
            for obj_i in self.ids():
                if isinstance(self.objects[obj_i], IncompleteObject):
                    continue

                cached_data = None

                reg_object = self.objects[obj_i]
                obj = stripProxy(reg_object)
                colour = self._getColour(obj)

                vals = []
                for item in self._display_columns:
                    display_str = "display:" + str(item)
                    #logger.info("Looking for : %s" % display_str)
                    width = self._display_columns_width.get(item, default_width)
                    try:
                        if item == "fqid":
                            vals.append(str(obj._index_cache[display_str]))
                        else:
                            vals.append(str(obj._index_cache[display_str])[0:width])
                        continue
                    except KeyError as err:
                        logger.debug("_display KeyError: %s" % err)
                        #pass
                        if item == "fqid":
                            vals.append(self._get_display_value(obj, item))
                        elif item == 'subjob status':
                            vals.append('---') #As this is new we don't want to reload everything
                        else:
                            vals.append(self._get_display_value(obj, item)[0:width])
                ds += markup(this_format % tuple(vals), colour)

        return ds

    __str__ = _display

    def _id(self):
        return id(self)

