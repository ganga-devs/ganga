# NOTICE:
# this module is used by config system to configure logging therefore the logger
# requires special handling
# this module must be importable without any side-effect, especially without
# creation of loggers by surrounding packages...

from inspect import isclass

found_types = {}
found_values = {}

def dummy_func( _input ):
    return _input

def safer_eval( _input ):
    try:
        from Ganga.GPIDev.Base.Proxy import getRuntimeGPIObject
        temp_output = getRuntimeGPIObject( _input, True)
        if temp_output is None:
            if len(_input) > 0 and _input != ".":
                try:
                    _output = eval(str(_input))
                except:
                    _output = str(_input)
            else:
                _output = None
        elif isclass(temp_output):
            _output = stripProxy(temp_output)
        else:
            _output = temp_output
    except ImportError:
        if len(_input) > 0 and _input != ".":
            try:
                _output = eval(str(_input))
            except:
                _output = str(_input)
        else:
            _output = None

    return _output

def _valueTypeAllowed(val, valTypeList, logger=None):
    for _t in valTypeList:

        ## Return None when None
        if _t is None:
            if val is None:
                return True

        if type(_t) is str:
            global found_types

            if _t not in found_types.keys():
                temp = safer_eval(_t)
                if type(temp) != type(type('')):
                    temp = type(temp)
                found_types[_t] = temp
            _type = found_types[_t]
        else:
            _type = _t
        
        if type(val) is str:
            global found_values

            if val not in found_values.keys():
                found_values[val] = safer_eval(val)
            _val = found_values[val]
        else:
            _val = val

        try:
            from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
            knownLists = (list, tuple, GangaList)
        except Exception as err:
            knownLists = (list, tuple)

        # Deal with the case where type is None.
        if _type is None:
            if _val is None:
                return True
            else:
                continue

        try:
            ## Type Checking ""Should"" be proxy agnoistic but this may have problems loading before certain classes
            from Ganga.GPIDev.Base.Proxy import stripProxy
        except ImportError:
            stripProxy = dummy_func

        raw_type = stripProxy(_type)
        raw_val = stripProxy(val)
        try:
            if raw_type in knownLists:
                if isinstance(raw_val, knownLists):
                    return True
            elif isinstance(raw_val, raw_type):
                return True
        except:
            continue

    return False

