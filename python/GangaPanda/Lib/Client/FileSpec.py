"""
file specification

"""

class FileSpec(object):
    # attributes
    _attributes = ('rowID','PandaID','GUID','lfn','type','dataset','status','prodDBlock',
                   'prodDBlockToken','dispatchDBlock','dispatchDBlockToken','destinationDBlock',
                   'destinationDBlockToken','destinationSE','fsize','md5sum')
    # slots
    __slots__ = _attributes+('_owner',)


    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self,attr,None)
        # set owner to synchronize PandaID
        self._owner = None


    # override __getattribute__ for SQL and PandaID
    def __getattribute__(self,name):
        # PandaID
        if name == 'PandaID':
            if self._owner == None:
                return 'NULL'
            return self._owner.PandaID
        # others
        ret = object.__getattribute__(self,name)
        if ret == None:
            return "NULL"
        return ret


    # set owner
    def setOwner(self,owner):
        self._owner = owner
        
    
    # return a tuple of values
    def values(self):
        ret = []
        for attr in self._attributes:
            val = getattr(self,attr)
            ret.append(val)
        return tuple(ret)


    # pack tuple into FileSpec
    def pack(self,values):
        for i in range(len(self._attributes)):
            attr= self._attributes[i]
            val = values[i]
            setattr(self,attr,val)


    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self,attr)
            state.append(val)
        # append owner info
        state.append(self._owner)
        return state


    # restore state from the unpickled state values
    def __setstate__(self,state):
        for i in range(len(self._attributes)):
            if i+1 < len(state):
                setattr(self,self._attributes[i],state[i])
            else:
                setattr(self,self._attributes[i],'NULL')                
        self._owner = state[-1]
        
        
    # return column names for INSERT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
        return ret
    columnNames = classmethod(columnNames)


    # return expression of values for INSERT
    def valuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        ret += ")"            
        return ret
    valuesExpression = classmethod(valuesExpression)


    # return an expression for UPDATE
    def updateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        return ret
    updateExpression = classmethod(updateExpression)


        

                       
