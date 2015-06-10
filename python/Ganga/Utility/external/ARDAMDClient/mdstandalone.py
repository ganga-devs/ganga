################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: mdstandalone.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
################################################################################
import os
import re
import errno
import time
import mdinterface
from mdinterface import CommandException, MDInterface
from mdparser import MDParser
from mdtable import MDTable
from diskutils import RLock, getLast, readLast, write, remove

DEBUG = False

def visitLocksRemove(arg, directory, files):
    age = arg
    for name in files:
        if name != 'LOCK':
            continue
        filename = os.path.join(directory, name)
        mtime = os.stat(filename).st_mtime
        if age > 0 and mtime > age:
            continue
        os.remove(filename)


def visitLocksList(arg, directory, files):
    (age, root, lines) = arg
    for name in files:
        if name != 'LOCK':
            continue
        filename = os.path.join(directory, name)
        mtime = os.stat(filename).st_mtime
        if age > 0 and mtime > age:
            continue               
        line = filename[len(root)-1:] # Remove path to root directory
        line = os.path.normpath(line)
        line = line[0: len(line)-5] # Remove /LOCK
        if len(line) == 0:
            line = "/"
        line = os.path.normpath(line)
        lines.append(line)


class MDStandalone (mdinterface.MDInterface):
    def __mkdir(self, newdir):
        """ works the way a good mkdir should :)
        - already exists, silently complete
        - regular file in the way, raise an exception
        - parent directory(ies) does not exist, make them as well
        """
        if os.path.isdir(newdir):
            pass
        elif os.path.isfile(newdir):
            raise mdinterface.CommandException(16, "Directory exists")
        try:
            os.makedirs(newdir)
        except OSError as e:
            if e[0] == errno.EPERM or e[0] == errno.EACCES:
                raise mdinterface.CommandException(4, "Could not create dir: Permission denied")
            else:
                raise mdinterface.CommandException(16, "Directory existed")


    def __init__(self, root,
                 blocklength  = 1000,
                 cache_size   = 100000,
                 tries_limit  = 200):
        self.root = os.path.normpath(root)
        self.blocklength = blocklength
        self.cache_size  = cache_size
        self.tries_limit = tries_limit
        self.rows=[]
        self.tables = {}
        self.currentDir = '/'
        self.loaded_tables = {}
        self.upload_cmd = {}
        self.transaction_in_prgs = False
        self.sequence_reserve = {}


    def __isDir(self, dirname):
        name = self.__systemPath(dirname)
        return os.path.isdir(name)


    def __absolutePath(self, table):
        if DEBUG: print '__absolutePath for ', table
        if len(table) and table[0] == '/':
            prefix = ''
        else:
            prefix = self.currentDir + '/'
        table = os.path.normpath(prefix + table).replace(os.sep, '/')
        if DEBUG: print '__absolutePath: returning', table
        return table


    def __systemPath(self, path):
        path = os.path.normpath(self.root + '/' + path)
        if DEBUG: print '__systemPath returns: ', path
        return path


    def __initTransaction(self):
        if not self.transaction_in_prgs:
            self.tables = {}


    def __loadTable(self, table, update = True):
        table = self.__absolutePath(table)
        if DEBUG: print 'Loading table', table
        if table in self.tables:
            mdtable = self.tables[table]
        else:
            mdtable = self.__getTable(table) 
            if not mdtable.lock():
                raise CommandException(9, 'Could not acquire table lock %s' % table)
            self.tables[table] = mdtable
            mdtable.load()
        return mdtable, table


    def __saveTable(self, table):
        if not self.transaction_in_prgs:
            if DEBUG: print 'Saving table', table
            mdtable = self.loaded_tables[table]
            mdtable.save()

        
    def __addEntry(self, mdtable, entry, attrs, values):
        if entry in mdtable.entries:
            raise CommandException(15, "Entry exists")       
        # new entry
        e=[''] * (len(mdtable.attributes)+1)
        e[0]=entry
        for i in range(0, len(attrs)):
            e[mdtable.attributeDict[attrs[i]]+1] = values[i]
        mdtable.entries.append(e)
        

    def __getTable(self, table):
        dirname = self.__systemPath(table)
        if not os.path.isdir(dirname):
            raise CommandException(1, 'File or directory does not exist')               
        if table not in self.loaded_tables:
            self.loaded_tables[table] = MDTable(dirname,
                                                blocklength = self.blocklength,
                                                cache_size  = self.cache_size,
                                                tries_limit = self.tries_limit)
        return self.loaded_tables[table]
    

    def releaseAllLocks(self):
        if not self.transaction_in_prgs:
            for table in self.tables:
                self.tables[table].unlock()

        
    def eot(self):
        return len(self.rows) == 0


    def addEntries(self, entries):
        self.__initTransaction()
        try:
            tablename = os.path.dirname(entries[0])
            mdtable, tablename = self.__loadTable(tablename, True)
            emptyAttr = [''] * len(mdtable.attributes)
            for entry in entries:
                entry_key = os.path.basename(entry)
                if entry_key in mdtable.entries:
                    raise CommandException(15, "Entry exists")
                row = [entry_key]
                row.extend(emptyAttr)
                mdtable.entries.append(row)
            self.__saveTable(tablename)
        finally:
            self.releaseAllLocks()


    def addAttr(self, file, name, t):
        self.__initTransaction()
        try:
            dirname = self.__absolutePath(file)
            if not self.__isDir(dirname):
                dirname = os.path.dirname(dirname)
            mdtable, tablename = self.__loadTable(dirname, True)
            if name in mdtable.attributeDict:
                raise CommandException(15, "Attribute exists")
            mdtable.attributes.append([name, t])
            mdtable.update()
            e_len = len(mdtable.attributes)
            for i in range(0, len(mdtable.entries)):
                e = mdtable.entries[i]
                e[e_len:] = ['']
                mdtable.entries[i] = e
            self.__saveTable(tablename)
        finally:
            self.releaseAllLocks()
        

    def listAttr(self, file):
        self.__initTransaction()
        try:
            dirname = self.__absolutePath(file)
            if not self.__isDir(dirname):
                dirname = os.path.dirname(dirname)
            mdtable, dirname = self.__loadTable(dirname)
            names = []
            types = []
            for i in range(0, len(mdtable.attributes)):
                pair = mdtable.attributes[i]
                names.append(pair[0])
                types.append(pair[1])
            return names, types
        finally:
            self.releaseAllLocks()


    def addEntry(self, file, keys, values):
        self.__initTransaction()
        try:
            tablename, entry = os.path.split(file)
            mdtable, tablename = self.__loadTable(tablename, True)
            self.__addEntry(mdtable, entry, keys, values)
            self.__saveTable(tablename)
        finally:
            self.releaseAllLocks()


    def getattr(self, file, attrs):
        self.__initTransaction()
        try:
            if self.__isDir(file):
                tablename = file
                entry = '*'
            else:
                tablename, entry = os.path.split(file)
            mdtable, tablename = self.__loadTable(tablename)
            self.rows = []
            pattern = entry.replace('*', '.*')
            pattern = pattern.replace('?', '.')
            for i in range(0, len(mdtable.entries)):
                e = mdtable.entries[i]
                r = re.match(pattern, e[0])
                if r and r.group(0) == e[0]:
                    row = []
                    row.append(e[0])
                    for j in range(0, len(attrs)):
                        index = mdtable.attributeDict[attrs[j]]
                        row.append(e[index+1])
                    self.rows.append(row)
        finally:
            self.releaseAllLocks()


    def getEntry(self):
        e = self.rows.pop(0)
        # entry, row = (e[0], e[1:])
        return (e[0], e[1:])

    
    def createDir(self, dirname):
        if DEBUG: print 'createDir ', dirname
        newdir = self.__systemPath(self.__absolutePath(dirname))
        self.__mkdir(newdir)


    def listEntries(self, dirname):
        if DEBUG: print 'listEntries ', dirname
        self.__initTransaction()
        try:
            dirname = self.__absolutePath(dirname)
            if self.__isDir(dirname):
                entry = "*"
            else:
                dirname, entry = os.path.split(dirname)
                
            # List directories, first
            dirs = os.listdir(self.__systemPath(dirname))
            for d in dirs:
                if self.__isDir(dirname + '/' +d):
                    row = []
                    row.append(dirname + '/' +d)
                    row.append('collection')
                    self.rows.append(row)
            
            # Now list entries
            mdtable, dirname = self.__loadTable(dirname)
            pattern = entry.replace('*', '.*')
            pattern = pattern.replace('?', '.')
            for i in range(0, len(mdtable.entries)):
                e = mdtable.entries[i]
                r = re.match(pattern, e[0])
                if r and r.group(0) == e[0]:
                    row = []
                    row.append(e[0])
                    row.append('entry')
                    self.rows.append(row)
        finally:
            self.releaseAllLocks()


    def removeAttr(self, file, name):
        if DEBUG: print 'removeAttr ', file, name
        self.__initTransaction()
        try:
            dirname = self.__absolutePath(file)
            if not self.__isDir(dirname):
                dirname = os.path.dirname(dirname)
            mdtable, dirname = self.__loadTable(dirname, True)
            if name not in mdtable.attributeDict:
                raise CommandException(10, "No such key")
            iattr = mdtable.attributeDict[name]
            del mdtable.attributes[iattr]
            mdtable.update()
            for i in range(0, len(mdtable.entries)):
                e = mdtable.entries[i]
                del e[iattr+1] 
                mdtable.entries[i] = e
            self.__saveTable(dirname)
        finally:
            self.releaseAllLocks()
        

    def rm(self, path):        
        if DEBUG: print 'rm ', path
        self.__initTransaction()
        try:
            dirname, entry = os.path.split(path)
            mdtable, dirname = self.__loadTable(dirname, True)
            pattern = entry.replace('*', '.*')
            pattern = pattern.replace('?', '.')
            for i in range(len(mdtable.entries)-1, -1, -1):
                e =  mdtable.entries[i]
                r = re.match(pattern, e[0])
                if r and r.group(0) == e[0]:
                    del mdtable.entries[i]
            self.__saveTable(dirname)
        finally:
            self.releaseAllLocks()


    def sequenceCreate(self, name, directory, increment=1, start=1):
        name = self.__systemPath(self.__absolutePath(directory) + '/SEQ'+name)
        lockfile = name+".LOCK"
        lock = RLock(lockfile)
        if lock.acquire():
            try:
                try:
                    lfn = getLast(name)
                except OSError:
                    pass
                else:
                    raise CommandException(15, "Sequence exists: " + lfn)
                entry = [str(start), str(increment), str(start)]
                try:
                    write([entry], name)
                except (OSError,IOError) as e:
                    raise CommandException(17, "sequenceCreate error: " + str(e))
            finally:
                lock.release()
        else:
            raise CommandException(9, "Cannot lock %s" % name)


    def sequenceNext(self, name, reserve = 1):
        # with reserve > 1 this method returns
        # reserved  value for the counter if there is one available,
        # otherwise it will reserve specified number of values and return the first one
        def next(name, reserve):
            dirname, seq = os.path.split(name)
            name = self.__systemPath(self.__absolutePath(dirname) + '/SEQ'+seq)
            lockfile = name+".LOCK"
            lock = RLock(lockfile)
            if lock.acquire():
                try:
                    try:
                        entries = readLast(name)
                    except (OSError,IOError) as e:
                        raise CommandException(17, "Not a sequence: " + str(e))
                    if not (len(entries)==1 and len(entries[0])==3):
                        raise CommandException(17, "Not a sequence: " + str(e))
                    start, increment, now = entries[0]   
                    newval = str(int(now) + int(increment) * reserve)
                    entry = [start, increment, newval]
                    try:
                        write([entry], name)
                    except (OSError,IOError) as e:
                        raise CommandException(17, "sequenceNext error: " + str(e))
                    return (now, increment, newval)
                finally:
                    lock.release()
            else:
                raise CommandException(9, "Cannot lock %s" % name)
        # use reserved values if possible (don't read the sequence file)
        if reserve > 1:
            try:
                if name in self.sequence_reserve:
                    now, increment, reserved = self.sequence_reserve[name]
                else:
                    now, increment, reserved = map(int, next(name, reserve))
                    self.sequence_reserve[name] = [now, increment, reserved]
                newval = now + increment
                if newval >= reserved:
                    del self.sequence_reserve[name]
                else:
                    self.sequence_reserve[name][0] = newval
                return str(now)
            except Exception as e:
                raise CommandException(17, "sequenceNext error: " + str(e))
        else:
            return next(name, reserve)[0]


    def sequenceRemove(self, name):
        if name in self.sequence_reserve:
            del self.sequence_reserve[name]
        dirname, seq = os.path.split(name)
        name = self.__systemPath(self.__absolutePath(dirname) + '/SEQ'+seq)
        lockfile = name+".LOCK"
        lock = RLock(lockfile)
        if lock.acquire():       
            try:
                try:
                    remove(name)
                except OSError as e:
                    raise CommandException(17, "sequenceRemove error: " + str(e))
            finally:
                lock.release()
        else:
            raise CommandException(9, "Cannot lock %s" % name)
        
    
    def removeDir(self, dirname):
        if DEBUG: print 'removeDir ', dirname
        name = self.__systemPath(self.__absolutePath(dirname))
        try:
            os.rmdir(name)
        except OSError as e:
            if e[0] == errno.ENOENT:
                raise mdinterface.CommandException(1, "Directory not found")
            else:
                raise mdinterface.CommandException(11, "Directory not empty")                                    
    
        
    def selectAttr(self, attrs, query):
        if DEBUG: print 'selectAttr ', attrs, query
        self.__initTransaction()
        try:
            self.rows = []
            tList = []
            aList = []
            for a in attrs:
                table, attr = a.split(':', 1)
                mdtable, table = self.__loadTable(table)
                tList.append(table)
                aList.append(attr)
            
            parser = MDParser(query, self.tables,
                              self.currentDir, self.__loadTable)      
            
            # First, only load necessary tables
            parser.parseWhereClause()
            allTables = []
            iterations = 0
            for k, v in self.tables.iteritems():
                v.initRowPointer()
                allTables.append(v)
                if iterations == 0:
                    iterations = len(v.entries)
                else:
                    iterations = iterations * len(v.entries)

            for i in range(0, iterations):
                if parser.parseWhereClause():
                    row = []
                    for j in range(0, len(attrs)):
                        t = self.tables[tList[j]]
                        if aList[j] == "FILE":
                            index = 0
                        else:
                            index = t.attributeDict[aList[j]]+1
                        row.append(t.entries[t.currentRow][index])
                    self.rows.append(row)
                for j in range (0, len(allTables)):
                    if allTables[j].currentRow < 0:
                        continue
                    allTables[j].currentRow = allTables[j].currentRow + 1
                    if allTables[j].currentRow < len(allTables[j].entries):
                        break
                    allTables[j].currentRow = 0   
        finally:
            self.releaseAllLocks()


    def getSelectAttrEntry(self):
        # attributes = self.rows.pop(0)
        return self.rows.pop(0)


    def updateAttr(self, pattern, updateExpr, condition):
        if DEBUG: print 'updateAttr ', pattern, updateExpr, condition
        self.__initTransaction()
        try:
            dirname = self.__absolutePath(pattern)
            if self.__isDir(dirname):
                entry = "*"
            else:
                dirname, entry = os.path.split(dirname)
            mdtable, tablename = self.__loadTable(dirname, True)
            pattern = entry.replace('*', '.*')
            pattern = pattern.replace('?', '.')

            parser = MDParser(condition, self.tables,
                              tablename, self.__loadTable)

            # First, only load necessary tables
            parser.parseWhereClause()

            # Prepare update expressions
            expressions = []
            for e in updateExpr:
                var, exp = self.splitUpdateClause(e)
                index = mdtable.attributeDict[var]+1
                p = MDParser(exp, self.tables, tablename,
                             self.__loadTable)
                parser.parseWhereClause()
                expressions.append( (index, exp, p) )

            # Find all tables which have entries and list them in allTables,
            # and count the number of iterations we need to do
            allTables = []
            iterations = 0
            for k, v in self.tables.iteritems():
                v.initRowPointer()
                if len(v.entries) == 0:
                  continue
                allTables.append(v)
                if iterations == 0:
                    iterations = len(v.entries)
                else:
                    iterations = iterations * len(v.entries)

            # Iterate over all combinations of table entries, check whether
            # update clause if fulfilled and do the update
            for i in range(0, iterations):
                e = mdtable.entries[mdtable.currentRow]
                r = re.match(pattern, e[0])
                if r and r.group(0) == e[0]:
                    if parser.parseWhereClause():
                        for j in range (0, len(expressions)):
                            index, exp, p = expressions[j]
                            value = p.parseStatement()
                            e[index] = value
                        mdtable.entries[mdtable.currentRow] = e
                for j in range (0, len(allTables)):
                    allTables[j].currentRow = allTables[j].currentRow + 1
                    if allTables[j].currentRow < len(allTables[j].entries):
                        break
                    else:
                        allTables[j].currentRow = 0
            self.__saveTable(tablename)
        finally:
            self.releaseAllLocks()


    def setAttr(self, file, keys, values):
        self.__initTransaction()
        tablename, entry = os.path.split(file)
        mdtable, tablename = self.__loadTable(tablename, True)
        try:
            pattern = entry.replace('*', '.*')
            pattern = pattern.replace('?', '.')
            for i in range(0, len(mdtable.entries)):
                e =  mdtable.entries[i]
                r = re.match(pattern, e[0])
                if r and r.group(0) == e[0]:
                    for j in range(0, len(keys)):
                        index = mdtable.attributeDict[keys[j]]
                        e[index+1] = values[j]
                    mdtable.entries[i] = e
                self.__saveTable(tablename)
        finally:
            self.releaseAllLocks()


    def pwd(self):
        return self.currentDir


    def cd(self, dir):
        dir = self.__absolutePath(dir)
        if not self.__isDir(dir):
            raise CommandException(1, "Not a directory")
        self.currentDir = dir


    def upload(self, collection, attributes):
        self.transaction() # start transaction
        mdtable, tablename = self.__loadTable(collection, True)
        self.upload_cmd['collection'] = tablename
        self.upload_cmd['attributes'] = attributes


    def put(self, file, values):
        if not self.transaction_in_prgs:
            raise CommandException(9, "Could not abort No transaction in progress. Command was: abort")
        elif len(values)!=len(self.upload_cmd['attributes']):
            raise CommandException(3, "Illegal command")
        tablename, entry = os.path.split(file)
        if tablename:
            assert(tablename == self.upload_cmd['collection'])
        mdtable = self.tables[self.upload_cmd['collection']]
        attrs = self.upload_cmd['attributes']
        self.__addEntry(mdtable, entry, attrs, values)


    def abort(self):
        if not self.transaction_in_prgs:
            raise CommandException(9, "Could not abort No transaction in progress. Command was: abort")
        try:
            self.transaction_in_prgs = False
            self.upload_cmd = {}
        finally:
            self.releaseAllLocks()


    def commit(self):
        if not self.transaction_in_prgs:
            raise CommandException(9, "Could not commit No transaction in progress. Command was: commit")
        try:
            self.transaction_in_prgs = False
            for table in self.tables:
                self.__saveTable(table)
        finally:
            self.releaseAllLocks()

            
    def transaction(self):
        self.__initTransaction()
        if not self.transaction_in_prgs:
            self.transaction_in_prgs = True
        

    # Removes all locks, even those of other processes recursively
    # starting at the root directory by deleting the LOCK files
    # If age is given it restricts the deletion operation on most
    # operating systems to locks older than age seconds
    def removeAllLocks(self, age = -1):
        if age > 0:
            age = time.time()-age
        os.path.walk(self.root, visitLocksRemove, age);


    # Returns a list witha all currently held locks (locks older than age if age>0)
    def listAllLocks(self, age = -1):
        if age > 0:
            age = time.time()-age
        
        lines = []
        os.path.walk(self.root, visitLocksList, (age, self.root, lines));
        return lines
