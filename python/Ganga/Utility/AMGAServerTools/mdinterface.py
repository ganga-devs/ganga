#
# $Id: mdinterface.py,v 1.1 2008-07-17 16:41:01 moscicki Exp $
#


class CommandException(Exception):

    """ Raised when the command failed to execute on the server.
    This is not a fatal error, the connection is not necessarily
    broken after this exception is raised.
    """

    def __init__(self, errorCode, msg):
        self.errorCode = errorCode
        self.msg = msg

    def __str__(self):
        return repr(self.errorCode) + ' - ' + repr(self.msg)


class MDInterface:

    def eot(self):
        print "Please implement eot"

    def getattr(self, file, attributes):
        print "Please implement getattr"

    def getEntry(self):
        print "Please implement getEntry"

    def setAttr(self, file, keys, values):
        print "Please implement setAttr"

    def addEntry(self, file, keys, values):
        print "Please implement addEntry"

    def addEntries(self, entries):
        print "Please implement addEntries"

    def addAttr(self, file, name, t):
        print "Please implement addAttr"

    def removeAttr(self, file, name):
        print "Please implement removeAttr"

    def clearAttr(self, file, name):
        print "Please implement clearAttr"

    def listEntries(self, pattern):
        print "Please implement listEntries"

    def pwd(self):
        print "Please implement pwd"

    def listAttr(self, file):
        print "Please implement listAttr"

    def createDir(self, dir):
        print "Please implement createDir"

    def removeDir(self, dir):
        print "Please implement removeDir"

    def rm(self, path):
        print "Please implement rm"

    def selectAttr(self, attributes, query):
        print "Please implement selectAttr"

    def getSelectAttrEntry(self):
        print "Please implement getSelectAttrEntry"

    def updateAttr(self, pattern, updateExpr, condition):
        print "Please implement updateAttr"

    def upload(self, collection, attributes):
        print "Please implement upload"

    def put(self, file, values):
        print "Please implement put"

    def abort(self):
        print "Please implement abort"

    def commit(self):
        print "Please implement commit"

    def sequenceCreate(self, name, directory, increment=1, start=1):
        print "Please implement sequenceCreate"

    def sequenceNext(self, name):
        print "Please implement sequenceNext"

    def sequenceRemove(self, name):
        print "Please implement sequenceRemove"

    def splitUpdateClause(self, clause):
        # skip leading white space
        i = 0
        while i < len(clause) and (clause[i] == ' ' or clause[i] == '\t'):
            i = i + 1
        clause = clause[i:]

        espcaped = False
        quoted = False
        i = 0
        while i < len(clause):
            if clause[i] == "'" and not escaped:
                quoted = not quoted
            if clause[i] == '/':
                escaped = not escaped
            if (clause[i] == ' ' or clause[i] == '/t') and not quoted:
                break
            i = i + 1
        if i == 0 or i >= len(clause) - 1:
            raise mdinterface.CommandException(3, "Invalid update statement")

        var = clause[0:i]
        exp = clause[i + 1:]
        i1 = exp.find("'")
        i2 = exp.rfind("'")
        if i1 == 0 and i2 == len(exp) - 1:
            exp = exp[i1 + 1:i2]
        i1 = var.find("'")
        i2 = var.rfind("'")
        if i1 == 0 and i2 == len(var) - 1:
            var = var[i1 + 1:i2]
        return var, exp
