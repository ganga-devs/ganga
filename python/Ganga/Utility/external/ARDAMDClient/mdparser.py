
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: mdparser.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
##########################################################################
from mdinterface import CommandException
from mdtable import EmptyTableException
import re

# TODO: Check types when writing

DEBUGS = False
DEBUG = False


class MDParser:

    def __init__(self, query, tables, mainTable, loadTable):
        self.reINT = re.compile("\d+")
        self.reFLOAT = re.compile("\.\d+\.\d+")
        self.reNAME = re.compile("[_a-zA-Z0-9.]+")
        self.reTABLEREF = re.compile("[_a-zA-Z0-9/.]+:[_a-zA-Z0-9.]+")
        self.reVARID = re.compile("[a-zA-Z][_a-zA-Z0-9]*")
        self.reSTRING = re.compile('"[^"]*"')
        self.q = query
        self.tables = tables
        if not len(mainTable):
            mainTable = '/'
        if mainTable[len(mainTable) - 1] == '/':
            mainTable = mainTable[0:len(mainTable) - 1]
        self.mainTable = mainTable
        self.loadTable = loadTable
        self.parseDict = [(None, None, None)] * (len(query) + 1)

    def parseWhereClause(self):
        if DEBUG:
            print "parsing where clause: ", self.q
        if len(self.q) == 0:
            return True
        self.__qp = 0
        try:
            token, v = self.__pStatement()
        except EmptyTableException:
            return False
        if token == 'CONST':
            return v > 0
        if token == 'EXP':
            return v > 0
        raise CommandException(8, "Illegal " + self.q + ": not boolean")

    def parseStatement(self):
        if DEBUG:
            print "parsing statement: ", self.q
        self.__qp = 0
        try:
            token, v = self.__pStatement()
        except EmptyTableException:
            return None
        return v

    def __isLiteral(self, start, literal):
        if self.__qp + len(literal) <= len(self.q) \
            and self.q[self.__qp:self.__qp + len(literal)] == literal \
            and (((len(self.q) > self.__qp + len(literal))
                  and self.q[self.__qp + len(literal)].isspace())
                 or len(self.q) == self.__qp + len(literal)):
            self.__qp = self.__qp + len(literal)
            if DEBUGS:
                'Found literal ', literal
            self.parseDict[start] = literal, literal, self.__qp
            return literal, literal
        else:
            return None, 0

    def __readToken(self):
        if DEBUGS:
            print 'Reading token at ' + str(self.__qp)
        # Check whether it is in our parser dictionary
        t, v, next = self.parseDict[self.__qp]
        if t:
            if DEBUGS:
                print 'Returning ', [self.__qp], t, ' ', v
            self.__qp = next
            return t, v
        start = self.__qp

        while self.__qp < len(self.q):
            if self.q[self.__qp].isspace():
                self.__qp = self.__qp + 1
            else:
                break

        if DEBUGS:
            print 'Reading token 2 '

        if self.__qp >= len(self.q):
            if DEBUGS:
                print "Token " + str(self.__qp) + " END"
            self.parseDict[start] = 'END', 0, self.__qp
            return 'END', 0

        t = self.reINT.match(self.q[self.__qp:])
        if t != None:
            t = t.group()
            self.__qp = self.__qp + len(t)
            if DEBUGS:
                print "Token " + str(self.__qp) + " CONST " + t
            self.parseDict[start] = 'CONST', int(t), self.__qp
            return 'CONST', int(t)

        t = self.reFLOAT.match(self.q[self.__qp:])
        if t != None:
            t = t.group()
            self.__qp = self.__qp + len(t)
            if DEBUGS:
                print "Token " + str(self.__qp) + " CONST " + t
            self.parseDict[start] = 'CONST', float(t), self.__qp
            return 'CONST', float(t)

        if DEBUGS:
            print 'Reading token 3 '

        t, v = self.__isLiteral(start, 'and')
        if t:
            return t, v
        t, v = self.__isLiteral(start, 'or')
        if t:
            return t, v
        t, v = self.__isLiteral(start, 'FILE')
        if t:
            return t, v

        if DEBUGS:
            print 'Reading token 4 '
        t = self.reTABLEREF.match(self.q[self.__qp:])
        if t != None:
            t = t.group()
            self.__qp = self.__qp + len(t)
            if DEBUGS:
                print "Token ", self.__qp, " TABLREF ", t
            self.parseDict[start] = 'TABLEREF', t, self.__qp
            return 'TABLEREF', t

        t = self.reNAME.match(self.q[self.__qp:])
        if t != None:
            t = t.group()
            self.__qp = self.__qp + len(t)
            if DEBUGS:
                print "Token ", self.__qp, " NAME ", t
            self.parseDict[start] = 'NAME', t, self.__qp
            return 'NAME', t

        if DEBUGS:
            print 'Reading token 5 '

        # = > <
        if self.__qp + 1 < len(self.q):
            t = self.q[self.__qp:self.__qp + 1]
            if "=<>*/+-".find(t) >= 0:
                if DEBUGS:
                    print "Token " + str(self.__qp) + " " + t
                self.__qp = self.__qp + 1
                self.parseDict[start] = t, t, self.__qp
                return t, t

        if DEBUGS:
            print 'Reading token 6 '

        if self.__qp + 2 < len(self.q):
            if self.q[self.__qp:self.__qp + 2] == '>=':
                if DEBUGS:
                    print "Token " + str(self.__qp) + " >="
                self.__qp = self.__qp + 2
                self.parseDict[start] = '>=', 0, self.__qp
                return ">=", 0
            if self.q[self.__qp:self.__qp + 2] == '<=':
                if DEBUGS:
                    print "Token " + str(self.__qp) + " <="
                self.__qp = self.__qp + 2
                self.parseDict[start] = '<=', 0, self.__qp
                return "<=", 0
            if self.q[self.__qp:self.__qp + 2] == '!=':
                if DEBUGS:
                    print "Token " + str(self.__qp) + " !="
                self.__qp = self.__qp + 2
                self.parseDict[start] = '!=', 0, self.__qp
                return "!=", 0
            if self.q[self.__qp:self.__qp + 2] == '<>':
                self.__qp = self.__qp + 2
                self.parseDict[start] = '<>', 0, self.__qp
                return "<>", 0

        # string
        t = self.reSTRING.match(self.q[self.__qp:])
        if t != None:
            t = t.group()
            self.__qp = self.__qp + len(t)
            t = t[1:-1]
            if DEBUGS:
                print "Token " + str(self.__qp) + " CONST " + t
            self.parseDict[start] = 'CONST', str(t), self.__qp
            return 'CONST', str(t)

        return self.q[self.__qp], 0

    def __pStatement(self):
        token, v = self.__pExpression()
        ntoken, nv = self.__readToken()
        if ntoken == 'END':
            if token:
                return token, v
        raise CommandException(8, "Illegal query: expecting END")

    def __pToken(self):
        token, v = self.__readToken()
        if DEBUG:
            print 'Got ', token, " ", v
        if token == 'CONST':
            if DEBUG:
                print 'Token: CONST ', v
            return 'CONST', v

        return self.__evalVar(token, v)

    def __pEvaluateOp(self, op, exp1, exp2):
        if DEBUG:
            print 'Evaluating operation: ', exp1, op, exp2
        if op == '=':
            return 'CONST', exp1 == exp2
        if op == '>':
            return 'CONST', exp1 > exp2
        if op == '<':
            return 'CONST', exp1 < exp2
        if op == '<=':
            return 'CONST', exp1 <= exp2
        if op == '>=':
            return 'CONST', exp1 >= exp2
        if op == '!=':
            return 'CONST', exp1 != exp2
        if op == '+':
            return 'CONST', exp1 + exp2
        if op == '-':
            return 'CONST', exp1 - exp2
        if op == '*':
            return 'CONST', exp1 * exp2
        if op == '/':
            return 'CONST', exp1 / exp2
        if op == 'or':
            return 'CONST', exp1 or exp2
        if op == 'and':
            return 'CONST', exp1 and exp2
        raise CommandException(8, 'Illegal query: unknown op' + op)

    def __evalVar(self, token, v):
        if DEBUG:
            print "Evaluating variable ", token, " : ", v

        if token == 'NAME':
            v = self.mainTable + ':' + v
            token = 'TABLEREF'

        if token == 'TABLEREF':
            table, v = v.split(':', 1)
            # Ok, that's a hack, it really only helps for absolute paths...
            if DEBUG:
                print 'TABLE BEFORE ', table
            if table not in self.tables:
                mdtable, table = self.loadTable(table)
            if DEBUG:
                print 'TABLE AFTER ', table
            mdtable = self.tables[table]
            # No entries in table: return None
            if mdtable.currentRow < 0:
                return None, 0
            row = mdtable.entries[mdtable.currentRow]
            if DEBUG:
                print 'Evaluating ', table, v, row
            if v == 'FILE':
                return 'CONST', str(row[0])
            if v not in mdtable.attributeDict:
                raise CommandException(8, 'Illegal query, unknown ' + v)
            if mdtable.typeDict[v] == 'int':
                if row[mdtable.attributeDict[v] + 1] == '':
                    return 'CONST', None
                nv = int(row[mdtable.attributeDict[v] + 1])
            elif mdtable.typeDict[v] == 'float' or mdtable.typeDict[v] == 'double':
                nv = float(row[mdtable.attributeDict[v] + 1])
                if DEBUG:
                    print 'CONST ', row[mdtable.attributeDict[v] + 1]
            else:
                nv = str(row[mdtable.attributeDict[v] + 1])
            return 'CONST', nv

        if token == 'FILE':
            return 'CONST', str(row[0])
        return None, 0

    def __pExpression(self):
        if DEBUG:
            print "Expression: Trying x and/or  y"
        myi = self.__qp
        if DEBUG:
            print 'Expression: was ' + str(self.__qp)
        token, v = self.__pComparison()
        if DEBUG:
            print 'Expression: now ' + str(self.__qp)
        if token:
            if DEBUG:
                print 'Comp ' + str(v)
            myi2 = self.__qp
            DEBUGS = True
            op, opv = self.__readToken()
            DEBUGS = False
            if DEBUG:
                print 'Expression token: ' + str(op) + str(opv)
            if op == 'and' or op == 'or':
                ntoken, nv = self.__pExpression()
                if not ntoken:
                    raise CommandException(8, 'Illegal query: expecting exp')
                if DEBUG:
                    print 'Expression: reducing a op b'
                return self.__pEvaluateOp(op, v, nv)
            self.__qp = myi2
        self.__qp = myi

        if DEBUG:
            print 'Expression: Trying comp' + str(self.__qp)
        token, v = self.__pComparison()
        if not token:
            raise CommandException(8, 'Illegal query: expecting comp')
        if DEBUG:
            print "Expression: Found comp " + str(v)
        return token, v

    def __pComparison(self):
        if DEBUG:
            print "Comparison: Trying x =><  y"
        myi = self.__qp
        if DEBUG:
            print 'Comparison: was ' + str(self.__qp)
        token, v = self.__pSum()
        if DEBUG:
            print 'Comparison: now ' + str(self.__qp)
        if token:
            myi2 = self.__qp
            op, opv = self.__readToken()
            if DEBUG:
                print 'Comparison: got op ' + op + ' '
            if op == '=' or op == '>' or op == '<' \
                    or op == '<=' or op == '>=' or op == '!=':
                ntoken, nv = self.__pComparison()
                if not ntoken:
                    raise CommandException(8, 'Illegal query: expecting comp')
                if DEBUG:
                    print 'Comparison: reducing a op b'
                return self.__pEvaluateOp(op, v, nv)
            self.__qp = myi2
        self.__qp = myi

        if DEBUG:
            print 'Comparison: Trying sum' + str(self.__qp)
        token, v = self.__pSum()
        if not token:
            raise CommandException(8, 'Illegal query: expecting sum')
        if DEBUG:
            print "Comparison: Found sum " + str(v)
        return token, v

    def __pSum(self):
        if DEBUG:
            print "Sum: Trying x +- y"
        myi = self.__qp
        if DEBUG:
            print 'Sum: was ' + str(self.__qp)
        token, v = self.__pTerm()
        if DEBUG:
            print 'Sum: now ' + str(self.__qp)
        if token:
            if DEBUG:
                print 'Sum ' + str(v)
            myi2 = self.__qp
            op, opv = self.__readToken()
            if op == '+' or op == '-':
                ntoken, nv = self.__pSum()
                if not ntoken:
                    raise CommandException(8, 'Illegal query: expecting term')
                return self.__pEvaluateOp(op, v, nv)
            self.__qp = myi2
        self.__qp = myi

        if DEBUG:
            print 'Sum: Trying term' + str(self.__qp)
        token, v = self.__pTerm()
        if not token:
            raise CommandException(8, 'Illegal query: expecting term')
        if DEBUG:
            print "Sum: Found term " + str(v)
        return token, v

    def __pTerm(self):
        if DEBUG:
            print "Term: Trying x */ y"
        myi = self.__qp
        if DEBUG:
            print 'Term: I was ' + str(self.__qp)
        token, v = self.__pFactor()
        if DEBUG:
            print 'Term: I now ' + str(self.__qp)
        if token:
            if DEBUG:
                print "Term: " + str(v)
            myi2 = self.__qp
            op, opv = self.__readToken()
            if op == '*' or op == '/':
                ntoken, nv = self.__pTerm()
                if not ntoken:
                    raise CommandException(8, 'Illegal query: expecting fact')
                return self.__pEvaluateOp(op, v, nv)
            self.__qp = myi2
        self.__qp = myi

        if DEBUG:
            print 'Term: Trying faktor' + str(self.__qp)
        token, v = self.__pFactor()
        if not token:
            raise CommandException(8, 'Illegal query: expecting factor')
        if DEBUG:
            print "Term: Found factor " + str(v)
        return token, v

    def __pFactor(self):
        if DEBUG:
            print "Factor: Trying ()"
        myi = self.__qp
        if DEBUG:
            print 'Factor: I was ' + str(self.__qp)
        token, v = self.__readToken()
        if DEBUG:
            print "Factor: now " + str(self.__qp)
        if token == '(':
            token, v = self.__pExpression()
            ntoken, nv = self.__readToken()
            if ntoken != ')':
                raise CommandException(8, 'Illegal query: expecting )')
            return token, v
        self.__qp = myi

#        if DEBUG:
        if DEBUG:
            print "Factor: Trying Token"
        token, v = self.__pToken()

        if not token:
            raise EmptyTableException
        if DEBUG:
            print "Factor: Found token " + str(v)
        return token, v
