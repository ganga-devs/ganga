

'Utilities for string manipulation'

import textwrap

import re
_ident_expr = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')


def is_identifier(x):
    'return true if a string is valid python identifier'
    return bool(_ident_expr.match(x))


def drop_spaces(x):
    'drop all spaces from a string, so that "this is an example " becomes "thisisanexample"'
    return x.replace(' ', '')


class ItemizedTextParagraph(object):

    """Format a paragraph with itemized text.

    <------------------ width ------------->

    Example: (*)
      (#)
      item1  (**)  description1
      (#)
      item2  (**)  description2 which is
                   very long but breaks OK
      (#)
      item3  (**)

    (*) is a head of the paragraph
    (**) is a separator
    (#) is a line separator
    """

    __slots__ = ('head', 'head_size', 'items', 'desc', 'width', 'sep', 'sep_size', 'linesep')

    def __init__(self, head, width=80, separator=' ', linesep=None):
        self.head = head
        self.head_size = len(self.head)
        self.items = []
        self.desc = []
        self.width = width
        self.sep = separator
        self.sep_size = len(self.sep)
        self.linesep = linesep

    def addLine(self, item, description):
        self.items.append(item)
        self.desc.append(description)

    def getString(self):

        maxitem = 0
        for it in self.items:
            if len(it) > maxitem:
                maxitem = len(it)

        indent = ' ' * int(self.head_size*0.5)

        buf = self.head + '\n'

        for it, d in zip(self.items, self.desc):
            if self.linesep is not None:
                buf += self.linesep + '\n'
            buf2 = '%-*s%s%s' % (maxitem, it, self.sep, d)
            buf += textwrap.fill(buf2, width=self.width, initial_indent=indent,
                                 subsequent_indent=' ' * (maxitem + self.sep_size) + indent) + '\n'

        return buf


if __name__ == "__main__":
    assert(is_identifier('a'))
    assert(not is_identifier('1'))
    assert(is_identifier('a1'))
    assert(not is_identifier('a1 '))
    assert(not is_identifier(' a'))
    assert(not is_identifier('a b'))
    assert(is_identifier('_'))

    assert(drop_spaces(' a b c ') == 'abc')
    assert(drop_spaces('abc') == 'abc')

    it = ItemizedTextParagraph('Functions:')
    it.addLine('dupa', 'jerza;fdlkdfs;lkdfkl;')
    it.addLine('dupaduuza', 'jerza;fdlkdfs;lkdfkl;')
    it.addLine('d', 'jerza;fdlkdfs;lkdfkl;')

    print(it.getString())

    print('strings: Test Passed OK')

