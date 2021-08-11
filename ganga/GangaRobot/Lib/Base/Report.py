"""Object model for report data as handled by base action implementations.

The Report class represents a report containing a title and a list of lines,
where each line is a list of elements, and any object can be used as an element.

The Heading, Link, Pre, and Table classes can be used as elements in a report to
provide simple plain text and HTML formatting. 

"""

def _tohtml(element):
    """Return HTML version of element, or string version if no tohtml method."""
    if _ishtml(element):
        return element.tohtml()
    else:
        return str(element)

def _ishtml(element):
    """Return true if the element has a tohtml method."""
    import inspect
    return hasattr(element, 'tohtml') and inspect.ismethod(getattr(element, 'tohtml'))


class Report(object):
    
    """Simple object model of a report.
    
    This is essentially a title and a list of lines, where each line is a list
    of elements. Any object can be used as an element. When converting to plain
    text, str() is used. When converting to HTML, the element's tohtml() method
    is used if present, otherwise str() is used.
    
    Methods are provided for adding lines and elements, see addline() and
    addelement(), respectively.
    
    The title and lines can also be accessed directly via the title and lines
    attributes, respectively.

    In addition methods are provided to convert to plain text and HTML, see
    __str__() and tohtml(), respectively.
    
    Example (code / output):

    r = Report('The Report')
    r.addline(Heading('Results', 2))
    r.addline()
    t = Table('%-10s | %10s', ('Area', 'Total'), '%-10s | %10d')
    t.addrow('Research', 8)
    t.addrow('Sales', 5)
    r.addline(t)
    r.addline()
    r.addline('Follow this link:')
    r.addelement(Link('data', 'http://localhost'))
    print(str(r))
    print()
    print(r.tohtml())

    The Report
    **********
    
    Results
    *******
    
    Area       |      Total
    -----------------------
    Research   |          8
    Sales      |          5
    
    Follow this link: data (http://localhost)
    
    <html>
    <head>
    <title>The Report</title>
    </head>
    <body>
    <h1>The Report</h1>
    <h2>Results</h2>
    <br/>
    <table border="1">
    <tr><th>Area</th><th>Total</th></tr>
    <tr><td>Research</td><td>8</td></tr>
    <tr><td>Sales</td><td>5</td></tr>
    </table>
    <br/>
    Follow this link: <a href="http://localhost" >data</a>
    <br/>
    </body>
    </html>
    
    """
    
    def __init__(self, title = None, lines = None):
        """Create a new report.
        
        Keyword arguments:
        title -- Optional string title.
        lines -- Optional list of lines, where each line is a list of elements.
            If None then defaults to an empty list.
        
        """
        self.title = title
        if lines is None:
            lines = []
        self.lines = lines # a list (lines) of list (elements)
    
    def addline(self, line = None):
        """Add a new line.
        
        Keyword arguments:
        line -- Optional list of elements. If None then an empty list is added.
            If not a list then [line] is added.
            
        Example:
        report.addline('Some text')
        report.addline(['Some', 'text'])
        report.addline()
        report.addline(Heading('My Heading', 2))
        report.addline(['Follow this link:', Link('My Machine', 'http://localhost')])
        
        """
        if line is None:
            line = []
        elif not isinstance(line, list):
            line = [line]
        self.lines.append(line[:])
        
    def addelement(self, element = ''):
        """Add a new element to the last line.
        
        Keyword arguments:
        element -- Optional element. If lines is empty then a new line [element]
            is added.
        
        Example:
        report.addelement('Follow this link:')
        report.addelement(Link('my machine', 'http://localhost'))

        """
        if self.lines:
            self.lines[-1].append(element)
        else:
            self.lines.append([element])
    
    def __str__(self):
        """Return a plain text representation.
        
        The title, if defined, is added as a level 1 Heading followed by a blank
        line.
        
        The lines are concatenated with '\\n' as separator, the elements of each
        line being converted to plain text using str() and concatenated with ' '
        as separator.
        
        """
        texts = []
        if self.title:
            texts.append(str(Heading(self.title, 1)))
            texts.append('')
        for line in self.lines:
            text = ' '.join([str(element) for element in line])
            texts.append(text)
        return '\n'.join(texts)
    
    def tohtml(self):
        """Return an HTML representation.
        
        The title, if defined, is added as a level 1 Heading, and used in the
        html/head/title tag.
        
        The lines are concatenated with a '<br/>\\n' as separator, the elements
        of each line being converted to HTML using their tohtml() method, if
        defined, or str() otherwise, and concatenated with ' ' as separator.
        
        N.B. If a line contains a single element which has an HTML
        representation, i.e. defines tohtml(), then no '<br/>' is appended. This
        is to avoid excessive newlines after headings and tables etc.
        
        """
        htmls = []
        htmls.append('<html>')
        htmls.append('<head>')
        if self.title:
            htmls.append('<title>%s</title>' % self.title)
        htmls.append('</head>')
        htmls.append('<body>')
        if self.title:
            htmls.append(_tohtml(Heading(self.title, 1)))
        for line in self.lines:
            # if line not empty
            if line:
                html = ' '.join([_tohtml(element) for element in line])
                htmls.append(html)
            # do not add <br/> after single html element
            # i.e. to avoid excessive lines after headings, tables etc.
            if len(line) != 1 or not _ishtml(line[0]):
                htmls.append('<br/>')
        htmls.append('</body>')
        htmls.append('</html>')
        return '\n'.join(htmls)


class Heading(object):
    
    """Heading element providing plain text and HTML representations.

    Example (code / output):

    h = Heading('Introduction', 2)
    print(str(h))
    print()
    print(h.tohtml())

    Introduction
    ************

    <h2>Introduction</h2>
    
    """
    
    _levelchars = ('*', '*', '=', '=', '-', '-')
    
    def __init__(self, element, level = 3):
        """Create new heading element.
        
        Keyword arguments:
        element -- Any object, possibly with tohtml() method.
        level -- Integer 1 - 6, indicating level (1 is most important).
        
        """
        self.element = element
        self.level = level
    
    def __str__(self):
        """Return a plain text representation. See class documentation."""
        texts = []
        text = str(self.element)
        texts.append(text)
        texts.append(Heading._levelchars[self.level - 1] * len(text))
        return '\n'.join(texts)
    
    def tohtml(self):
        """Return an HTML representation. See class documentation.
        
        The element is converted to HTML using its tohtml() method, if defined,
        or str() otherwise.
        
        """
        return '<h%s>%s</h%s>' % (self.level, _tohtml(self.element), self.level)


class Link(object):
    
    """Link element providing plain text and HTML representations.

    Example (code / output):

    l = Link('My Machine', 'http://localhost')
    print(str(l))
    print()
    print(l.tohtml())

    My Machine (http://localhost)
    
    <a href="http://localhost" >My Machine</a>
    
    """

    def __init__(self, element, location):
        """Create a new link element.
        
        Keyword arguments:
        element -- Any object, possibly with tohtml() method.
        location -- Hypertext reference.
        
        """
        self.element = element
        self.location = location
        
    def __str__(self):
        """Return a plain text representation. See class documentation."""
        return '%s (%s)' % (self.element, self.location)
    
    def tohtml(self):
        """Return an HTML representation. See class documentation.
        
        The element is converted to HTML using its tohtml() method, if defined,
        or str() otherwise.
        
        """
        return '<a href="%s" >%s</a>' % (self.location, _tohtml(self.element))
        

class Pre(object):
    
    """Pre element providing plain text and HTML representations.

    Example (code / output):

    p = Pre('First line of pre-formatted text.\\nSecond line.')
    print(str(p))
    print()
    print(p.tohtml())

    First line of pre-formatted text.
    Second line.

    <pre>First line of pre-formatted text.
    Second line.</pre>
    
    """

    def __init__(self, text):
        """Create a new pre element.
        
        Keyword arguments:
        text -- Any object.
        
        """
        self.text = text
    
    def __str__(self):
        """Return a plain text representation. See class documentation."""
        return self.text
    
    def tohtml(self):
        """Return an HTML representation. See class documentation."""
        return '<pre>%s</pre>' % self.text


class Table(object):
    
    """Table element providing plain text and HTML representations.
    
    The format and content can be set via the constructor or directly via the
    hformat, header, rformat and rows attributes.
    
    A convenience method addrow() is also provided.

    Example (code / output):

    t = Table()
    t.hformat = '%-20s | %8s'
    t.header = ('Status', 'Total')
    t.rformat = '%-20s | %8d'
    t.addrow('submitted', 3)
    t.addrow('failed', 2)
    t.addrow('completed', 5)
    print(str(t))
    print()
    print(t.tohtml())

    Status               |    Total
    -------------------------------
    submitted            |        3
    failed               |        2
    completed            |        5
    
    <table border="1">
    <tr><th>Status</th><th>Total</th></tr>
    <tr><td>submitted</td><td>3</td></tr>
    <tr><td>failed</td><td>2</td></tr>
    <tr><td>completed</td><td>5</td></tr>
    </table>
    
    """

    def __init__(self, hformat = None, header = None, rformat = None, rows = None):
        """Create a new table element. See class documentation.
        
        Keyword arguments:
        hformat -- Optional header format, using Python string formatting.
        header -- Optional header tuple, tuple length should match hformat.
        rformat -- Optional row format, using Python string formatting.
        rows -- Optional list of row tuples, tuple length should match rformat.
        
        """
        self.hformat = hformat
        self.header = header
        self.rformat = rformat
        if rows is None:
            rows = []
        self.rows = rows # a list (rows) of tuple (cells)
        
    def addrow(self, *cells):
        """Add a new row.
        
        Argument list:
        *cells -- The row tuple, tuple length should match rformat, if defined.
        
        """
        self.rows.append(cells)
        
    def __str__(self):
        """Return a plain text representation. See class documentation.
        
        The header is created using hformat, if defined, or by concatenating
        header cells using ' ' as separator. Similarly, the rows are created
        using rformat, if defined, or by concatenating row cells using ' ' as
        separator.
        
        """
        texts = []
        if self.header:
            if self.hformat:
                text = self.hformat % self.header
            else:
                text = ' '.join(self.header)
            texts.append(text)
            texts.append('-' * len(text))
        for row in self.rows:
            if self.rformat:
                texts.append(self.rformat % row)
            else:
                texts.append(' '.join(row))
        return '\n'.join(texts)
    
    def tohtml(self):
        """Return an HTML representation. See class documentation.
        
        The row cells are converted to HTML using their tohtml() method, if
        defined, or str() otherwise.
        
        """
        htmls = []
        if self.header or self.rows:
            htmls.append('<table border="1">')
            if self.header:
                html = '<tr>'
                for cell in self.header:
                    html += '<th>%s</th>' % _tohtml(cell)
                html += '</tr>'
                htmls.append(html)
            for row in self.rows:
                html = '<tr>'
                for cell in row:
                    html += '<td>%s</td>' % _tohtml(cell)
                html += '</tr>'
                htmls.append(html)
            htmls.append('</table>')
        return '\n'.join(htmls)
        
