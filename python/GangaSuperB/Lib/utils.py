'''Utility library'''

from Ganga.Core import GangaException

class QuitException(GangaException):
    '''A custom exception used to stop the execution of a method without 
    print any error message to the user'''
    def __str__(self):
        return ''

def format_dict_table(rows, column_names=None, max_column_width=None, border_style=2):
    '''Returns a string representation of a tuple of dictionaries in a
    table format. This method can read the column names directly off the
    dictionary keys, but if a tuple of these keys is provided in the
    'column_names' variable, then the order of column_names will follow
    the order of the fields/keys in that variable.'''
    
    if column_names or len(rows) > 0:
        lengths = {}
        rules = {}
        if column_names:
            column_list = column_names
        else:
            try:
                column_list = rows[0].keys()
            except:
                column_list = None
        if column_list:
            # characters that make up the table rules
            border_style = int(border_style)
            if border_style >= 1:
                vertical_rule = ' | '
                horizontal_rule = '-'
                rule_junction = '-+-'
            else:
                vertical_rule = '  '
                horizontal_rule = ''
                rule_junction = ''
            if border_style >= 2:
                left_table_edge_rule = '| '
                right_table_edge_rule = ' |'
                left_table_edge_rule_junction = '+-'
                right_table_edge_rule_junction = '-+'
            else:
                left_table_edge_rule = ''
                right_table_edge_rule = ''
                left_table_edge_rule_junction = ''
                right_table_edge_rule_junction = ''
                
            if max_column_width:
                column_list = [c[:max_column_width] for c in column_list]
                trunc_rows = []
                for row in rows:
                    new_row = {}
                    for k in row.keys():
                        new_row[k[:max_column_width]] = str(row[k])[:max_column_width]
                    trunc_rows.append(new_row)
                rows = trunc_rows
                
            for col in column_list:
                rls = [len(str(row[col])) for row in rows]
                lengths[col] = max(rls+[len(col)])
                rules[col] = horizontal_rule*lengths[col]
                
            template_elements = ["%%(%s)-%ss" % (col, lengths[col]) for col in column_list]
            row_template = vertical_rule.join(template_elements)
            border_template = rule_junction.join(template_elements)
            full_line = left_table_edge_rule_junction + (border_template % rules) + right_table_edge_rule_junction 
            display = []
            if border_style > 0:
                display.append(full_line)
            display.append(left_table_edge_rule + (row_template % dict(zip(column_list, column_list))) + right_table_edge_rule)
            if border_style > 0:
                display.append(full_line)
            for row in rows:
                display.append(left_table_edge_rule + (row_template % row) + right_table_edge_rule)
            if border_style > 0:
                display.append(full_line)
            return "\n".join(display)
        else:
            return ''
    else:
        return ''

def getIndex(minInclusive=0, minExclusive=None, maxInclusive=None, maxExclusive=None):
    '''It asks the user to insert a number included into a specific range'''
    
    while True:
        try:
            s = raw_input("enter an integer or (q)uit: ")
            if s == 'q':
                raise QuitException()
            i = int(s)
            
            if minExclusive is not None:
                if i <= minExclusive:
                    continue
            else:
                if i < minInclusive:
                    continue
            
            if maxExclusive is not None:
                if i >= maxExclusive:
                    continue
            elif maxInclusive is not None:
                if i > maxInclusive:
                    continue
            else:
                raise Exception('max index is not defined')
            
            return i
        except ValueError:
            pass
        except KeyboardInterrupt:
            raise QuitException()

def sizeof_fmt_binary(num):
    '''It returns a string with a normalized representation of the given number,
    followed by the unit of measure. Binary version.'''
    if num is None:
        return "0"
    
    for x in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0

def sizeof_fmt_decimal(num):
    '''It returns a string with a normalized representation of the given number,
    followed by the unit of measure. Decimal version.'''
    if num is None:
        return "0"
    
    for x in ['', 'K', 'M', 'G', 'T']:
        if num < 1000.0:
            return "%3.1f%s" % (num, x)
        num /= 1000.0

def getTerminalSize():
    '''It returns console window width and height'''
    
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return None
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        try:
            cr = (env['LINES'], env['COLUMNS'])
        except:
            cr = (25, 80)
    return int(cr[1]), int(cr[0])

def getOwner():
    '''This method generates user string identificator using user proxy DN'''
    
    import re
    import subprocess
    
    process = subprocess.Popen(['voms-proxy-info', '-identity'], stdout=subprocess.PIPE, close_fds=True)
    outData, errData = process.communicate()
    retCode = process.poll()
    
    user_id = outData.splitlines()[0]
    pattern = re.compile(r'/.*?=')
    user_id = pattern.sub('-', user_id)
    return user_id.replace(' ', '_')[1:]
