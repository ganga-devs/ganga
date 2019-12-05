

from GangaCore.Utility.ColourText import getColour
import readline
import inspect
import re
import itertools
import subprocess
import types
import operator
from GangaCore.Utility.logging import getLogger
logger = getLogger('CompletionErrorChecker')


complete_bracket_matcher = re.compile('(\([^()]*?\))')
# used to not keep track of the replacements
# def bracket_sanitiser(text):


def bracket_sanitiser(text, replacement_list=None):
    if replacement_list is None:
        replacement_list = []
    #    print "checking:", text
    match_list = complete_bracket_matcher.findall(text)
    if match_list:
        for bracket in match_list:
            #            print "found:", bracket
            ##            text = text.replace(bracket,'')
            text = text.replace(bracket, '##%d##' % len(replacement_list), 1)
#            print "text:",text
            replacement_list.append(bracket)
#            print "rl:", replacement_list
##        text = bracket_sanitiser(text)
        text = bracket_sanitiser(text, replacement_list)
    return text

bracket_replacement_matcher = re.compile('##([0-9]+?)##')


def arg_splitter(text, strip_args=True):
    replacement_list = []
    text = bracket_sanitiser(text, replacement_list)
#    print "text:", text
#    print "rl:", replacement_list
    if not replacement_list or replacement_list[-1] == '()':
        return []
    if strip_args:
        # remove () and split and strip
        args = [x.strip() for x in replacement_list.pop()[1:-1].split(',')]
    else:
        args = replacement_list.pop()[1:-1].split(',')
#    print "args:", str(args)
    for i, a in enumerate(args):
        #        print "arg:", a
        while a.find('#') != -1:
            for r in bracket_replacement_matcher.findall(a):
                #                print "replacing:", r, str(replacement_list)
                a = a.replace('##%s##' % r, replacement_list[int(r)])
        args[i] = a
    return args

open_method_matcher = re.compile('([a-zA-Z0-9_.]+?)\(.*?')


def current_class_method_and_arg(text, namespace=globals()):
    '''
    should return class_object_label, class_name, method_name, arg_num
    '''
    text = bracket_sanitiser(text)
    matches = open_method_matcher.findall(text)
    namespace.update({'types': types})
#    print "open methods:", matches
    if not matches:
        return None, None, None, None
    split_match = matches[-1].rsplit('.', 1)  # get the class object as well

    class_label = None
    class_name = None
    method_name = split_match[-1]
    if len(split_match) > 1:
        class_label = split_match[0]
        class_name = eval('%s.__class__.__name__' % class_label, namespace)
        tmp = '.'.join([class_label, method_name])
        type_obj = eval('type(%s)' % tmp, namespace)
        if eval('%s != types.FunctionType and %s != types.MethodType' % (type_obj, type_obj), namespace):
            # constructor
            class_label = tmp
            class_name = eval('%s.__class__.__name__' % tmp, namespace)
            method_name = None
    elif eval('type(%s) != types.FunctionType and type(%s) != types.MethodType' % (method_name, method_name), namespace):
        # constructor
        class_label = method_name
        class_name = method_name
        method_name = None

    arg_count = text[text.find(split_match[-1]):].count(',')

    return class_label, class_name, method_name, arg_count


class GangaCompleter(object):

    def __init__(self, func, ns):
        self.func = func
        self.ns = ns
        self.ns['inspect'] = inspect

    def shell_size(self):
        '''
        Returns the size of the calling shell

        Returns a tuple of two ints representing the shells height and width respectively
        '''
        # note in python 3 can just do
        # shutil.get_terminal_size
        size_list = subprocess.Popen(
            ['stty', 'size'], stdout=subprocess.PIPE, stdin = subprocess.DEVNULL).communicate()[0].split()
        return eval(size_list[0]), eval(size_list[1])

    # want to push all colouring into the displayer as then wont mess up text
    # strngs for python < 2.whatever who dont get it
    def colouriser(self, match, user_input):
        #        try:
        if user_input.rstrip().endswith('(') or user_input.rstrip().endswith(','):
            class_label, class_name, method_name, arg_num = current_class_method_and_arg(
                user_input, self.ns)
            args = arg_splitter(match)
            var_args = [x for x in args if '*' in x]
            non_var_args = [x for x in args if '*' not in x]
            if method_name is None:  # constructor
                match = match.replace(
                    class_name, getColour('fg.green') + class_name + getColour('fg.normal'))
                return match

            match = match.replace(
                method_name, getColour('fg.green') + method_name + getColour('fg.normal'))
            if args:
                match = match.replace(', '.join(args), '##ARGS##')

            if arg_num < len(non_var_args):
                non_var_args[arg_num] = getColour(
                    'fg.blue') + non_var_args[arg_num] + getColour('fg.normal')
            else:
                if var_args:
                    var_args = [getColour('fg.blue') + x + getColour('fg.normal') for x in var_args]
                elif arg_num > 0:
                    logger.warning('Too many arguments provided')
                    match = match.replace(')', getColour(
                        'fg.red') + ')' + getColour('fg.normal'))

            return match.replace('##ARGS##', ', '.join(itertools.chain(non_var_args, var_args)))

        if user_input.endswith('.'):
            split_text = match.split('.')
            schema_items = {}
            if eval('hasattr(%s, "_schema") and hasattr(%s, "_readonly")' % (split_text[0], split_text[0]), self.ns):
                read_only = eval('%s._readonly()' % split_text[0], self.ns)
                schema_items = eval('dict(%s._schema.allItems())' % split_text[0], self.ns)

            elif eval('hasattr(%s,"_impl") and hasattr(%s._impl, "_schema") and hasattr(%s._impl, "_readonly")' % (split_text[0], split_text[0], split_text[0]), self.ns):
                read_only = eval('%s._impl._readonly()' % split_text[0], self.ns)
                schema_items = eval('dict(%s._impl._schema.allItems())' % split_text[0], self.ns)

            # if schema_items is not None and split_text[1] in schema_items:
            current_item = schema_items.get(split_text[1], None)
            if current_item is not None:
                if read_only:
                    split_text[1] = getColour(
                        'fg.red') + split_text[1] + getColour('fg.normal')
                elif current_item['protected']:
                    split_text[1] = getColour(
                        'fg.red') + split_text[1] + getColour('fg.normal')
                else:
                    split_text[1] = getColour(
                        'fg.green') + split_text[1] + getColour('fg.normal')
                return '.'.join(split_text)

        return match
#        except:
#            import traceback
#            print traceback.format_exc()

    def error_checker(self, user_input):
        # was going to use this to make displayer smaller by factoring out the
        # error checking bit
        pass

    # be on guard that the columns might break if the largest completion used to set the
    # column width is infact a method so gets added to by the colours
    def displayer(self, substitution, matches, longest_match_length):
        #        try:
        max_line_width = self.shell_size()[1]
        # max(itertools.imap(lambda x: len(x), matches)) + 2 #two blank spaces
        column_width = longest_match_length + 2
        num_per_line = max_line_width / column_width
        # need this as if partial auto-complete happens then get duplicate text
        # this is bcoz readline.get_line_buffer() already contains the partial
        # completed text
        user_input = readline.get_line_buffer()[:readline.get_endidx()]

        class_label, class_name, method_name, arg_num = current_class_method_and_arg(
            user_input, self.ns)

        if num_per_line == 0 or user_input.strip().endswith('(') or user_input.strip().endswith(','):
            num_per_line = 1

        # constructor
        if class_name is not None and method_name is None and user_input.strip().endswith(','):
            # if user_input.strip().endswith('(') or
            # user_input.strip().endswith(','):
            tmp = arg_splitter(user_input + ')', strip_args=False)
            new = tmp[:]  # map(lambda x: x.strip(), tmp)
            wrong = False
            unrecognised = []
            for i, arg in enumerate(filter(lambda x: x != '', map(lambda x: x.strip(), new))):
                if '=' in arg:
                    split_arg = arg.split('=')
                    if split_arg[0].strip() not in eval('dict(%s._impl._schema.allItems())' % class_name, self.ns):
                        unrecognised.append((split_arg[0].strip(), class_name))
                        # include = to avoid replacing letters e.g. a in the
                        # RHS also to keep the users spaces intact
                        new[i] = new[i].replace(split_arg[
                                                0] + '=', getColour('fg.red') + split_arg[0] + getColour('fg.normal') + '=')
                elif not eval('isinstance(%s, %s)' % (arg, class_name), self.ns):
                    new[i] = new[i].replace(
                        arg, getColour('fg.red') + arg + getColour('fg.normal'))
                    wrong = True

            user_input = user_input.replace(','.join(tmp), ','.join(new))
            if wrong:
                logger.warning('Only one positional arg allowed which must be an object of the same type to copy from')
                #user_input = user_input.replace(','.join(tmp), ','.join(new))
            if unrecognised:
                for a, c in unrecognised:
                    logger.warning(
                        "Unrecognised keyword argument, '%s' is not a modifyable attribute of '%s'" % (a, c))

        colour_green = getColour('fg.green')
        colour_red = getColour('fg.red')
        #colour_blue   = getColour('fg.blue')
        colour_normal = getColour('fg.normal')

        display_str = '\n'
        for i, m in enumerate(filter(lambda x: x != '', matches)):
            coloured_text = self.colouriser(m, user_input)
            width = column_width
            if colour_green in coloured_text:
                width = column_width + len(colour_green) + len(colour_normal)
            elif colour_red in coloured_text:
                width = column_width + len(colour_red) + len(colour_normal)
            display_str += ('{0:<%d}' % width).format(coloured_text)
            if num_per_line and (i + 1) % num_per_line == 0:
                display_str += '\n'

        if num_per_line and (i + 1) % num_per_line == 0:
            display_str += '\n'
        else:
            display_str += '\n\n'
        display_str += getColour('fg.blue')
        display_str += 'In [%s]:' % len(self.ns['In'])
        display_str += getColour('fg.normal')
        display_str += user_input

        print(display_str, end='')
        readline.redisplay()
#        except:
#            import traceback
#            print traceback.format_exc()

    def build_func_string(self, line):
        #        try:
        matches = ['']
        # get the argspec
        class_label, class_name, method_name, arg_num = current_class_method_and_arg(
            line, self.ns)
        if method_name is not None and class_name is not None:
            # must use class label here so as to get things not defined working
            # like queues
            tmp_arg_spec = eval(
                'inspect.getargspec(%s.%s)' % (class_label, method_name), self.ns)
            arg_spec = inspect.ArgSpec(tmp_arg_spec.args[
                                       1:], tmp_arg_spec.varargs, tmp_arg_spec.keywords, tmp_arg_spec.defaults)
        elif method_name is not None:
            arg_spec = eval('inspect.getargspec(%s)' % method_name, self.ns)
        elif class_name is not None:  # constructor
            schema_items = None
            if eval('hasattr(%s,"_schema")' % class_name, self.ns):
                schema_items = eval(
                    '%s._schema.allItems()' % class_name, self.ns)
            elif eval('hasattr(%s,"_impl") and hasattr(%s._impl,"_schema")' % (class_name, class_name), self.ns):
                schema_items = eval(
                    '%s._impl._schema.allItems()' % class_name, self.ns)

            # note wont work for build in types as these are defined in C or
            # things with slot wrappers
            if schema_items is None:
                tmp_arg_spec = eval(
                    'inspect.getargspec(%s.__init__)' % class_name, self.ns)
                arg_spec = inspect.ArgSpec(tmp_arg_spec.args[
                                           1:], tmp_arg_spec.varargs, tmp_arg_spec.keywords, tmp_arg_spec.defaults)
            else:
                t_args = []
                t_defaults = []
                for n, item in sorted(schema_items, key=operator.itemgetter(0)):
                    if not item._meta.get('hidden', False) and not item._meta.get('protected', False):
                        t_args.append(n)
                        t_defaults.append(item._meta.get('defvalue', None))
                if not t_defaults:
                    t_defaults = None
                else:
                    t_defaults = tuple(t_defaults)
                arg_spec = inspect.ArgSpec(t_args, None, None, t_defaults)
            matches.append('%s()' % class_name)
            matches.append('%s(%s)' % (class_name, class_name))
        # else:
        #    if class_name is not None:
        #        arg_spec     = eval('inspect.getargspec(%s.%s)' % (class_name, method_name), self.ns)
        #    else:
        #        arg_spec     = eval('inspect.getargspec(%s)' % method_name       , self.ns)

        # create string
        t = ''
        if method_name is not None and class_name is not None:
            t += '.'.join([str(class_name), str(method_name)])
        elif method_name is not None:
            t += str(method_name)
        elif class_name is not None:
            t += str(class_name)
        t += '('

        if arg_spec.defaults is None:
            t += ', '.join(arg_spec.args)
        else:
            num_default_free = len(arg_spec.args) - len(arg_spec.defaults)
            if arg_spec.args[:num_default_free]:
                t += ', '.join(arg_spec.args[:num_default_free]) + ', '
            for var, deflt in zip(arg_spec.args[num_default_free:], arg_spec.defaults):
                if isinstance(deflt, str):
                    t += "%s ='%s', " % (var, deflt)
                else:
                    t += '%s =%s, ' % (var, str(deflt))

        if arg_spec.varargs is not None:
            t += '*%s, ' % arg_spec.varargs
        if arg_spec.keywords is not None:
            t += '**%s, ' % arg_spec.keywords
        if t.endswith(', '):
            t = t[:-2]  # remove trailing ,
        t += ')'
        matches.append(t)
        return matches
#        except:
#            import traceback
#            print traceback.format_exc()

    def complete(self, text, state):
        #        try:
        response = None
        whole_line = readline.get_line_buffer()
        replace_complete_start = readline.get_begidx()
        replace_complete_end = readline.get_endidx()
        if state == 0:
            if text == ''\
                    and whole_line != '' \
                    and replace_complete_start == len(whole_line) \
                    and replace_complete_end == len(whole_line):
                #cls_label, class_name, method, arg_num = current_class_method_and_arg(whole_line.rstrip())
                # print cls, method
                if whole_line.rstrip().endswith('(') or whole_line.rstrip().endswith(','):
                    # if method is not None:
                    self.matches = self.build_func_string(whole_line.rstrip())

                # elif whole_line.rstrip().endswith(','):
                    # if method is not None:
                #    self.matches=self.build_func_string(whole_line.rstrip())
                else:  # e.g. a=<tab>
                    self.matches = [self.func(text, 0)]
                    for i in itertools.count(1):
                        next = self.func(text, i)
                        if next is None:
                            break
                        self.matches.append(next)
            else:
                self.matches = [self.func(text, 0)]
                for i in itertools.count(1):
                    next = self.func(text, i)
                    if next is None:
                        break
                    self.matches.append(next)
#        except:
#            import traceback
#            print traceback.format_exc()
#            raise
        try:
            response = self.matches[state]
        except IndexError:
            response = None
        return response
