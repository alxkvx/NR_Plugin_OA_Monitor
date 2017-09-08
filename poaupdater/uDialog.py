__rcs_id__ = """$Id$"""
import re
import sys

from uGenerics import *

no_pattern = re.compile(r"^\s*n(o)?\s*$", re.IGNORECASE)
yes_pattern = re.compile(r"^\s*y(e|es)?\s*$", re.IGNORECASE)

auto_defaults = False


__pychecker__ = "unusednames=__rcs_id__"


def init(config):
    global auto_defaults
    if hasattr(config, 'auto'):
        auto_defaults = config.auto.accept


def input(query_str):
    print query_str,
    sys.stdout.flush()
    return raw_input()


def askYesNo(question=None, default=None):
    answer = None
    if question:
        print question
    if default is not None:
        if default:
            query_string = '((Y)es/(N)o)[Yes]:'
        else:
            query_string = '((Y)es/(N)o)[No]:'
        if auto_defaults:
            print query_string
            return default
    else:
        query_string = '((Y)es/(N)o):'

    while True:
        answer = input(query_string)
        if no_pattern.match(answer):
            return False
        elif yes_pattern.match(answer):
            return True

        if default is not None:
            return default

    return None  # unreached


def get_screen_width():
    return 80


def ask(question=None, default=None, validator=None):
    if question:
        query_string = question
    else:
        query_string = ''
    if default is not None:
        query_string += ' [%s]' % default
        if auto_defaults:
            print query_string
            return default
    query_string += ':'

    while True:
        answer = input(query_string).strip()

        if answer:
            if validator is not None and not validator(answer):
                answer = None
            else:
                return answer
        elif default is not None and answer is not None:
            return default

    return None  # unreached


def askVariants(question=None, default=None, variants=None):
    if not question:
        query_string = ''
    else:
        query_string = question
    if variants is None:
        raise Exception("Variants are mandatory and should not be None")

    query_string += '(' + '/'.join(variants) + ')'
    if default is not None:
        query_string += '[%s]' % default
        if auto_defaults:
            print query_string
            return default
    query_string += ':'
    while True:
        answer = input(query_string).strip().lower()
        if not answer:
            if default:
                return default
        else:
            rv = first(variants, lambda x: x.translate(
                ''.join([chr(y) for y in xrange(0, 256)]), '()').lower().startswith(answer))
            if rv is not None:
                return rv
    return None  # unreached


def select(header, choice_map):
    if not choice_map:
        return None

    columns = [len(x) for x in header]
    useful_width = get_screen_width() - len(columns)
    full_width = len(columns) + reduce(lambda s, x: s + x, columns)
    columns = [x * useful_width / full_width for x in columns]
    full_width = len(columns) + reduce(lambda s, x: s + x, columns)

    format_str = '|'.join(["%%-%ds" % x for x in columns])

    total_columns = len(columns)
    while True:
        print format_str % tuple(header)
        print '-' * full_width
        sorted_keys = choice_map.keys()[:]
        sorted_keys.sort()
        for key in sorted_keys:
            to_print = [key]
            if type(choice_map[key]) == list or type(choice_map[key]) == tuple:
                to_print += choice_map[key][:total_columns - 1]
            else:
                to_print += [choice_map[key]]
            if len(to_print) < total_columns:
                to_print += [' '] * (total_columns - len(to_print))
            print format_str % tuple(to_print)

        print
        try:
            answered = input('Please select one:')
        except EOFError:
            return None

        if answered in choice_map:
            return answered
        elif answered.lower() in choice_map:
            return answered.lower()
        elif answered.upper() in choice_map:
            return answered.upper()
        else:
            try:
                answered = int(answered)
                if answered in choice_map:
                    return answered
            except:
                pass

    return None


def selectMany(header, choice_map):
    need_more = True
    chm_copy = choice_map.copy()
    rv = []
    while need_more:
        one = select(header, chm_copy)
        if one is None:
            return rv

        rv.append(one)
        del (chm_copy[one])
        print "You've selected items: %s" % ', '.join([str(x) for x in rv])
        need_more = chm_copy and askYesNo("Add more items?")

    return rv

# Source: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/134892


class _Getch:

    """Gets a single character from standard input.  Does not echo to the
screen."""

    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self): return self.impl()


class _GetchUnix:

    def __init__(self):
        pass

    def __call__(self):
        import sys
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


class _GetchWindows:

    def __init__(self):
        import msvcrt

    def __call__(self):
        import msvcrt
        return msvcrt.getch()

getch = _Getch()

__all__ = ['ask',  'askYesNo', 'select', 'selectMany', 'askVariants', 'getch']
