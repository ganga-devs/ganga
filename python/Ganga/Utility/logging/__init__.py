from __future__ import print_function, absolute_import

##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.7 2009-06-25 13:30:00 moscicki Exp $
##########################################################################

#
# Thin wrapper for logging subsystem:
#  - makes standard python 2.3 logging available in python 2.2
#  - integrates loggers with Ganga configuration subsystem in a most covinient way
#  - extra reporting utilities:
#     log_user_exception
#
# Design principles:
#  - getLogger() returns the standard logger object for the current execution context
#     i.e. with the name which indicates the current package or module name
#  - all loggers are automatically configured according to this modules config dictionary (see below)
#  - special functions:
#       - log_user_exception() allows to format nicely exception messages

import cStringIO
import logging
import logging.handlers
import os.path
import sys
import threading
import traceback

# logger configuration
# settings for new loggers may be added here at will (for example read
# from the config file)

import Ganga.Utility.ColourText as ColourText

from Ganga.Utility.Config import getConfig
config = getConfig("Logging")

# initialize the root logger for the logger created directly in python
# executable scripts which have no name starting by "Ganga."
# By default everything goes to stdout

_hasInit = False

if not _hasInit:
    logging.basicConfig(stream=sys.stdout)
    _hasIinit = True

_formats = {
    'DEBUG': '%(asctime)s "%(filename)s":%(funcName)-10s at %(lineno)d, %(threadName)s: %(levelname)-8s %(message)s',
    'VERBOSE': '%(asctime)s %(name)-35s: %(levelname)-8s %(message)s',
    'NORMAL': '%(name)-35s: %(levelname)-8s %(message)s',
    'TERSE': 'Ganga: %(levelname)-8s %(message)s'
}

requires_shutdown = False

private_logger = None  # private logger of this module

# main logger corresponds to the root of the hierarchy
main_logger = logging.getLogger()

# this is the handler used to print on screen directly
direct_screen_handler = main_logger.handlers[0]  # get default StreamHandler

# if defined this is the handler used for caching background messages at
# interactive prompt
cached_screen_handler = None

# this is the handler currenty in use by main_logger (either direct_screen_handler or cached_screen_handler)
# or it may be overriden by bootstrap() to be arbitrary handler
default_handler = direct_screen_handler

# if defined this is ADDITIONAL handler that is used for the logfile
file_handler = None


# FIXME: this should be probably an option in the config
# if defined, global level (string) overrides anything which is in config
_global_level = None

# all loggers which are used by all modules
_allLoggers = {}


# use this function to get new loggers into your packages
# if you do not provide the name then logger will detect your package name
# if you specify the modulename as a string then it will be appended to the package name
# if you specify the modulename==1 then your module name will be guessed and appended to the package name
# the guessing algorithm may be modified by passing the frame object (to emulate a different physical location of the logger)
# this is only useful for special usage such as IBackend base class
def getLogger(name=None, modulename=None):
    return _getLogger(name, modulename)


class ColourFormatter(logging.Formatter, object):

    def __init__(self, *args, **kwds):
        logging.Formatter.__init__(self, *args, **kwds)
        fg = ColourText.Foreground()
        fx = ColourText.Effects()
        self.colours = {logging.INFO: fx.normal,
                        logging.WARNING: fg.orange,
                        logging.ERROR: fg.red,
                        logging.CRITICAL: fg.red,
                        logging.DEBUG: fx.normal}
        self.markup = ColourText.ANSIMarkup()

    def format(self, record):
        try:
            s = super(ColourFormatter, self).format(record)
        except TypeError:
            print("%s" % str(record))
            return None
        if record.levelno in self.colours.keys():
            code = self.colours[record.levelno]
            return self.markup(s, code)
        else:
            return s

    def setColour(self, yes):
        if yes:
            self.markup = ColourText.ANSIMarkup()
        else:
            self.markup = ColourText.NoMarkup()


def _set_formatter(handler):
    if config['_customFormat'] != "":
        for k in _formats.keys():
            _formats[k] = config['_customFormat']

    formatter = ColourFormatter(_formats[config['_format']])
    formatter.setColour(config['_colour'])
    handler.setFormatter(formatter)


def _make_file_handler(logfile, logfile_size):
    logfile = os.path.expanduser(logfile)
    global file_handler
    if logfile:
        try:
            new_file_handler = logging.handlers.RotatingFileHandler(
                logfile, maxBytes=logfile_size, backupCount=1)
        except IOError as x:
            private_logger.error('Cannot open the log file: %s', str(x))
            return
        # remove old handler if exists
        if file_handler:
            main_logger.removeHandler(file_handler)
            file_handler.flush()
            file_handler.close()
            # this is required to properly remove the file handler from the logging system
            # otherwise I/O Error at shutdown
            try:
                # WARNING: this relies on the implementation details of the
                # logging module
                del logging._handlers[file_handler]
            except KeyError:
                # don't complain if the handler was correctly unregistered by
                # the logging system
                pass

        new_file_handler.setFormatter(logging.Formatter(_formats['VERBOSE']))
        main_logger.addHandler(new_file_handler)
        file_handler = new_file_handler


# set the loglevel for a logger to a given string value (example: "DEBUG")
def _set_log_level(logger, value):

    global _global_level

    if _global_level is not None:
        value = _global_level

    # convert a string "DEBUG" into enum object logging.DEBUG
    def _string2level(name):
        if hasattr(logging, name):
            return getattr(logging, name)

    try:
        logger.setLevel(_string2level(value))
        return value
    except AttributeError as err:
        logger.error('Attribute Error: %s', str(err))
        logger.warning('possible configuration error: invalid level value (%s), using default level', value)
        return None


# reflect all user changes immediately
def post_config_handler(opt, value):

    if config is not None and '_customFormat' in config and config['_customFormat'] != "":
        for k in _formats.keys():
            _formats[k] = config['_customFormat']

    if config is not None:
        _format, colour = config['_format'], config['_colour']

    if opt in ['_format', '_customFormat']:
        badConfig = False
        if opt == "_customFormat":
            if config is not None:
                value = config['_format']
            else:
                value = None
        if _formats is not None and value in _formats:
            _format = _formats[value]
        else:
            if private_logger is not None:
                private_logger.error('illegal name of format string (%s), possible values: %s' % (str(value), _formats.keys()))
            return

    if opt == '_colour':
        colour = value

    if opt in ['_format', '_colour', '_customFormat']:
        fmt = ColourFormatter(_format)
        fmt.setColour(colour)
        direct_screen_handler.setFormatter(fmt)
        return

    if config is not None:
        logfile, logfile_size = config['_logfile'], config['_logfile_size']

    if opt in ['_logfile', '_logfile_size']:
        global file_handler
        _make_file_handler(logfile, logfile_size)
        return

    # FIXME: has no effect at runtime, should raise a ConfigError
    if opt == '_interactive_cache':
        return

    # set the logger level
    if private_logger is not None:
        private_logger.info('setting loglevel: %s %s', opt, value)

    if _set_log_level is not None and getLogger is not None:
        _set_log_level(getLogger(opt), value)


config.attachUserHandler(None, post_config_handler)
config.attachSessionHandler(None, post_config_handler)

lookup_frame_names = {}


def _guess_module_logger_name(modulename, frame=None):
    # find the filename of the calling module
    if frame is None:
        # assuming 2 nested calls to the module boundary!
        frame = sys._getframe(3)
    else:
        print('using frame from the caller')

    global lookup_frame_names


    this__file__ = None
    if '__file__' in frame.f_globals.keys():
        this__file__ = frame.f_globals['__file__']
        if this__file__ in lookup_frame_names:
            del frame
            return lookup_frame_names[this_file]
        else:
            should_store = True
    else:
        should_store = False

    # accessing __file__ from globals() is much more reliable than
    # f_code.co_filename (name = os.path.normcase(frame.f_code.co_filename))
    if this__file__ is not None:
        name = os.path.realpath(os.path.abspath(this__file__))
    else:
        # no file associated with the frame (e.g. interactive prompt, exec
        # statement)
        name = '_program_'

    # print " _guess_module_logger_name",name
    del frame

    # if private_logger:
    #    private_logger.debug('searching for package matching calling module co_filename= %s',str(name))

    # sometimes the filename is an absolute path, try to find a relative module path from the PYTHONPATH
    # and remove the trailing path -> the result will be used as the logger
    # name

    from Ganga.Utility.files import remove_prefix

    name = remove_prefix(name, sys.path)

    def remove_tail(s, tail):
        idx = s.rfind(tail)
        if idx != -1:
            return s[:idx]
        return s

    # get rid of trailing .py  .pyc .pyo
    name = remove_tail(name, '.py')

    # replace slashes with dots
    name = name.replace(os.sep, '.')

    # return full module name
    if modulename == 1:
        return name

    # remove module name
    name = remove_tail(name, '.')

    if name == 'ganga':  # interactive IPython session
        name = "Ganga.GPI"

    # return package name
    if not modulename:
        return name

    return_name = name + '.' + modulename

    if should_store is True:
        lookup_frame_names[this__file__] = return_name

    # return custom module name
    return return_name

_MemHandler = logging.handlers.MemoryHandler


# Caching will not be done for messages which are generated by the main thread.
class FlushedMemoryHandler(_MemHandler):

    def __init__(self, *args, **kwds):
        _MemHandler.__init__(self, *args, **kwds)

    def shouldFlush(self, record):
        """
        Right here is where we make the decision on what to buffer or not in the logger in the interactive mode.
        The only thread we don't want to buffer is the MainThread. All other threads are supporting threads and not of primary interest.
        The exception to this is when the MemHandler says we flush as we want to always flush then.
        """
        return (threading.currentThread().getName() == "MainThread") or \
                super(FlushedMemoryHandler, self).shouldFlush(record)


def enableCaching():
    """
    Enable caching of log messages at interactive prompt. In the interactive IPython session, the messages from monitoring
    loop will be cached until the next prompt. In non-interactive sessions no caching is required.
    """

    if not config['_interactive_cache']:
        return

    private_logger.debug('CACHING ENABLED')
    global default_handler, cached_screen_handler
    main_logger.removeHandler(default_handler)
    cached_screen_handler = FlushedMemoryHandler(1000, target=direct_screen_handler)
    default_handler = cached_screen_handler
    main_logger.addHandler(default_handler)


def _getLogger(name=None, modulename=None):

    if name is None:
        name = _guess_module_logger_name(modulename)

    if name.split('.')[0] != 'Ganga' and name != 'Ganga':
        name = 'Ganga.' + name

    if name in _allLoggers:
        return _allLoggers[name]
    else:

        logger = logging.getLogger(name)
        _allLoggers[name] = logger

        if name in config:
            thisConfig = config[name]
            _set_log_level(logger, thisConfig)

        return logger


# this bootstrap method should be called very early in the bootstrap phase of the entire system to enable
# the log level configuration to take effect...
# the optional handler overrides the default handler and is used for GUI environment
# NOTE: the additional message buffering handler is available only for the
# default handler

def bootstrap(internal=False, handler=None):

    global private_logger, main_logger

    if internal is True and private_logger is not None:
        return

    private_logger = getLogger('Ganga.Utility.logging')
    #_set_log_level(private_logger, 'CRITICAL')
    #main_logger = _getLogger('Ganga',_roothandler=1,handler=handler)

    private_logger.debug('bootstrap')

    if internal:
        _make_file_handler(config['_logfile'], config['_logfile_size'])
    else:
        # override main_logger handler
        global default_handler
        if handler is not None and handler is not default_handler:
            main_logger.removeHandler(default_handler)
            main_logger.addHandler(handler)
            default_handler = handler

    _set_formatter(default_handler)
    # main_logger.propagate = 0 # do not propagate messages upwards...
    # main_logger.addHandler(default_handler)
    if file_handler:
        main_logger.addHandler(file_handler)

    opts = filter(lambda o: o.find('Ganga') == 0, config)
    for opt in opts:

        # should reconfigure Ganga and private logger according to the config
        # file contents

        msg = 'logging %s in %s mode' % (opt, config[opt])
        if internal:
            private_logger.debug(msg)
        else:
            private_logger.info(msg)

        _set_log_level(getLogger(opt), config[opt])

    if not internal:
        class NoErrorFilter(logging.Filter):
            """
            A filter which only allow messages which are WARNING or lower to be logged
            """
            def filter(self, record):
                return record.levelno <= 30

        # Make the default handler not print ERROR and CRITICAL
        direct_screen_handler.addFilter(NoErrorFilter())

        # Add a new handler for ERROR and CRITICAL which prints to stderr
        error_logger = logging.StreamHandler(sys.stderr)
        error_logger.setLevel(logging.ERROR)
        _set_formatter(error_logger)
        main_logger.addHandler(error_logger)

    global requires_shutdown
    requires_shutdown = True

    private_logger.debug('end of bootstrap')


def final_shutdown():
    private_logger.debug('shutting down logsystem')
    logging.shutdown()


# do the initial bootstrap automatically at import -> this will bootstrap
# the main and private loggers
bootstrap(internal=True)

# force all loggers to use the same level


def force_global_level(level):
    if level is not None:
        for l in _allLoggers:
            _set_log_level(_allLoggers[l], level)
        global _global_level
        _global_level = level


def log_user_exception(logger=None, debug=False):
    buf = cStringIO.StringIO()
    traceback.print_exc(file=buf)
    banner = 10 * '-' + ' error in user/extension code ' + 10 * '-'
    if not logger:
        logger = private_logger
    logmethod = logger.warning
    if debug:
        logmethod = logger.debug
    logmethod(banner)
    for line in buf.getvalue().splitlines():
        logmethod(line)
    logmethod('-' * len(banner))


def log_unknown_exception():
    """
    This is convenince function which is used to track down what exceptions
    are being caught by ambiguous 'except' clauses.
    It should only be called from within an exception handler.
    """
    tb_logger = getLogger('Ganga.Utility.logging.log_unknown_exception')

    # Fetch the place from where this function was called to locate the bare
    # except
    from inspect import getframeinfo, stack
    caller = getframeinfo(stack()[1][0])

    tb_logger.debug('Bare except clause triggered {0}:{1}'.format(caller.filename, caller.lineno))
    tb_logger.debug('Exception caught:', exc_info=True)
