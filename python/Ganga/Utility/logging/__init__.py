from __future__ import print_function

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

import logging
import logging.handlers as handlers
import sys

# logger configuration
# settings for new loggers may be added here at will (for example read
# from the config file)
import Ganga.Utility.Config

import Ganga.Utility.ColourText as ColourText

import threading

# initialize the root logger for the logger created directly in python
# executable scripts which have no name starting by "Ganga."
# By default everything goes to stdout
logging.basicConfig(stream=sys.stdout)

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

config = Ganga.Utility.Config.makeConfig("Logging", """control the messages printed by Ganga
The settings are applied hierarchically to the loggers. Ganga is the name of the top-level logger which
applies by default to all Ganga.* packages unless overriden in sub-packages.
You may define new loggers in this section.
The log level may be one of: CRITICAL ERROR WARNING INFO DEBUG
""", is_open=True)

# FIXME: Ganga WARNING should be turned into INFO level when the messages
# are reviewed in all the code
config.addOption('Ganga', "INFO", "top-level logger")
config.addOption('Ganga.Runtime.bootstrap', "INFO", 'FIXME')
config.addOption('Ganga.GPIDev', "INFO", "logger of Ganga.GPIDev.* packages")
config.addOption('Ganga.Utility.logging', "WARNING", "logger of the Ganga logging package itself (use with care!)")
config.addOption('_format', "NORMAL", "format of logging messages: TERSE,NORMAL,VERBOSE,DEBUG")
config.addOption('_colour', True, "enable ASCII colour formatting of messages e.g. errors in red")
config.addOption('_logfile', "~/.ganga.log", "location of the logfile")
config.addOption('_logfile_size', 100000,
                 "the size of the logfile (in bytes), the rotating log will never exceed this file size")  # 100 K
config.addOption('_interactive_cache', True,
                 'if True then the cache used for interactive sessions, False disables caching')
config.addOption('_customFormat', "", "custom formatting string for Ganga logging\n e.g. '%(name)-35s: %(levelname)-8s %(message)s'")


class ColourFormatter(logging.Formatter):

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
        s = logging.Formatter.format(self, record)
        try:
            code = self.colours[record.levelno]
            return self.markup(s, code)
        except KeyError:
            return s

    def setColour(self, yes):
        if yes:
            self.markup = ColourText.ANSIMarkup()
        else:
            self.markup = ColourText.NoMarkup()


def _set_formatter(handler):
    if config['_customFormat'] != "":
        formatter = ColourFormatter(config['_customFormat'])
    else:
        formatter = ColourFormatter(_formats[config['_format']])
    formatter.setColour(config['_colour'])
    handler.setFormatter(formatter)


def _make_file_handler(logfile, logfile_size):
    import os.path
    logfile = os.path.expanduser(logfile)
    global file_handler
    if logfile:
        try:
            new_file_handler = handlers.RotatingFileHandler(
                logfile, maxBytes=logfile_size, backupCount=1)
        except IOError as x:
            private_logger.error('Cannot open the log file: %s', str(x))
            return
        # remove old handler if exists
        # print 'removing old file handler',file_handler
        # print 'installing new file handler',new_file_handler
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


# reflect all user changes immediately
def post_config_handler(opt, value):
    format, colour = config['_format'], config['_colour']

    if opt == '_format':
        try:
            if config['_customFormat'] != "":
                format = config['_customFormat']
            else:
                format = _formats[value]
        except KeyError, err:
            private_logger.error('illegal name of format string (%s), possible values: %s' % (str(value), _formats.keys()))
            return

    if opt == '_colour':
        colour = value

    if opt in ['_format', '_colour', '_customFormat']:
        fmt = ColourFormatter(format)
        fmt.setColour(colour)
        direct_screen_handler.setFormatter(fmt)
        return

    logfile, logfile_size = config['_logfile'], config['_logfile_size']

    if opt in ['_logfile', '_logfile_size']:
        global file_handler
        _make_file_handler(logfile, logfile_size)
        return

    # FIXME: has no effect at runtime, should raise a ConfigError
    if opt == '_interactive_cache':
        return

    # set the logger level
    private_logger.debug('setting loglevel: %s %s', opt, value)
    _set_log_level(getLogger(opt), value)

config.attachUserHandler(None, post_config_handler)
config.attachSessionHandler(None, post_config_handler)


# set the loglevel for a logger to a given string value (example: "DEBUG")
def _set_log_level(logger, value):

    if _global_level is not None:
        value = _global_level

    # convert a string "DEBUG" into enum object logging.DEBUG
    def _string2level(name):
        return getattr(logging, name)

    try:
        logger.setLevel(_string2level(value))
        #if value != 'DEBUG':
        #    return value
        #    #logging._srcfile = None
        #    #logging.logThreads = 0
        #    #logging.logThreads = 0
        return value
    except AttributeError as x:
        logger.error('%s', str(x))
        logger.warning('possible configuration error: invalid level value (%s), using default level', value)
        return None


def _guess_module_logger_name(modulename, frame=None):
    # print " _guess_module_logger_name",modulename
    # find the filename of the calling module
    import sys
    import os.path
    if frame is None:
        # assuming 2 nested calls to the module boundary!
        frame = sys._getframe(3)
    else:
        print('using frame from the caller')

    # accessing __file__ from globals() is much more reliable than
    # f_code.co_filename (name = os.path.normcase(frame.f_code.co_filename))
    try:
        name = os.path.realpath(os.path.abspath(frame.f_globals['__file__']))
    except KeyError:
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

    # return custom module name
    return name + '.' + modulename


# use this function to get new loggers into your packages
# if you do not provide the name then logger will detect your package name
# if you specify the modulename as a string then it will be appended to the package name
# if you specify the modulename==1 then your module name will be guessed and appended to the package name
# the guessing algorithm may be modified by passing the frame object (to emulate a different physical location of the logger)
# this is only useful for special usage such as IBackend base class
def getLogger(name=None, modulename=None, frame=None):
    return _getLogger(name, modulename, frame=frame)

# Caching will not be done for messages which are generated by the main thread.
class FlushedMemoryHandler(logging.handlers.MemoryHandler):

    def __init__(self, *args, **kwds):
        logging.handlers.MemoryHandler.__init__(self, *args, **kwds)

    def shouldFlush(self, record):
        return threading.currentThread().getName().find("GANGA_Update_Thread") == -1 or \
            logging.handlers.MemoryHandler.shouldFlush(self, record)


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


def _getLogger(name=None, modulename=None, _roothandler=0, handler=None, frame=None):

    if handler is None:
        handler = default_handler

    requested_name = name

    if name is None:
        name = _guess_module_logger_name(modulename, frame=frame)

    if name.split('.')[0] != 'Ganga' and name != 'Ganga':
        name = 'Ganga.' + name

    if private_logger:
        private_logger.debug('getLogger: effective_name=%s original_name=%s', name, requested_name)

    if name in _allLoggers:
        return _allLoggers[name]
        # print 'reusing existing logger: ',name
    else:

        logger = logging.getLogger(name)
        _allLoggers[name] = logger
        #logger.setLevel( logging.CRITICAL )

        if name in config:
            thisConfig = config[name]
            _set_log_level(logger, thisConfig)

        if private_logger:
            private_logger.debug('created logger %s in %s mode', name, logging.getLevelName(logger.getEffectiveLevel()))
            private_logger.debug('applied %s format string to %s', config['_format'], name)

        # print '------------>',logger
        #logger.critical('initialization of logger for module %s',name)
        # print '------------> should have printed the message'

        return logger


# this bootstrap method should be called very early in the bootstrap phase of the entire system to enable
# the log level configuration to take effect...
# the optional handler overrides the default handler and is used for GUI environment
# NOTE: the additional message buffering handler is available only for the
# default handler

def bootstrap(internal=False, handler=None):

    global private_logger, main_logger
    private_logger = getLogger('Ganga.Utility.logging')
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

        # logging.getLevelName(getLogger(opt).getEffectiveLevel()))
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

    # if internal:
    #    import atexit
    #    atexit.register(shutdown)

    global requires_shutdown
    requires_shutdown = True

    private_logger.debug('end of bootstrap')


def shutdown():
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
    import traceback
    import cStringIO
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
    from inspect import getframeinfo, stack, getsourcefile
    caller = getframeinfo(stack()[1][0])

    tb_logger.debug('Bare except clause triggered {0}:{1}'.format(caller.filename, caller.lineno))
    tb_logger.debug('Exception caught:', exc_info=True)


# extra imports for more convenient work with the logging module
getLevelName = logging.getLevelName

if __name__ == "__main__":
    print('Little test')

    private_logger = logging.getLogger("TESTLOGGER.CHILD.GRANDCHILD")
    formatter = logging.Formatter(_formats['DEBUG'])
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    private_logger.setLevel(logging.DEBUG)
    private_logger.addHandler(console)
    private_logger.critical('hello')
