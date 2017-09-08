import sys
import codecs
import time
import logging
import os
import shutil

__record = None

logfile = None
do_time = True

ext_log_dump = None
log_to_console = True	# need to turn off console logging in UI installer


def failed_msg(prio):
    return "WARNING: failed to log '%s' message, got exception: %s: %s\n" % (prio, str(sys.exc_info()[0]), str(sys.exc_info()[1]))


def timestamp():
    now = time.time()
    return time.strftime('%Y-%m-%d %H:%M:%S') + '.%03d' % int((now - int(now)) * 1000)


def print_log(stream, prio, message, *msg_args):
    if len(msg_args) == 0:
        msg = message
    else:
        msg = (message % msg_args)
    if prio:
        to_write = "%5s %s\n" % (prio, msg)
    else:
        to_write = msg + '\n'
    if do_time:
        to_write = timestamp() + ' ' + to_write

    if type(to_write) == unicode:
        to_write_encoded = to_write.encode('utf8')
    else:
        to_write_encoded = to_write

    if logfile:
        try:
            logfile.write(to_write_encoded)
        except:
            logfile.write(failed_msg(prio))
        logfile.flush()

    if __record and prio in LEVELS_TO_RECORD:
        __record.write(msg + '\n')
        __record.flush()

    if stream and log_to_console:
        try:
            if isinstance(stream, codecs.Codec):
                stream.write(to_write)
            else:
                stream.write(to_write_encoded)
            stream.flush()
        except:
            sys.stderr.write(failed_msg(prio))

    if ext_log_dump:
        ext_log_dump(to_write)


def log_func(stream, prio):
    def rv(message, *args):
        try:
            return print_log(stream, prio, message, *args)
        except IOError, (errno, strerror):
            if errno != 11:
                raise IOError, (errno, strerror)
    return rv


def start_recording(record):
    global __record
    __record = record


def stop_recording():
    global __record
    __record = None

# Do not forget map log type labels changes to uLogging.Handler.emit()
# Log type labels that passed in __get_logging_func() must be the same as logging.LogRecord() levelname attribute
# https://docs.python.org/2/library/logging.html#logrecord-attributes

WARNING = "[WARNING]"
ERROR = "[ERROR]"
DEBUG = "[DEBUG]"
INFO = "[INFO]"
CRITICAL = "[CRITICAL]"

LEVELS_TO_RECORD = [INFO, WARNING, ERROR]

warn = log_func(sys.stdout, WARNING)
err = log_func(sys.stderr, ERROR)
debug = log_func(sys.stdout, DEBUG)
info = log_func(sys.stdout, INFO)
log = log_func(sys.stdout, None)

# handler for python logging library
class Handler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)

    def __get_logging_func(self, levelname):
        # default python logging doesn't define any more log levels besides standard DEBUG, INFO ..
        # but there is for example yum, which defines some more levels like INFO_1, INFO_2
        # so here we try to treat all the INFO_X levels as INFO
        if levelname.startswith(DEBUG):
            return debug
        elif levelname.startswith(INFO):
            return info
        elif levelname.startswith(WARNING):
            return warn
        elif levelname.startswith(ERROR):
            return err
        elif levelname.startswith(CRITICAL):
            return err

    def emit(self, record):
        self.__get_logging_func("[" + record.levelname + "]")(record.getMessage())

def init(config):
    init2(config.log_file, config.log_file_rotation, config.verbose)

def init2(log_file, log_file_rotation, verbose):
    if log_file:
        if log_file_rotation and os.path.exists(log_file):  # rotate previous log
            shutil.move(log_file, "%s-%s%s" %
                        (log_file.rstrip('.log'), timestamp().replace(':', '-').replace(' ', '_'), ".log"))
        global logfile
        if not os.path.exists(os.path.dirname(log_file)):
            os.makedirs(os.path.dirname(log_file))

        logfile = file(log_file, "a")

    global warn, err, debug, info, log

    # reinit log functions for win32 (after call uWindows.setupMSILogging)
    warn = log_func(sys.stdout, WARNING)
    err = log_func(sys.stderr, ERROR)
    debug = log_func(sys.stdout, DEBUG)
    info = log_func(sys.stdout, INFO)
    log = log_func(sys.stdout, None)

    if log_file_rotation:
        info('Log file has been created: %s' % log_file)
    else:
        info('Log file will be located in: %s' % log_file)

    # print debug logs on screen only in verbose mode
    if not verbose:
        debug = log_func(None, DEBUG)


def init_external(logger_instance):
    global warn, err, debug, info, log
    warn = logger_instance.warn
    err = logger_instance.error
    debug = logger_instance.debug
    info = logger_instance.info
    log = logger_instance.log
    global logfile
    logfile = logger_instance.log_file_stream


def get_certain_log_file(log_family):
    logs_dir = '/var/log/pa/'
    certain_logs_dir = log_family + "/"
    script_logfile_name = os.path.basename(sys.argv[0]).replace(".py", ".log")
    return logs_dir + certain_logs_dir + script_logfile_name

def save_traceback():
    if logfile:
        info("See additional info at %s" % logfile.name)
    import traceback
    debug(str(sys.exc_info()))
    debug(traceback.format_exc())


__all__ = ["warn", "err", "debug", "info", "log"]
