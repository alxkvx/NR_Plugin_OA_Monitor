import time
import code
import sys
import cStringIO
import pickle

import uLogging
import uUtil
import uDialog
import uSysDB
import uPrecheck


def pretty_timing(totals):
    mins, secs = divmod(abs(totals), 60)
    mins = int(mins)
    hours, mins = divmod(mins, 60)
    time_str = '%06.3f' % secs
    if mins or hours:
        time_str = '%02d' % mins + ':' + time_str
        if hours:
            time_str = '%s' % hours + ':' + time_str
    if totals < 0:
        time_str = "-" + time_str
    return time_str


class Progress:

    def __init__(self):
        self.whats = []
        self.phase = None

    def do(self, what, *args):
        now = time.time()
        self.whats.append((now, what, args))
        if what:
            msg = what
        else:
            msg = ""
        uLogging.debug("%s%s> %s", self.phase is not None and (
            '[' + self.phase + '] ') or '', '-' * len(self.whats), msg % args)

    def done(self, success=True):
        now = time.time()
        if not self.whats:
            uLogging.err("Internal error / unmatched do/done")
            return
        st, what, args = self.whats[-1]

        del self.whats[-1]
        wargs = what % args
        totals = now - st
        time_str = pretty_timing(totals)

        if success:
            uLogging.debug("%s<%s finished %s [%s]", self.phase is not None and (
                '[' + self.phase + '] ') or '', '-' * (len(self.whats) + 1), wargs, time_str)
        else:
            uLogging.debug("%s<%s %s failed [%s]", self.phase is not None and (
                '[' + self.phase + '] ') or '', '-' * (len(self.whats) + 1), wargs, time_str)

    def what(self):
        if not self.whats:
            return None
        else:
            st, what, args = self.whats[-1]
            try:
                return what % args
            except TypeError:
                return "%s %s" % (what, args)

import re
retry_pattern = re.compile(r"^\s*r(e|et|etr|etry)?\s*$", re.IGNORECASE)
ignore_pattern = re.compile(r"\s*ignore\s*", re.IGNORECASE)
abort_pattern = re.compile(r"\s*abort\s*", re.IGNORECASE)
console_pattern = re.compile(r"\s*c(o|on|ons|onso|onsol|onsole)?\s*", re.IGNORECASE)


class ActionIgnored:
    # not derived from Exception intentionally

    def __init__(self):
        pass


def ntimes_retried(fun, n=3, timeout=30):
    def fn(*args):
        for i in range(1, n + 1):
            uLogging.info('ntimes retried action %s, try %s of %s ...' % (fun.__name__, i, n))
            try:
                return fun(*args)
            except Exception, e:
                if i >= n:
                    raise e
                uLogging.info('got exception, retrying after timeout %s: %s' % (timeout, e))
                time.sleep(timeout)
    return fn


def timed(fun, progress, what, *wargs):
    def fn(*args):
        progress.do(what, *wargs)
        success = False
        try:
            rv = fun(*args)
            success = True
            return rv
        finally:
            progress.done(success=success)
    return fn

default_error_action = None


def retriable(fun, raise_on_ignore=False, allow_console=None):
    """ Automatically retries fun on Error """
    def fn(*args, **kwds):
        default_tried = False
        while True:
            try:
                return fun(*args, **kwds)
            except (KeyboardInterrupt, Exception), e:
                print_trace = True
                if isinstance(e, KeyboardInterrupt):
                    err_msg = "Interrupted"
                    print_trace = False
                elif not len(e.args):
                    err_msg = e.__class__.__name__
                else:
                    err_msg = ' '.join([str(x) for x in e.args])
                if print_trace:
                    uUtil.logLastException()
                uLogging.debug("%s occurred during %s\n%s", e.__class__, progress.what(), err_msg)
                answer = None
                question = "An error has occurred, please select next action:\n(r)etry/abort/ignore"
                if allow_console:
                    question += "/(c)onsole"
                question += ": "
                while not answer:
                    if default_error_action and not default_tried:
                        answer = default_error_action
                        default_tried = True
                    else:
                        answer = uDialog.input(question)
                    if abort_pattern.match(answer):
                        uLogging.warn("Action %s has been ABORTED" % progress.what())
                        raise
                    elif ignore_pattern.match(answer):
                        uLogging.warn("Action %s has been IGNORED" % progress.what())
                        if raise_on_ignore:
                            raise ActionIgnored()
                        else:
                            return None
                    elif retry_pattern.match(answer):
                        uLogging.warn("Retrying action %s ..." % progress.what())
                        pass
                    elif allow_console and console_pattern.match(answer):
                        console = code.InteractiveConsole(allow_console)
                        answer = None
                        try:
                            console.interact("Type sys.exit() when you're done")
                        except:
                            pass

                    else:
                        answer = None
            except:
                uLogging.err("not exception raised")
                global last_exc
                last_exc = sys.exc_info()
                print >> sys.stderr, sys.exc_info()
                raise
    return fn


def set_default_error_action(act):
    global default_error_action
    default_error_action = act


def get_default_error_action():
    global default_error_action
    return default_error_action


def nothrowable(fun):
    def nf(*args):
        try:
            return fun(*args)
        except:
            return None
    return nf


def performActions(actions, precheck=False, save_to_db=True):
    for action in actions:
        if action.id:
            con = uSysDB.connect()
            cur = con.cursor()
            cur.execute("SELECT 1 FROM updater_atomic_actions WHERE action_id = %s", action.id)
            performed = cur.fetchone()
            cur.close()

            if performed:
                uLogging.info("action %s is already performed, skipping it", action.id)
                continue

        started = time.time()
        progress.do("executing %s %s owner %s", action.kind(), action.id, action.owner)

        success = False

        if not precheck:
            try:
                action_result = retriable(action.execute, True, globals())(readonly=False, precheck=False)
                if action_result:
                    uLogging.info("Action %s result is: %s" % (action.id, action_result))
                success = True
            except ActionIgnored:
                uLogging.warn("Action %s of %s has been ignored", action.id, action.owner)
        else:  # if precheck
            try:
                record = cStringIO.StringIO()
                uLogging.start_recording(record)
                try:
                    action.execute(readonly=True, precheck=True)
                    uPrecheck.precheck_results.append((action.id, action.owner, record.getvalue().strip()))
                except Exception, e:
                    uPrecheck.precheck_results.append((action.id, action.owner, e))
                    uUtil.logLastException()
            finally:
                uLogging.stop_recording()

        done = time.time()

        actions_timing.append((action.id, started, done))

        progress.done()
        if save_to_db and not precheck and success and action.id:
            to_insert = None
            if not action_result is None:
                to_insert = pickle.dumps(action_result)
                to_insert = uSysDB.toRaw(to_insert)

            con = uSysDB.connect()
            cur = con.cursor()
            cur.execute(
                "INSERT INTO updater_atomic_actions (action_id, action_output) VALUES(%s, %s)", (action.id, to_insert))
            con.commit()


def print_timings():
    if not actions_timing:
        return
    actions_timing.sort(lambda x, y: cmp(x[2] - x[1], y[2] - y[1]))
    actions_timing.reverse()
    long_action_threshold = 180
    too_long = [x for x in actions_timing if x[2] - x[1] >= long_action_threshold]
    if not too_long:
        aid, started, finished = actions_timing[0]
        uLogging.debug("Most time consuming action is '%s': %s seconds" % (aid, pretty_timing(finished - started)))
    else:
        uLogging.debug('List of actions that took more than %d seconds:' % long_action_threshold)
        for aid, started, finished in too_long:
            uLogging.debug('%-48s %s' % (aid, pretty_timing(finished - started)))

last_exc = None
actions_timing = []
progress = Progress()

__all__ = ["progress", "retriable", "ActionIgnored", "nothrowable", "pretty_timing",
           "timed", "set_default_error_action", "get_default_error_action"]
