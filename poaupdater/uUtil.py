from __future__ import generators
import sys
import os
import stat
import re
import shutil
import tempfile
import pickle
import fnmatch
import uLogging
from uConst import Const

import subprocess as sp

if Const.isWindows():
    startup_info = sp.STARTUPINFO()
    startup_info.dwFlags = sp.STARTF_USESHOWWINDOW
    startup_info.wShowWindow = sp.SW_HIDE
else:  # shut up pychecker
    startup_info = None


class DictToStruct:

    def __init__(self, dict):
        self.dict = dict

    def __getattr__(self, name):
        return self.dict[name]


class CounterCallback:

    def __init__(self):
        self.total = 0
        self.processed = 0

    def set_total(self, total):
        self.total = total

    def new_item(self, dummy):
        self.processed += 1

    def percents(self):
        return 100 * self.processed / self.total


class ExecFailed(Exception):

    def __init__(self, command, status, err=None, out=None):
        if status > 0:
            Exception.__init__(self, "%s exited with non-zero status %s, stderr: %s, stdout: %s" %
                               (command, status, err, out))
        elif status < 0:
            Exception.__init__(self, "%s is terminated by signal %s" % (command, -status))

        self.command = command
        self.status = status
        self.err = err
        self.out = out


def execCommand(command, valid_codes=None, retries=0, command_to_log=None):
    if not command_to_log:
        command_to_log = command
    uLogging.debug("Executing command: '%s'", command_to_log)

    if valid_codes is None:
        valid_exit_codes = [0]
    else:
        valid_exit_codes = valid_codes

    stdout = None
    stderr = sp.STDOUT
    if uLogging.logfile:
        stdout = uLogging.logfile
        stderr = stdout
    use_shell = type(command) not in (tuple, list)

    while True:
        if Const.isWindows():
            cmd = sp.Popen(command, stdout=stdout, stderr=stderr, shell=use_shell, startupinfo=startup_info)
        else:
            env = os.environ.copy()
            env["LANG"] = "C"
            env["LD_LIBRARY_PATH"] = env.get("LD_LIBRARY_PATH", "") + ":/usr/pgsql-9.5/lib/"
            cmd = sp.Popen(command, stdout=stdout, stderr=stderr, close_fds=True, shell=use_shell, env=env)
        status = cmd.wait()

        if status and status not in valid_exit_codes:
            uLogging.debug("Executing command '%s' failed with status '%s', '%s' retries left" % (command_to_log, status, retries))
            if retries > 0:
                retries -= 1
                continue
            raise ExecFailed(command_to_log, status)

        return status


def readCmdExt(command, input_data=None, env=None):
    uLogging.debug("Executing command: '%s'", command)
    if Const.isWindows():
        cmd = sp.Popen(command, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, startupinfo=startup_info)
    else:
        cmd = sp.Popen(command, close_fds=True, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, env=env)

    out, err = cmd.communicate(input=input_data)
    status = cmd.returncode

    return str(out), str(err), status


def readCmd(command, valid_codes=None):
    if valid_codes is None:
        valid_exit_codes = [0]
    else:
        valid_exit_codes = valid_codes

    out, err, status = readCmdExt(command)
    if status and status not in valid_exit_codes:
        raise ExecFailed(command, status, err, out)

    return out

def getSSH_ASKPASS(userPassword):
    fd, tmpname = tempfile.mkstemp()
    try:
        os.write(fd, "#!/bin/sh\n")
        os.write(fd, "rm -f '%s'\n" % tmpname)
        os.write(fd, "echo -n '%s' ; exit\n" % userPassword)
        os.write(fd, "#EOF\n")
    finally:
        os.close(fd)
    os.chmod(tmpname, 0700)
    return tmpname


def runRemoteCmd(commandText, remoteAddress, userLogin, userPassword):
    tmpname = getSSH_ASKPASS(userPassword)
    try:
        commandText = commandText.replace("'", """'\\''""")
        remoteCommand = """DISPLAY=:0 SSH_ASKPASS='%s' setsid ssh -T -q -2 -oLogLevel=error \
-oStrictHostKeyChecking=no \
-oCheckHostIP=no \
-oUserKnownHostsFile=/dev/null \
-oPreferredAuthentications=publickey,password,keyboard-interactive \
-l '%s' '%s' '%s'""" % (tmpname, userLogin, remoteAddress, commandText)
        uLogging.debug(remoteCommand)
        pp = os.popen(remoteCommand)
        o = pp.read()
        s = pp.close()
        if s is not None:
            s = int(s)
        else:
            s = 0
        if o:
            uLogging.debug(o)
        if s != 0:
            raise Exception("Command exited with status %d" % (s,))
        return o
    finally:
        if os.path.isfile(tmpname):
            os.unlink(tmpname) #delete temporary file if exists just for sure


def copyFileToRemote(localFile, remoteDir, remoteAddress, userLogin, userPassword):
    tmpname = getSSH_ASKPASS(userPassword)
    try:
        remoteCommand = """DISPLAY=:0 SSH_ASKPASS='%s' setsid scp -p -2 -oLogLevel=error \
-oStrictHostKeyChecking=no \
-oCheckHostIP=no \
-oUserKnownHostsFile=/dev/null \
-oPreferredAuthentications=publickey,password,keyboard-interactive \
'%s' '%s@%s:%s'""" % (tmpname, localFile, userLogin, remoteAddress, remoteDir)
        uLogging.debug(remoteCommand)
        pp = os.popen(remoteCommand)
        o = pp.read()
        s = pp.close()
        if s is not None:
            s = int(s)
        else:
            s = 0
        if o: uLogging.debug(o)
        if s != 0:
            raise Exception("Command exited with status %d" % (s,))
        return o
    finally:
        if os.path.isfile(tmpname):
            os.unlink(tmpname) #delete temporary file if exists just for sure


def getSSHRemoteRunner(serverAddr, masterPwd = None):
    attemptPwd = None
    while True:
        isFirst = attemptPwd is None
        if not isFirst: #not a first time attempt?
            if not sys.stdin.isatty():
                raise Exception("Current STDIN is not a TTY: TTY STDIN is required to ask password from!")
            if masterPwd is None:
                attemptPwd = getpass.getpass("Root password is required to operate on remote node\nPlease type here:")
            else:
                attemptPwd = masterPwd
            uLogging.info("Checking connectivity with the password specified...")
        else:
            attemptPwd = ""
            uLogging.info("Checking connectivity with SSH pubkeys if some are installed...")
        try:
            runRemoteCmd("python -c 'import platform; print (platform.linux_distribution()[1])'", serverAddr, "root", attemptPwd).strip()
            uLogging.info("SSH Connectivity is OK!")
            break
        except Exception, x:
            import traceback, cStringIO
            if isFirst:
                uLogging.info("Could not connect to remote node with SSH keys, asking root password...")
            else:
                uLogging.info("Could not connect to remote node with root password specified.")
            if (not isFirst) and (masterPwd is not None):
                raise x
            import cStringIO
            ostr = cStringIO.StringIO()
            traceback.print_exc(file=ostr)
            uLogging.info('-'*60)
            uLogging.info(ostr.getvalue())
            uLogging.info('-'*60)
    return lambda cmd: runRemoteCmd(cmd, serverAddr, "root", attemptPwd)


def runLocalCmd(commandText):
    uLogging.debug(commandText)
    pp = os.popen(commandText)
    o = pp.read()
    s = pp.close()
    if s is not None:
        s = int(s)
    else:
        s = 0
    if o: uLogging.debug(o)
    if s != 0:
        raise Exception("Command exited with status %d" % (s,))
    return o


def findHome():
    if Const.isWindows():
        return os.environ.get('HOMEDRIVE', 'C:') + os.environ.get('HOMEPATH', '\\')
    else:
        return os.environ.get('HOME', '/root')


class PEMHost:

    def __init__(self, host_id, name, htype, platform, rootpath, pleskd_id=None, note=None):
        self.host_id = host_id
        self.name = name
        self.type = htype
        self.platform = platform
        self.rootpath = rootpath
        self.pleskd_id = pleskd_id
        self.note = note

    def __str__(self):
        return "%s (%s)" % (self.name, self.host_id)

__prop_line_pattern = re.compile("^([^=]+)=(.*)$")


def readPropertiesFile(f, fail_on_errors=True):
    rv = {}
    line_no = 1
    for line in f:
        if line.strip().startswith('#') or not line.strip():
            continue
        pmatch = __prop_line_pattern.match(line)
        if pmatch is None:
            errmsg = "%d: '%s' bad property line" % (line_no, line)
            if fail_on_errors:
                raise Exception(errmsg)
            else:
                uLogging.err("%s", errmsg)

        else:
            rv[pmatch.group(1)] = pmatch.group(2)
        line_no += 1

    return rv


def readPropertiesResultSet(rows):
    rv = {}
    for r in rows:
        rv[r[0]] = r[1]

    return rv

if Const.isWindows():
    def moveFile(src, dst):
        if os.path.exists(dst):
            print 'destination file %s exists: trying to remove...' % dst
            os.remove(dst)
        shutil.move(src, dst)

    def ln_cp(src, dst):
        try:
            if os.path.isdir(dst):
                dst = os.path.join(dst, os.path.basename(src))
            if os.path.isfile(dst):
                os.chmod(dst, stat.S_IWRITE)
                os.unlink(dst)

            shutil.copy2(src, dst)
        except Exception, e:
            uLogging.err("%s while copying %s to %s", e.__class__, src, dst)
            raise
else:
    def moveFile(src, dst):
        os.rename(src, dst)

    def ln_cp(src, dst):
        try:
            if os.path.isdir(dst):
                dst = os.path.join(dst, os.path.basename(src))
            if os.path.isfile(dst):
                os.unlink(dst)

            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)

# TODO use shutil from python 2.6


def copytree(src, dst, symlinks=False, ignore=None):
    names = os.listdir(src)
    if ignore is not None:
        ignored_names = ignore(src, names)
    else:
        ignored_names = set()

    if not os.path.isdir(dst):
        os.makedirs(dst)
    errors = []
    for name in names:
        if name in ignored_names:
            continue
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if symlinks and os.path.islink(srcname):
                linkto = os.readlink(srcname)
                os.symlink(linkto, dstname)
            elif os.path.isdir(srcname):
                copytree(srcname, dstname, symlinks, ignore)
            else:
                shutil.copy(srcname, dstname)
                # XXX What about devices, sockets etc.?
        except (IOError, os.error), why:
            errors.append((srcname, dstname, str(why)))
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error, err:
            errors.extend(err.args[0])
    try:
        shutil.copystat(src, dst)
    except WindowsError:
        # can't copy file access times on Windows
        pass
    except OSError, why:
        errors.extend((src, dst, str(why)))
    if errors:
        raise shutil.Error(errors)


def ignore_patterns(*patterns):
    """Function that can be used as copytree() ignore parameter.

    Patterns is a sequence of glob-style patterns
    that are used to exclude files"""
    def _ignore_patterns(path, names):
        ignored_names = []
        for pattern in patterns:
            ignored_names.extend(fnmatch.filter(names, pattern))
        return set(ignored_names)
    return _ignore_patterns


def stmt_parser(code, concatOperator, nowfun):
    # it is translated to python function parseSQL from "src/library/pem_client/sqltext.cpp"
    next = ''
    quote = None
    haveSomething = eos = False
    p = 0
    while p < len(code):
        # skip comment
        if quote is None and code[p:p + 2] == '--':
            p += 2
            while p < len(code) and code[p] != '\n':
                p += 1
            if p >= len(code):
                break
        if not code[p].isspace():
            haveSomething = True
        if code[p] == "'":
            if quote == "'":
                quote = None
            elif quote is None:
                quote = "'"
        elif code[p] == '"':
            if quote == '"':
                quote = None
            elif quote is None:
                quote = '"'
        elif code[p:p + 2] == '$$':
            if quote == '$$':
                quote = None
            elif quote is None:
                quote = '$$'
            next += '$'
            p += 1
        elif code[p:p + 2] == '/*' and quote is None:
            quote = '/*'
            p += 2
            next += '/*'
        elif code[p:p + 2] == '*/' and quote == '/*':
            quote = None
            p += 2
            next += '*/'
        elif code[p:p + 2] == '||' and quote is None:
            next += concatOperator
            p += 2
        elif code[p:p + 3] == 'now' and quote is None:
            next += nowfun
            p += 3
        elif code[p] == ';':
            if quote is None:
                if haveSomething:
                    eos = True
                    yield next.strip()
                next = ''
                haveSomething = False
                p += 1
                continue
        if eos and not code[p].isspace():
            eos = False
            next += code[p]
        else:
            next += code[p]
        p += 1
    if haveSomething:
        yield next.strip()


def os_random_byte(dummy):
    return os.urandom(1)


def read_byte(f):
    return f.read(1)


def generate_random_password(length):
    arg = None
    if os.__dict__.has_key('urandom'):
        random_byte_fun = os_random_byte
    else:
        random_byte_fun = read_byte
        arg = open('/dev/urandom')

    rv = random_byte_fun(arg)
    while not rv.isalpha():
        rv = random_byte_fun(arg)

    while len(rv) < length:
        ch = random_byte_fun(arg)
        if ch.isalpha() or ch.isdigit():
            rv += ch
    if arg:
        arg.close()

    return rv


def saveObj(obj, dirname, filename):
    fd, tmpfn = tempfile.mkstemp(dir=dirname)
    f = os.fdopen(fd, "w")
    pickle.dump(obj, f)
    f.close()
    target = os.path.join(dirname, filename)
    moveFile(tmpfn, target)
    os.chmod(target, 0644)


def loadObj(dirname, filename, default):
    ffname = os.path.join(dirname, filename)
    if not os.path.exists(ffname):
        return default()
    else:
        return pickle.load(open(ffname))


def editFileSafe(filename, fun, backup_filename, *args, **kwds):
    inf = outf = None
    try:
        fstat = os.stat(filename)
        fd, tmpfn = tempfile.mkstemp(dir=os.path.dirname(filename))
        outf = os.fdopen(fd, "w")
        inf = open(filename)
        rv = fun(inf, outf, *args, **kwds)
        if backup_filename:
            ln_cp(filename, backup_filename)

        try:
            os.chmod(tmpfn, fstat.st_mode)
            os.chown(tmpfn, fstat.st_uid, fstat.st_gid)
        except:
            pass
    finally:
        if outf:
            outf.close()
        if inf:
            inf.close()
    moveFile(tmpfn, filename)
    return rv


def createPropertiesFile(properties, filename, mode=0640, uid=0, gid=0):
    fd, tmpfn = tempfile.mkstemp(dir=os.path.dirname(filename))
    f = os.fdopen(fd, "w")
    if type(properties) == dict:
        to_iter = [x for x in properties.iteritems()]
        to_iter.sort()
    else:
        to_iter = properties
    for prop, value in to_iter:
        f.write('%s=%s\n' % (prop, value))

    f.close()
    os.chmod(tmpfn, mode)
    if not Const.isWindows():
        os.chown(tmpfn, uid, gid)
    moveFile(tmpfn, filename)


def doAppendToFile(inf, outf, portion, markers=None):
    if markers is not None:
        begin, end = markers
    else:
        begin = end = None

    existing = []
    in_section = False
    for line in inf.readlines():
        if not in_section:
            if line == begin:
                in_section = True
            else:
                print >> outf, line,
        else:
            if line == end:
                in_section = False
            else:
                existing.append(line)

    portion_lines = portion.splitlines()
    same = True
    if len(portion_lines) == existing:
        for i in xrange(0, len(existing)):
            if existing[i].strip() != portion_lines[i].strip():
                same = False
                break
    else:
        same = False

    if not same:
        if begin:
            print >> outf, begin
        for line in portion_lines:
            print >> outf, line
        if end:
            print >> outf, end
    return not same


def appendToFile(filename, portion, markers=None):
    return editFileSafe(filename, doAppendToFile, None, portion, markers)


def startShell():
    shell_map = {Const.getWinPlatform() : ('COMSPEC', 'command.com'), None: ('SHELL', '/bin/sh')}
    shell = shell_map.get(sys.platform, shell_map.get(None))
    shell = os.environ.get(*shell)
    return os.system(shell)


def dumpEnviron(func):
    func("Environment variables:")
    for var in os.environ:
        func("%s=%s", var, os.environ[var])


def logLastException():
    """
    logs last exception with stack trace
    """
    import traceback
    uLogging.debug("Error trace:")
    uLogging.debug(traceback.format_exc())
    uLogging.err(str(sys.exc_info()[1]))


def replaceInFile(fileName, replaceFrom, replaceTo, regex=False):
    file = open(fileName, 'r')
    content = file.read()
    if regex:
        content = re.sub(replaceFrom, replaceTo, content)
    else:
        content = content.replace(replaceFrom, replaceTo)
    file = open(fileName, 'w')
    file.write(content)
    file.close()


def nice_time_delta(stime, etime, print_days=False, print_hours=False):
    td = etime - stime
    if (td.days > 0):
        out = re.sub(r" day(?:s)?, ", ":", str(td))
    elif print_days:
        out = "0:" + str(td)
    else:
        out = str(td)

    out_list = out.split(':')

    if not print_hours:
        out_list = out_list[1:]

    out_list = ["%02d" % (int(float(x))) for x in out_list]
    out = ":".join(out_list)
    return out


def module_install_report(name, tstart, tend):
    return "Module %s is successfully installed, took %s" % (name, nice_time_delta(tstart, tend))


def is_secret_key(key):
    pass_pattern = re.compile(".*(pass|priv_key|encryption_key|license|aps_token|secret).*", re.IGNORECASE)
    return pass_pattern.match(key)


def stipPasswords(params):
    """Search sensitive information in params dict, remove plain-password keys, return clean dict"""
    params_stipped = params.copy()
    for key, value in params.iteritems():
        if is_secret_key(key):
            params_stipped[key] = "***"
    return params_stipped


__all__ = ["execCommand", "readCmd", "readCmdExt", "findHome", "PEMHost", "readPropertiesFile", "moveFile", "ln_cp",
           "stmt_parser", "saveObj", "loadObj", "startShell", "DictToStruct", "waitTasksComplete", "logLastException",
           "replaceInFile", "nice_time_delta", "module_install_report", "stipPasswords", "is_secret_key"]
