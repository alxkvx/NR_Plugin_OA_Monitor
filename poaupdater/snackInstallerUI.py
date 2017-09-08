#!/usr/bin/python

import sys
import socket
import os
import signal

import uLogging
import uUtil
import uLinux

__pychecker__ = "blacklist=snack"
from snack import *


# we want escape
ESC = chr(27)
hotkeys[ESC] = ord(ESC)
hotkeys[ord(ESC)] = ESC

Ok_Cancel_Names = ["Ok", ("Cancel", 'cancel', ESC)]


_screen = None


def scallback():
    if _screen is not None:
        _screen.suspend()
        os.kill(os.getpid(), signal.SIGSTOP)
        _screen.resume()


def initScreen():
    global _screen
    _screen = SnackScreen()
    _screen.suspendCallback(scallback)


def askYesNo(question, default=None):
    answer = ButtonChoiceWindow(_screen, "?", question, ["Yes", ("No", "no", ESC)])

    if answer:
        return answer.lower() == 'yes'
    else:
        return default


def showLicense(title, text):
    initScreen()
    try:
        width = _screen.width - _screen.width / 6
        rv = ButtonChoiceWindow(_screen, title, text,  [("I agree", 'ok'), ("I do not agree", 'cancel', ESC)], width)

        return rv.lower() == 'ok'
    finally:
        _screen.finish()


def message(title, text, btns):
    initScreen()
    try:
        width = _screen.width - _screen.width / 6
        ButtonChoiceWindow(_screen, title, text,  btns, width)
    finally:
        _screen.finish()


def congrat(text):
    message("CONGRATULATIONS!", text, [("Ok", "ok")])


def error(text):
    uLogging.err(text)
    message("ERROR", text, [("Close", "close")])


class InstallationProgress:

    def __init__(self):
        self.phase = None

    def new_phase(self, phase):
        uLogging.info("*** %s ***", phase)

        initScreen()
        self.phase = phase
        self.width = _screen.width * 4 / 5
        self.scale = Scale(self.width, 100)
        self.percents = 0
        self.scale.set(self.percents)
        g = Grid(1, 2)
        g.setField(self.scale, 0, 0)
        self.status = Textbox(self.width, 1, "")
        g.setField(self.status, 0, 1)
        _screen.gridWrappedWindow(g, self.phase)
        self.form = Form()
        self.form.add(self.scale)
        self.form.add(self.status)

    def set_progress(self, pval, what=""):
        if what:
            uLogging.info("*** %s ***", what)

        self.percents = pval
        self.scale.set(self.percents)
        if what:
            self.status.setText(what)
        self.form.draw()
        self.form.setTimer(1)
        self.form.run()
        _screen.refresh()

    def end_phase(self):
        if _screen:
            _screen.finish()


def choose_modules(build, modules):
    global _screen

    try:
        mprop = modules
        if mprop.lower() == "all":
            default_modules = [m.name for m in build.modules]
        else:
            default_modules = [m.strip() for m in modules.split(",")]
        initScreen()
        top_grid = Grid(1, 2)
    #	buttons_grid = Grid(2, 1)
        max_height = _screen.height * 2 / 3

        required_height = len(build.modules)
        height = min(required_height, max_height)
        mcheckboxes = CheckboxTree(height, required_height >= max_height)
        for module in build.modules:
            if module.name != 'Platform':
                mcheckboxes.append(module.description, module.name, module.name in default_modules)

        top_grid.setField(mcheckboxes, 0, 0)
        ok_cancel = ButtonBar(_screen, Ok_Cancel_Names)
        top_grid.setField(ok_cancel, 0, 1)
        _screen.gridWrappedWindow(top_grid, "Choose modules you want to install")
        form = Form()
        form.add(mcheckboxes)
        form.add(ok_cancel)
        result = form.run()
        bpressed = ok_cancel.buttonPressed(result)
        if bpressed == 'cancel':
            raise Exception("Installation is cancelled")

        return mcheckboxes.getSelection()
    finally:
        _screen.finish()


class InputElement:

    def __init__(self, title, inp, width):
        self.inp = inp
        self.grid = Grid(2, 1)
        title_elem = Textbox(width / 2, 1, title + ":")
        self.grid.setField(title_elem, 0, 0)
        self.grid.setField(inp, 1, 0)

    def activate(self, result):
        pass

    def error(self):
        return None

    def update_properties(self, properties):
        pass


class InputNetiface(InputElement):

    def __init__(self, title, variants, prefix, width):
        def iface_to_str(iface):
            name, ip, mask = iface
            return "%s (%s)" % (name, ip)

        inp = None
        self.prefix = prefix
        self.variants = variants
        if len(self.variants) == 0:
            raise Exception("Could not find any valid net interface")
        elif len(self.variants) == 1:
            inp = Textbox(width / 2, 1, iface_to_str(self.variants[0]))
        else:
            inp = Listbox(1, scroll=0, width=width / 2, showCursor=1)
            for v in self.variants:
                inp.append(iface_to_str(v), v)

        InputElement.__init__(self, title, inp, width)

    def update_properties(self, p):
        if len(self.variants) == 1:
            chosen = self.variants[0]
        else:
            chosen = self.inp.current()

        p['%s_netiface_name' % self.prefix], p['%s_ip' % self.prefix], p['%s_netmask' % self.prefix] = chosen


class InputText(InputElement):

    def __init__(self, title, default, width, password=False, propname=None, optional=False):
        inp = Entry(width / 2, text=default, scroll=1, password=password)
        InputElement.__init__(self, title, inp, width)
        self.propname = propname
        self.optional = optional

    def update_properties(self, properties):
        if self.propname is not None:
            properties[self.propname] = self.inp.value()

    def error(self):
        if self.propname is not None and not self.inp.value() and not self.optional:
            return "%s is not specified" % self.propname.capitalize()
        return None


def cmp_fn(first, second):
    if first.endswith('/') and not second.endswith('/'):
        return -1
    elif not first.endswith('/') and second.endswith('/'):
        return 1
    else:
        return cmp(first, second)


class InputFile(InputElement):

    def __init__(self, title, default, width, dialog_title):
        inp = Grid(2, 1)
        self.filename = Entry(width / 2 - 9, default, scroll=0)
        self.browse_button = CompactButton("Browse")
        self.dialog_title = dialog_title
        inp.setField(self.filename, 0, 0)
        inp.setField(self.browse_button, 1, 0)
        if default and os.path.exists(default):
            self.cwd = os.path.dirname(default)
            if not self.cwd:
                self.cwd = os.getcwd()
            else:
                self.cwd = os.path.realpath(self.cwd)
        else:
            self.cwd = os.getcwd()

        InputElement.__init__(self, title, inp, width)

    def activate(self, result):
        global _screen
        if result != self.browse_button:
            return
        uLogging.info("%s", result)
        height = _screen.height * 2 / 3
        width = 20
        box = Listbox(height, scroll=True, returnExit=1, width=width)
        chosen = None
        form = Form()
        grid = Grid(1, 1)
        grid.setField(box, 0, 0)
        form.add(grid)
        form.addHotKey(ESC)
        _screen.gridWrappedWindow(grid, self.dialog_title)
        i = 0
        selected = '..'
        while not chosen:
            files = os.listdir(self.cwd)
            for i, f in enumerate(files):
                if os.path.isdir(os.path.join(self.cwd, f)):
                    files[i] += '/'

            files.sort(cmp_fn)
            if self.cwd != '/':
                files = ['..'] + files
            for f in files:
                box.append(f[0:width], f)
            try:
                box.setCurrent(selected + '/')
            except KeyError:
                pass

            k = form.run()
            if k == ESC:
                break
            curr = box.current()
            selected = '..'
            if curr == '..':
                selected = os.path.basename(self.cwd)
                self.cwd = os.path.dirname(self.cwd)
            elif curr.endswith('/'):
                self.cwd = os.path.realpath(os.path.join(self.cwd, curr))
            else:
                chosen = os.path.join(self.cwd, curr)
            box.clear()
        _screen.popWindow()
        if chosen:
            self.filename.set(chosen)


from simpleInstallerUI import email_pattern, hostname_pattern


class InputHostname(InputText):

    def __init__(self, title, default, propname, width):
        InputText.__init__(self, title, default, width, propname=propname)

    def error(self):
        text = self.inp.value()
        if not hostname_pattern.match(text):
            return "%s: invalid hostname" % text
        return None


class InputInt(InputText):

    def __init__(self, title, default, propname, width):
        InputText.__init__(self, title, default, width, propname=propname)

    def error(self):
        text = self.inp.value()
        try:
            int(text)
        except:
            return "%s: invalid integer" % text
        return None


class InputEmail(InputText):

    def __init__(self, title, default, width):
        InputText.__init__(self, title, default, width, False, 'email')

    def error(self):
        text = self.inp.value()
        if not email_pattern.match(text):
            return "%s: invalid email address" % text
        return None

    def update_properties(self, properties):
        properties['email'] = properties['sc-MailMessenger:messenger_mail.from'] = self.inp.value()


class InputPassword(InputText):

    def __init__(self, title, default, propname, width):
        InputText.__init__(self, title, default, width, True, propname)


class RepeatPassword(InputText):

    def __init__(self, title, default, main_input, width):
        InputText.__init__(self, title, default, width, True)
        self.main_input = main_input

    def error(self):
        if self.main_input.inp.value() != self.inp.value():
            return "Passwords do not match"
        else:
            return None


def configure(default_parameters):
    try:
        rv = default_parameters.copy()
        initScreen()
        all_net_ifaces = uLinux.listNetifaces()
        width = _screen.width * 4 / 5
        good_net_ifaces = [x for x in all_net_ifaces if x[1] != '127.0.0.1']

        ctrls = []
        ctrls.append(InputText("Host name", rv.get('hostname'), width, propname='hostname'))
        ctrls.append(InputNetiface("Internal network interface", good_net_ifaces, 'communication', width))
        ctrls.append(InputNetiface("External network interface", good_net_ifaces, 'external', width))
        ctrls.append(InputText("Administrator login", rv.get("username", "admin"), width))
        ctrls.append(InputPassword("Administrator password", rv.get("password", ""), 'password', width))
        ctrls.append(RepeatPassword("Repeat administrator password", rv.get("password", ""), ctrls[-1], width))

        ctrls.append(
            InputEmail('Administrator email', rv.get('email', os.environ.get('USER', 'root') + '@' + rv.get('hostname')), width))
        ctrls.append(
            InputText("SMTP server address", rv.get('hostname'), width, propname='sc-MailMessenger:messenger_mail.smtphost'))
        ctrls.append(
            InputText("PA Central YUM repo base URL", rv.get("yum_repo_url", ""), width, propname="yum_repo_url"))
        ctrls.append(InputText("PA Central YUM repo proxy URL", rv.get(
            "yum_repo_proxy_url", ""), width, propname="yum_repo_proxy_url", optional=True))

        grid = Grid(1, len(ctrls) + 1)
        form = Form()
        for i, ctrl in enumerate(ctrls):
            grid.setField(ctrl.grid, 0, i)
            form.add(ctrl.grid)

        ok_cancel = ButtonBar(_screen, Ok_Cancel_Names)

        grid.setField(ok_cancel, 0, len(ctrls))
        form.add(ok_cancel)

        _screen.gridWrappedWindow(grid, "Installation parameters")
        done = False
        while not done:
            runResult = form.run()
            bpressed = ok_cancel.buttonPressed(runResult)
            if bpressed == 'cancel':
                raise Exception("Installation is cancelled")
            elif bpressed is None:
                for ctrl in ctrls:
                    ctrl.activate(runResult)
            elif bpressed == 'ok':
                errors = []
                for ctrl in ctrls:
                    err = ctrl.error()
                    if err is not None:
                        errors.append(err)
                if errors:
                    error_text = "Incorrect data:\n"
                    error_text += '\n'.join(errors)
                    ButtonChoiceWindow(_screen, "Error", error_text, [("Ok", 'ok', ESC)])
                else:
                    for ctrl in ctrls:
                        ctrl.update_properties(rv)

                    done = True

        for p in uUtil.stipPasswords(rv):
            uLogging.debug("%s = %s", p, rv[p])
        return rv
    finally:
        _screen.finish()


def initialize():
    uLogging.log_to_console = False
