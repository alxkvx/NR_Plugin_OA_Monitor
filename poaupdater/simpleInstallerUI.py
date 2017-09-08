#!/usr/bin/python
import re

import uDialog
import uLogging


def showLicense(title, license):
    pass  # not used in batch mode


def initialize():
    pass

only_letters_pattern = re.compile(r"[a-z]+")
email_pattern = re.compile(r"(\w|-)+@(\w|-)+\.(\w|-)+")
hostname_pattern = re.compile(r"(\w|-)+\.(\w|-)+")


def configure(default_parameters):
    pass  # not used in batch mode


class InstallationProgress:

    def __init__(self):
        pass

    def set_progress(self, progress, msg=None, dbg=False):
        if msg:
            if dbg:
                uLogging.debug(msg)
            else:
                uLogging.info(msg)

    def new_phase(self, msg):
        uLogging.debug(msg)

    def end_phase(self):
        pass


def choose_modules(build, props):
    pass  # not used in batch mode


def askYesNo(question, default=None):
    return uDialog.askYesNo(question, default)


def message(title, text):
    print
    print "=" * 70
    print title
    print text
    print "=" * 70
    print


def congrat(text):
    uLogging.info(text)


def error(text):
    uLogging.err(text)
    message("ERROR", text)
