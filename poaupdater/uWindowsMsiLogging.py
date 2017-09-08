import os
import sys
import MSIInstallationProgress


class MSILogger:

    def __init__(self, file):
        self.file = open(file, "a", 0)
        self.buf = ''

    def write(self, text):
        # log to file
        self.file.write(text)

        # append to line buffer and log full lines
        self.buf += text
        lines = self.buf.splitlines(True)
        self.buf = ''
        for l in lines:
            if l.endswith('\n'):
                MSIInstallationProgress.msi_log(l.rstrip('\n'))
            else:
                self.buf += l

    def flush(self):
        pass


def setupMSILogging(file):
    logger = MSILogger(file)
    sys.stdout = logger
    sys.stderr = logger


class InstallationProgress:

    def __init__(self):
        self.phase = None
        self.ticks = 0

    def new_phase(self, phase):
        self.phase = phase
        self.ticks = 0
        MSIInstallationProgress.reset_progress()
        MSIInstallationProgress.set_message(phase)

    def end_phase(self):
        self.phase = None
        self.ticks = 0

    def set_progress(self, pval, what=""):
        if what:
            if self.phase:
                msg = self.phase + ': '
            else:
                msg = ''
            msg += what
            MSIInstallationProgress.set_message(msg)
        ticks = pval - self.ticks
        MSIInstallationProgress.increment_progress(ticks)
        self.ticks += ticks


def getProgress():
    return InstallationProgress()
