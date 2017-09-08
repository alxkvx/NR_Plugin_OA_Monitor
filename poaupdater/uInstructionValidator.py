import os
import sys
import re
import cStringIO
import tempfile
import shutil
import __builtin__
import uDLModel
import getopt

from uDLModel import SQLScript, ExecScript
import uLogging
from uConfig import Config
from uConst import Const

def checkUpgradeActionNaming(instructions):

    # has to be set to:
    #     True -  for hotfixes/updates (to accept actions brought from trunk)
    #     False - for trunk
    ignore_upper_limit = False

    # be ready to bring cakes if you dare to change the list bellow:
    actionBlacklist = ["183010-APS-40668-content-type"]  # list upgrade action that should not be verified against naming conventions

    pattern = re.compile(r"(\d+)\-([A-Z]+\-\d+)([A-Za-z0-9_\-]*)")

    def check(actions, errors, min_limit, max_limit):
        for action in filter(lambda x: x.id not in actionBlacklist, actions):
            errors[action.id] = []
            if len(action.id) > 64:
                errors[action.id] += ["Action ID '%s' length is > 64 symbols: must be less than 64 symbols" % action.id]
            matches = pattern.match(action.id)
            if matches:
                identifer = matches.group(1)
                if identifer is None or len(identifer) == 0:
                    errors[action.id] += ["Action '%s' has no integer identifer specified" % action.id]
                if ignore_upper_limit:
                    if int(identifer) < min_limit:
                        errors[action.id] += ["Action '%s' has integer identifer '%s' lower than %s" %
                                              (action.id, identifer, min_limit)]
                else:
                    if int(identifer) < min_limit or int(identifer) > max_limit:
                        errors[action.id] += ["Action '%s' has integer identifer '%s' outside limits [%s; %s]" %
                                              (action.id, identifer, min_limit, max_limit)]
                issue = matches.group(2)
                if issue is None or len(issue) == 0:
                    errors[action.id] += ["Action '%s' has no linked issue specified" % action.id]
                description = matches.group(3)
                if description is None or len(description) == 0:
                    uLogging.warn("Action '%s' has no description" % action.id)
            else:
                errors[
                    action.id] += ["Action '%s' has malformed name: consider UA naming conventions described in .udl2 file" % action.id]

    errors = {}
    check(instructions.preparation.actions, errors, 170000, 171999)
    check(instructions.pre.actions, errors, 172000, 172999)
    check(instructions.actions, errors, 173000, 175999)
    check(instructions.prepkg.actions, errors, 176000, 176999)
    check(instructions.post.actions, errors, 177000, 178999)
    check(instructions.cleanup.actions, errors, 179000, 179999)

    errors_found = False
    for aid in errors:
        for item in errors[aid]:
            errors_found = True
            uLogging.err(item)

    if errors_found:
        raise Exception("Some actions have errors in their names")


def tryCheckSQL(instructions):
    actions = [x for x in instructions.preparation.actions if isinstance(x, SQLScript)]
    actions += [x for x in instructions.pre.actions if isinstance(x, SQLScript)]
    actions += [x for x in instructions.prepkg.actions if isinstance(x, SQLScript)]
    actions += [x for x in instructions.actions if isinstance(x, SQLScript)]
    actions += [x for x in instructions.post.actions if isinstance(x, SQLScript)]
    actions += [x for x in instructions.cleanup.actions if isinstance(x, SQLScript)]

    # need set some value for ConcatOperator when it is not initialized naturally (when one have database connected)
    import uSysDB
    if not uSysDB.ConcatOperator:
        uSysDB.ConcatOperator = '||'

    errors = False
    for action in actions:
        for stmt in action.parsed_code():
            kind, text = stmt
            if kind in ('CREATE', 'ALTER', 'DROP'):
                uLogging.err("Action:%s, not portable statement '%s'", action.id, stmt)
                errors = True
            elif kind in ('BEGIN', 'COMMIT', 'ROLLBACK'):
                uLogging.warn(
                    '%s statements are ignored, <SQL> action always implicitly begins and commits transaction', kind)

    if errors:
        raise Exception('Non portable SQL in some actions')

pychecker_import = None
orig_import = None


def tryCheckExec(instructions):
    actionBlacklist = []  # list upgrade action that should not be verified by pyChecker

    os.environ['PYCHECKER'] = '--limit 8196 --maxlines 8196 --maxlocals 8196'
    try:
        global pychecker_import, orig_import
        if pychecker_import is None:
            orig_import = __builtin__.__import__
            import pychecker.checker as checker
            pychecker_import = __builtin__.__import__
        else:
            __builtin__.__import__ = pychecker_import
    except ImportError, e:
        uLogging.err("Pychecker is not installed: not checking python scripts")
        return

    old_sys_path = sys.path
    tempdir = tempfile.mkdtemp()
    sys.path = [tempdir] + sys.path

    actions = [x for x in instructions.preparation.actions if isinstance(x, ExecScript)]
    actions += [x for x in instructions.pre.actions if isinstance(x, ExecScript)]
    actions += [x for x in instructions.prepkg.actions if isinstance(x, ExecScript)]
    actions += [x for x in instructions.actions if isinstance(x, ExecScript)]
    actions += [x for x in instructions.post.actions if isinstance(x, ExecScript)]
    actions += [x for x in instructions.cleanup.actions if isinstance(x, ExecScript)]

    actions = filter(lambda x: x.id not in actionBlacklist, actions)

    module_fn = os.path.join(tempdir, 'update_actions.py')
    uLogging.info("Using temporary actions file %s", module_fn)
    module = open(module_fn, 'w+')

    line_mapping = []
    ln = 1
    for action in actions:
        fcode = action.makeFunctionCode('fn_' + str(ln))
        numlines = len(fcode.splitlines())
        line_mapping.append(((ln, ln + numlines), action.script))
        module.write(fcode)
        ln += numlines

    module.close()
    out_str = cStringIO.StringIO()
    sys.stdout = out_str
    import update_actions as dummy
    __builtin__.__import__ = orig_import
    sys.path = old_sys_path
    sys.stdout = sys.__stdout__
    shutil.rmtree(tempdir)
    err_pattern = re.compile(r'\w?:?[^:]+:(\d+):(.+)$')
    errors = out_str.getvalue().splitlines()

    for errline in errors:
        match = err_pattern.match(errline)
        if not match:
            uLogging.warn("Cannot parse %s", errline)
        else:
            line = int(match.group(1))
            errmsg = match.group(2)
            found = False
            for mi in line_mapping:
                lr, aid = mi
                lb, le = lr
                if lb <= line < le:
                    uLogging.err("Action %s line %d -%s", aid, line - lb, errmsg)
                    found = True
            if not found:
                uLogging.err("Internal error - cannot find line %d", line)

    if errors:
        raise Exception("There are errors in upgrade procedure")

if __name__ == "__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    try:

        def printUsage():
            print '%s udl2file1 [udl2file2..]' % sys.argv[0]

        try:
            opts, args = getopt.getopt(sys.argv[1:], '', [])
            opts = dict(opts)
        except getopt.GetoptError, err:
            print str(err)
            printUsage()
            sys.exit(2)
        if opts and '--help' in opts:
            printUsage()
            sys.exit(0)

        udl_files = args

        if not Const.isWindows():
            invalid_udls = [x for x in udl_files if os.system(
                "xmllint -o /dev/null --dtdvalid %s %s" % (os.path.join(os.path.dirname(os.path.dirname(sys.argv[0])), 'UDL2.dtd'), x))]
            if invalid_udls:
                raise Exception("Following udl files are invalid: %s" % ", ".join(invalid_udls))

        update_builder = uDLModel.UpdateBuilder()
        for udl in udl_files:
            update_builder.build_from_file_system(udl)
        update = update_builder.product()
        uLogging.info("Running Operation Automation updater code sanity check")
        errors_found = False
        try:
            checkUpgradeActionNaming(update)
        except Exception, e:
            errors_found = True
            uLogging.err(e)
        try:
            tryCheckSQL(update)
        except Exception, e:
            errors_found = True
            uLogging.err(e)
        try:
            tryCheckExec(update)
        except Exception, e:
            errors_found = True
            uLogging.err(e)
        if errors_found:
            raise Exception("Errors are found in Operation Automation updater code")
    except:
        raise
