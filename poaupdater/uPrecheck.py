# -*- coding: utf-8 -*-

import time
import os
import uTextRender
import uLogging
import uSysDB
import uUtil
import codecs
import socket


class PrecheckFailed(Exception):

    """
    Exception to throw from precheck action to indicate, that it failed
    """

    def __init__(self, reason, what_to_do = None):
        message = "Precheck error: %s" % reason
        if not message.endswith('\n'):
            message += '\n'
        if what_to_do:
            message += "Do the following: %s\n" % what_to_do
        Exception.__init__(self, message)
        self.reason = reason
        self.what_to_do = what_to_do


def print_precheck_message_to_report(report_file, action_id, action_owner, message, counter):
    try:
        lines = message.splitlines()
        if len(lines) > 1:
            report_file.write(" 1.%-3s %s [%s]%s\n" % (str(counter) + '.', action_id, action_owner, ':'))
            for line in lines:
                report_file.write("\t" + line.strip() + "\n")
        else:
            report_file.write(" 1.%-3s %s [%s]: %s\n" % (str(counter) + '.', action_id, action_owner, lines[0]))
        report_file.write('\n')
    except:
        uUtil.logLastException()
        report_file.write('\n')


def process_precheck_report(build_info, version, poa_version, config):

    errors = []
    messages = []
    now = time.localtime()
    report_filename = time.strftime("precheck-report-%Y-%m-%d-%H%M%S.txt", now)
    report_filename = os.path.abspath(os.path.join(os.path.dirname(config.log_file), report_filename))
    report_file = codecs.open(report_filename, encoding='utf-8', mode='w+')

    for action_id, action_owner, result in precheck_results:
        if not result:
            message = "OK\n"
            messages.append((action_id, action_owner, result, message))
        elif isinstance(result, Exception):
            message = ""
            if isinstance(result, PrecheckFailed):
                if not result.reason.endswith('\n'):
                    result.reason += '\n'
                message += "%s" % result.reason
                if result.what_to_do:
                    message += "You should: %s\n" % result.what_to_do
                else:
                    message += "\n"
            else:
                message += " UNEXPECTED ERROR. See precheck log for details. Failed to complete precheck action %s [%s]: %s\n" % (
                    action_id, action_owner, result)

            errors.append((action_id, action_owner, result, message))
        else:
            message = "%s\n" % result
            messages.append((action_id, action_owner, result, message))

    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT company_name FROM accounts WHERE account_id = 1")
    company_name, = cur.fetchone()
    # TODO rewrite on PEMVersion.getCurrentVersion
    cur.execute("SELECT build FROM version_history ORDER BY install_date DESC")
    build_id, = cur.fetchone()
    cur.execute("SELECT name FROM hotfixes ORDER BY install_date DESC")
    hotfixes = cur.fetchone()

    report_file.write("Operation Automation Upgrade Precheck Report for '%s'\n\n" % company_name)
    report_file.write("Current domain:            %s\n" % socket.getfqdn())
    report_file.write("Date of report:            %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S", now)))
    if not hotfixes:
        hotfix = ""
    else:
        hotfix = " (%s)" % hotfixes[0]
    report_file.write("Current Operation Automation build:         %s%s\n" % (build_id, hotfix))
    target_build_id = ""
    for ver in build_info.version_list:
        name, build_ver, built, kind = ver
        target_build_id += "%s " % build_ver
    report_file.write("Target Operation Automation build:          %s\n" % target_build_id)

    report_file.write("Precheck version:          %s\n" % version)
    report_file.write("Operation Automation version:               %s\n" % poa_version)

    if errors:
        report_file.write(
            "\n%2s. Following errors need to be fixed before upgrade can be continued (refer also to '3. Additional information')\n\n" % '1')
        counter = 1
        for action_id, action_owner, result, message in errors:
            print_precheck_message_to_report(report_file, action_id, action_owner, message, counter)
            counter += 1
        report_file.write("\n%2s. Following checks have passed without errors\n" % '2')
    else:
        report_file.write(
            "%2s. Success\n%6sNo issues preventing upgrade were found: you may continue with upgrade, though before that, please, check results below.\n\n" % ('1', ' '))

    skipped_table = uTextRender.Table(indent=2)
    skipped_table.setHeader(["Owner", "Skipped actions"])

    results_skipped = {}
    results_fine = ''
    results_fine_order = {}
    succeeded, skipped, other = 0, 0, 0
    other_results = ''
    for action_id, action_owner, result, message in messages:
        try:
            if result and ', skipping' in result:
                skipped += 1
                if not results_skipped.has_key(result):
                    results_skipped[result] = ''
                results_skipped[result] += "%s\n" % action_id
            elif result and ', skipping' not in result:
                other += 1
                other_results += ' %s.%-3s %s [%s]:\n       %s\n' % (
                    '3', str(other) + '.', action_id, action_owner, result.replace('\n', '\n       '))
            else:
                succeeded += 1
                if not results_fine_order.has_key(result):
                    results_fine_order[result] = 0
                if results_fine_order[result] < 1:
                    results_fine += "  %-85s" % ("%s [%s]" % (action_id, action_owner))
                    results_fine_order[result] += 1
                else:
                    results_fine += "  %s\n" % ("%s [%s]" % (action_id, action_owner))
                    results_fine_order[result] = 0
        except Exception, e:
            other_results += ' %s.%-3s *** Processing of action %s FAILED: %s. Check %s\n' % (
                '3', str(other) + '.', action_id, e, config.log_file)
            uUtil.logLastException()
        except:
            other_results += ' %s.%-3s *** Processing of action %s FAILED: %s. Check %s\n' % (
                '3', str(other) + '.', action_id, 'unknown error', config.log_file)
            uUtil.logLastException()

    failed = len(errors)
    succeeded += other
    total = len(messages) + failed

    report_file.write("\n%4s. Following checks have succeeded\n\n" % '2.1')
    report_file.write(results_fine)
    if not results_fine.endswith('\n'):
        report_file.write('\n')
    report_file.write("\n%4s. Following checks have been skipped\n" % '2.2')

    # process skipped actions:
    for key in results_skipped:
        skipped_table.addRow([key.replace(', skipping', ''), results_skipped[key]])

    report_file.write("\n%s\n" % skipped_table)

    report_file.write("\n%2s. Additional information:\n" % '3')
    report_file.write(other_results)

    report_file.close()

    if errors:
        uLogging.info("Some of pre-upgrade checks failed. Pre-upgrade checks report summary:")
        uLogging.info("    Failed: %-3s Succeeded: %-3s Skipped: %-3s Total: %-3s" % (failed, succeeded, skipped, total))
        uLogging.info("    FAILURE. The report was saved to %s" % report_filename)
        uLogging.info("    See detailed precheck log in %s " % config.log_file)
        return False
    else:
        uLogging.info("Success. No checks failed. Pre-upgrade checks report summary:")
        uLogging.info("    Failed: %-3s Succeeded: %-3s Skipped: %-3s Total: %-3s" % (failed, succeeded, skipped, total))
        uLogging.info("    Success. No checks failed. The report was saved to %s" % report_filename)
        uLogging.info("    See detailed precheck log in %s " % config.log_file)
        return True


def warn_precheck_for_deprecated_os():
    import uLinux
    try:
        uLinux.check_platform_supported(uLinux.determinePlatform())
    except Exception, e:
        raise PrecheckFailed(e.args[0], "Use Services Upgrade/Migration Guide to migrate "
                                        "Operations Automation management node (https://kb.odin.com/en/130129), then re-run pre-checks.")


precheck_results = []
