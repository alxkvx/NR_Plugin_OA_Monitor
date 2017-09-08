from __future__ import generators

__pychecker__ = "unusednames=__rcs_id__,dummy"

from xml.dom import minidom as dom
import os
import sys
import time
import re

from uAction import *
import uActionContext
import uUtil
import uLogging
import uPEM
import uBuild
import uSysDB
from uDLCommon import *

RELEASE = "release"
UPDATE = "update"
PRECHECK = "precheck"


class Action:

    def __init__(self, id):
        self.id = id
        self.owner = "pleskd"

    def kind(self):
        return "atomic action"


class FuncAction(Action):

    def __init__(self, id, func):
        Action.__init__(self, id)
        self.func = func

    def execute(self, readonly, precheck=False):
        return self.func()


class Script(Action):

    def __init__(self, id, owner, script):
        Action.__init__(self, id)
        self.owner = owner
        # pleskd sc is expected to be installed everywhere
        # and scripts with owner == pleskd are expected to be executed everywhere
        if not self.owner:
            self.owner = "pleskd"

        self.script = script
        if not self.script or not os.path.exists(self.script):
            raise Exception("Invalid or non-existent script '%s' specified for action %s" % (self.script, id))

    def get_code(self):
        script_file = open(self.script)
        try:
            code = script_file.read()
            return code
        finally:
            script_file.close()

    def build_node(self, doc, name):
        rv = doc.createElement(name)
        rv.setAttribute("id", self.id)
        rv.setAttribute("owner", self.owner)
        # save relative paths to XML
        script_file_name = os.path.basename(self.script)
        script_parent_dir = os.path.basename(os.path.dirname(self.script))
        # here we intentionally do not use os.path.join to concatenate parts of path
        # so that build result will be the same on both linux and windows
        rv.setAttribute("script", script_parent_dir + "/" + script_file_name)
        return rv


def stmtKind(stmt):
    norm = stmt.upper().strip()
    for kind in ['BEGIN', 'COMMIT', 'ROLLBACK', 'SELECT', 'CREATE', 'ALTER', 'DROP']:
        if norm.startswith(kind):
            return kind
    else:
        return ''


class SQLScript(Script):

    def __init__(self, id, owner, script):
        Script.__init__(self, id, owner, script)

    def node(self, doc):
        return self.build_node(doc, "SQL")

    def kind(self):
        return "SQL"

    def get_code(self):
        return uUtil.stmt_parser(Script.get_code(self), uSysDB.ConcatOperator, uSysDB.nowfun)

    def parsed_code(self):
        return [(stmtKind(stmt), stmt) for stmt in self.get_code()]

    def execute(self, readonly, precheck=False):
        if readonly:
            raise Exception("SQLScript does not support readonly operations")

        if not uPEM.is_sc_installed(self.owner):
            uLogging.info("%s is not installed, skipping", self.owner)
            return None

        con = uSysDB.connect()
        cur = con.cursor()
        rv = None

        for stmt in self.get_code():
            uLogging.debug('executing %s', stmt)

            kind = stmtKind(stmt)
            if kind in ('BEGIN', 'COMMIT', 'ROLLBACK'):
                uLogging.warn(
                    '%s statements are ignored, <SQL> action always implicitly begins and commits transaction', kind)
            else:
                cur.execute(stmt)

        con.commit()
        uSysDB.close(con)

        return rv


class ExecScript(Script):

    def __init__(self, id, owner, script):
        Script.__init__(self, id, owner, script)

    def node(self, doc):
        return self.build_node(doc, "EXEC")

    def makeFunctionCode(self, funname):
        lines = self.get_code().strip().splitlines()
        function_code = "def %s():\n" % funname
        inquote = 0
        for line in lines:
            if not inquote:
                function_code += '\t'
            function_code += line
            function_code += '\n'
            num_quotes = line.count('"""') + inquote
            inquote = divmod(num_quotes, 2)[1]
        return function_code

    def execute(self, readonly, precheck=False):
        if not uPEM.is_sc_installed(self.owner):
            uLogging.info("%s is not installed, skipping", self.owner)
            return None

        uActionContext.prepare(self.id, precheck)
        dictionary = {}

        try:
            try:
                execfile(self.script, dictionary)
            except SystemExit, e:
                if e.code:
                    raise Exception("Execution of update script %s exited with non-zero error code %s" %
                                    (self.script, e.code))

            if readonly:
                # emulate readonly by rolling back all the changes
                uSysDB.rollback_all()
                uActionContext.rollback()
            else:
                uActionContext.commit()
        except:
            uSysDB.rollback_all()
            uActionContext.rollback()
            raise


class Instruction(Script):

    def __init__(self, id, owner, script):
        Script.__init__(self, id, owner, script)

    def kind(self):
        return "instruction"

    def node(self, doc):
        return self.build_node(doc, "INSTRUCTION")

    def execute(self, readonly, precheck=False):
        return None

action_file_name_regex = re.compile(r'([-\w]+)(\.(\w+))?\.(py|txt|sql)')


def parse_action_filename(action_file_name):
    m = action_file_name_regex.match(action_file_name)
    if not m:
        raise Exception('Malformed action script file name: %s' % action_file_name)
    return m.group(1), m.group(3), m.group(4)


class UpdBase:

    def __init__(self):
        self.actions = []
        self.required = None
        self.kind = None

    def add_script_from_node(self, root_dir, node):
        action_id, action_owner = node.getAttribute('id'), node.getAttribute('owner')
        script = os.path.abspath(os.path.join(root_dir, node.getAttribute('script')))
        kind = node.tagName
        if "EXEC" == kind:
            self.actions.append(ExecScript(action_id, action_owner, script))
        elif "SQL" == kind:
            self.actions.append(SQLScript(action_id, action_owner, script))
        elif "INSTRUCTION" == kind:
            self.actions.append(Instruction(action_id, action_owner, script))
        else:
            raise Exception("Invalid action type %s" % kind)

    def add_script_from_file(self, script):
        try:
            action_id, action_owner, action_type = parse_action_filename(os.path.basename(script))
        except Exception, e:
            uLogging.err(' *** Upgrade action script %s will be skipped owing to malformed script name' % script)
            # skip unsupported files in upgrade action directories
            raise

        if not action_id:
            raise Exception('Void action_id for script %s: malformed script name?' % script)

        uLogging.debug('%s %s %s' % (action_id, action_owner, action_type))

        if 'py' == action_type:
            self.actions.append(ExecScript(action_id, action_owner, script))
        elif 'sql' == action_type:
            self.actions.append(SQLScript(action_id, action_owner, script))
        elif 'txt' == action_type:
            self.actions.append(Instruction(action_id, action_owner, script))
        else:
            raise Exception("Unknown type of action script %s" % action_type)

    def save_actions(self, doc, node):
        for act in self.actions:
            node.appendChild(act.node(doc))


def savePackages(node, doc, packages):
    to_add = []
    for package in packages:
        for install_on in packages[package]:
            to_add.append((package[1], PkgInstallation(None, package[0], package[1], install_on).node(doc)))

    to_add.sort()

    for np in to_add:
        node.appendChild(np[1])

doAlwaysPre = [
    """Run precheck (this step does not require system downtime and thus can be performed any time before starting
	real Upgrade):
	./precheck.py --cache path_to_directory_for_cache [--database:host name_of_sysdb_host] [--database:password password for user to connect to system database]
	--database:password is used for Windows only.""",
]
doAlways = [
        """Execute included update script (it is located in the root of distribution) on management node:
	./update.py --cache path_to_directory_for_cache [--database:host name_of_sysdb_host] [--database:password password for user to connect to system database]
	If you wish to install several hotfixes at once, you can execute following commands instead (it will be faster than
	installing hotfixes one by one):
	cd latest_hotfix
	./update.py --cache cache [--database:host hostname] [--database:password] ../hotfix_1_directory ../hotfix_2_directory ... ../latest_hotfix""", ]


class Update(UpdBase):

    def __init__(self):
        UpdBase.__init__(self)

        self.name = ''
        self.version = ''
        self.preparation = UpdBase()
        self.pre = UpdBase()
        self.prepkg = UpdBase()
        self.post = UpdBase()
        self.packages = {}
        self.patches = {}
        self.aps = []
        self.current_update = None
        self.disabled_packages = set()

        self.native_packages = []

        self.update_windows = False

        self.cleanup = UpdBase()

        self.kind = "release"
        self.jboss_distrib = False       # flags should we take particular jboss part to update
        self.jboss_pau = False
        self.jboss_pui = False

    def has_package(self, name, ctype):
        return self.packages.has_key((name, ctype))

    def add_package(self, pkg):
        if self.packages.has_key((pkg.name, pkg.ctype)):
            self.packages[(pkg.name, pkg.ctype)].update([pkg.where_pkg])
        else:
            self.packages[(pkg.name, pkg.ctype)] = set([pkg.where_pkg])

    def add_node_package(self, root_dir, node):
        if node.getAttribute('host') == 'no_auto':
            self.disabled_packages.add((node.getAttribute('name'), node.getAttribute('ctype')))
        else:
            self.add_package(PkgInstallation(node))

    def add_patch(self, patch):
        if (patch.name,patch.ctype) in self.patches:
            self.patches[(patch.name, patch.ctype)].append(patch)
        else:
            self.patches[(patch.name, patch.ctype)]=[patch]

    def add_node_patch(self, root_dir, node):
        self.add_patch(Patch(node))

    def add_node_native_package(self, root_dir, node):
        pkg = parseNativePackageNode(node)
        self.__add_native_package(pkg)

    def add_new_package(self, name, ctype):
        self.add_package(PkgInstallation(None, name, ctype, None))

    def add_upgrade_package(self, name, ctype):
        self.add_package(PkgInstallation(None, name, ctype, (name, ctype)))

    def add_native_package(self, name, nptype, host):
        pkg = NativePackage(name, nptype, host, None)
        self.__add_native_package(pkg)

    def __add_native_package(self, pkg):
        self.native_packages.append(pkg)
        if pkg.name == "PAgent.exe":
            self.update_windows = True

    def save_native_packages(self, doc, node):
        for pkg in self.native_packages:
            elem = doc.createElement("NATIVE_PACKAGE")
            elem.setAttribute("name", pkg.name)
            elem.setAttribute("name_x64", pkg.name_x64)
            elem.setAttribute("host", pkg.host)
            elem.setAttribute("type", pkg.type)
            if pkg.where is not None:
                typ, nam = pkg.where
                elem.setAttribute("where_pkg_ctype", typ)
                elem.setAttribute("where_pkg_name", nam)

            node.appendChild(elem)

    def save_disabled_packages(self, doc, node):
        for name, ctype in self.disabled_packages:
            elem = doc.createElement("PACKAGE")
            elem.setAttribute("name", name)
            elem.setAttribute("host", "no_auto")
            elem.setAttribute("ctype", ctype)

            node.appendChild(elem)

    def save(self, to, prechecks_only=False):
        impl = dom.getDOMImplementation()
        dt = impl.createDocumentType("UDL2", "UDL2", "UDL2")
        doc = impl.createDocument("UDL2", "UDL2", dt)
        root = doc.documentElement

        root.setAttribute("name", self.name)
        root.setAttribute("version", self.version)
        root.setAttribute("required", self.required)
        root.setAttribute("built", self.built)
        root.setAttribute("type", self.kind)

        preparation_node = doc.createElement("PREPARE")
        self.preparation.save_actions(doc, preparation_node)
        root.appendChild(preparation_node)

        if not prechecks_only:
            pre_node = doc.createElement("PRE")
            self.pre.save_actions(doc, pre_node)
            root.appendChild(pre_node)

            self.save_native_packages(doc, root)

            savePackages(root, doc, self.packages)
            self.save_disabled_packages(doc, root)
            self.save_actions(doc, root)

            prepkg_node = doc.createElement("PRE_PKG")
            self.prepkg.save_actions(doc, prepkg_node)
            root.appendChild(prepkg_node)

            post_node = doc.createElement("POST")
            self.post.save_actions(doc, post_node)
            root.appendChild(post_node)

            cleanup_node = doc.createElement("CLEANUP")
            self.cleanup.save_actions(doc, cleanup_node)
            root.appendChild(cleanup_node)

        doc.writexml(writer=file(to, "w+"), indent="\t", addindent="\t", newl="\n")

    def augment(self, aug_update):
        self.actions = self.actions + aug_update.actions		# main

        def joinAttrs(attribute):
            vars(self)[attribute].actions = vars(self)[attribute].actions + vars(aug_update)[attribute].actions
        joinAttrs('preparation')
        joinAttrs('pre')
        joinAttrs('prepkg')
        joinAttrs('post')
        joinAttrs('cleanup')

        self.packages.update(aug_update.packages)
        self.disabled_packages.update(aug_update.disabled_packages)
        for p in aug_update.native_packages:
            self.__add_native_package(p)


class UpdateBuilder:

    def __init__(self):
        self.instance = Update()
        self.current_section = None

    def read_and_validate(self, path_to_udl2):
        root_dir = os.path.dirname(path_to_udl2)
        doc = dom.parse(path_to_udl2)
        node = doc.documentElement

        self.instance.name = node.getAttribute("name")
        self.instance.version = node.getAttribute("version")
        if not self.instance.version:
            raise Exception("UDL2 element must have non-empty version attribute")
        if self.instance.required is None:
            self.instance.required = node.getAttribute("required")
        self.instance.built = node.getAttribute("built")
        if not self.instance.built:
            self.instance.built = time.asctime()
        self.instance.kind = node.getAttribute("type") or self.instance.kind or "release"
        jnode = node.getElementsByTagName("JBOSS")
        if jnode:
            jnode = jnode[0]
            self.instance.jboss_distrib = jnode.getAttribute("reinstall") == "yes"
            self.instance.jboss_pau = jnode.getAttribute("pau") == "yes"
            self.instance.jboss_pui = jnode.getAttribute("pui") == "yes"
            if self.instance.jboss_distrib:
                self.instance.jboss_pau = True
                self.instance.jboss_pau = True

        XML_sections = {
            "PREPARE"	: self.instance.preparation,
            "PRE"			: self.instance.pre,
            "PRE_PKG"	: self.instance.prepkg,
            "POST"		: self.instance.post,
            "CLEANUP"	: self.instance.cleanup,
        }

        XML_mapping = {
            "EXEC"					: UpdBase.add_script_from_node,
            "SQL"						: UpdBase.add_script_from_node,
            "INSTRUCTION"		: UpdBase.add_script_from_node,
            "PACKAGE"				: Update.add_node_package,
            "NATIVE_PACKAGE": Update.add_node_native_package,
            "PATCH": Update.add_node_patch,
        }

        for child in [x for x in node.childNodes if x.nodeType == dom.Node.ELEMENT_NODE]:
            tag_name = child.tagName
            if tag_name in XML_sections:
                self.current_section = XML_sections[tag_name]
                # traverse child actions inside section
                for second_child in [x for x in child.childNodes if x.nodeType == dom.Node.ELEMENT_NODE]:
                    tag_name = second_child.tagName
                    if tag_name in XML_mapping:
                        XML_mapping[tag_name](self.current_section, root_dir, second_child)
                    else:
                        raise Exception("%s: unknown action type" % tag_name)
            elif tag_name in XML_mapping:
                self.current_section = self.instance
                XML_mapping[tag_name](self.current_section, root_dir, child)
            else:
                if tag_name != "JBOSS":
                    raise Exception("%s: unknown action type" % tag_name)

        # 'APS' section is not present in UDL2
        aps_scripts_path = os.path.join(root_dir, "aps")
        if os.path.exists(aps_scripts_path):
            self.instance.aps.append(aps_scripts_path)

        return self.instance.name, self.instance.version, self.instance.built, self.instance.kind

    def build_from_file_system(self, path_to_udl2):
        # first of all read what we've got in UDL2
        self.read_and_validate(path_to_udl2)

        # then collect files on disk
        subdir_sections = {
            "prepare"	: self.instance.preparation,
            "pre"		: self.instance.pre,
            "pre_pkg"	: self.instance.prepkg,
            "post"		: self.instance.post,
            "cleanup"	: self.instance.cleanup,
            "main"		: self.instance,
            "aps"		: self.instance.aps,
        }

        update_dir = os.path.dirname(path_to_udl2)
        for subdir_name in subdir_sections:
            subdir_path = os.path.join(update_dir, subdir_name)
            if not os.path.exists(subdir_path):
                continue

            if 'aps' == subdir_name:
                self.instance.aps.append(subdir_path)
                continue

            subdir_files = os.listdir(subdir_path)
            subdir_files = filter(lambda i: not i.startswith(".") and (
                i.lower().endswith(".py") or i.lower().endswith(".txt") or i.lower().endswith(".sql")), subdir_files)
            subdir_files.sort()

            self.current_section = subdir_sections[subdir_name]
            for script_file in subdir_files:
                self.current_section.add_script_from_file(os.path.join(subdir_path, script_file))

    def product(self):
        return self.instance


def depends(what, on, depend_map):
    counter = 0
    max_counter = len(depend_map) + 1

    cur = what
    while counter < max_counter:
        counter += 1
        if not depend_map.has_key(cur):
            uLogging.info("%s does not depend on %s", what, on)
            return False
        elif depend_map[cur] == on:
            uLogging.info("%s depends on %s", what, on)
            return True
        else:
            cur = depend_map[cur]

    raise Exception, "Cyclic dependency, update '%s' depends on itself" % cur


def depend_cmp(what, on, depend_map):
    if depends(what, on, depend_map):
        return 1
    elif depends(on, what, depend_map):
        return -1
    else:
        return uBuild.compare_versions(what, on)


class PreparationProgress:

    def __init__(self):
        self.native_repos_updated = False
        self.main_phase_of_upgrade_started = False
        self.updated_hosts = set()		# upgraded host ids, linux
        self.failed_hosts = []			# list of (PEMHost, error report), linux
        self.windows_update = {}


class BuildInfo:

    def __init__(self, builds):
        self.depends_to_check = None
        self.present_updates = None
        self.builds = builds  # list of uBuild.Build instances
        self.version_list = []
        self.progress = None
        self.conflicts = []
        self.upgrade_instructions = None    # merged upgrade actions. type uDLModel.Update. Produced by UpdateBuilder.product()
        self.rpms_to_update = {}            # merged rpms to update. type as Build.rpms
        self.jboss_components = uBuild.JBossDistrib()  # merged jboss components

    def loadProgress(self, config):
        try:
            self.progress = uUtil.loadObj(config.cache, "progress.dat", PreparationProgress)
        except Exception, e:
            uLogging.err("exception while loading progress: %s" % e)
            self.progress = PreparationProgress()

    def saveProgress(self, config):
        try:
            uUtil.saveObj(self.progress, config.cache, "progress.dat")
        except Exception, e:
            uLogging.err("exception while saving progress: %s" % e)


def getCumulativeUpdateInstructions(builds):
    uLogging.debug("Reading UDL files")
    name_file = []  # List of [(name, file), (name, file)]
    filelist = {}
    depend_map = {}
    fn_docmap = {}
    present_updates = set()
    required_updates = set()
    for build in builds:
        fn = build.udl2_file
        filelist[build.udl2_file] = build
        doc = dom.parse(fn)
        root = doc.documentElement
        name = root.getAttribute("name")
        depend = root.getAttribute("required")
        name_file.append((name, fn))
        fn_docmap[fn] = doc
        present_updates.update([name])
        if depend:
            depend_map[name] = depend
            required_updates.update([depend])

    name_file.sort(lambda x, y: depend_cmp(x[0], y[0], depend_map))

    if not name_file:
        raise Exception, "No update instructions found"

    ordered_udlfiles = [x[1] for x in name_file]
    ordered_builds = []
    rpms_with_max_version = {}
    jboss_components = uBuild.JBossDistrib()
    for x in ordered_udlfiles:
        build = filelist[x]
        ordered_builds.append(build)
        # merge rpms
        for i in build.rpms.keys():
            if rpms_with_max_version.get(i) is None:
                rpms_with_max_version[i] = build.rpms[i]
            else:
                if build.rpms[i]["info"] > rpms_with_max_version[i]["info"]:
                    rpms_with_max_version[i] = build.rpms[i]
        # merge jboss components. take the latest by build precedence. versioning required
        if build.jboss.pau:
            jboss_components.pau = build.jboss.pau
        if build.jboss.pui:
            jboss_components.pui = build.jboss.pui
        if build.jboss.distribution:
            jboss_components.distribution = build.jboss.distribution

    binfo = BuildInfo(ordered_builds)
    binfo.depends_to_check = required_updates - present_updates
    binfo.present_updates = present_updates
    binfo.rpms_to_update = rpms_with_max_version
    uLogging.debug("selected rpms to update: %s" % binfo.rpms_to_update)
    binfo.jboss_components = jboss_components
    uLogging.debug("selected jboss components to update: %s" % binfo.jboss_components)

    update_builder = UpdateBuilder()
    for doc in ordered_udlfiles:
        binfo.version_list.append(update_builder.read_and_validate(doc))
    binfo.upgrade_instructions = update_builder.product()
    return binfo


__all__ = ["Update", "Instruction", "SQLScript", "ExecScript", "PkgInstallation",
           "getCumulativeUpdateInstructions", "configureUDLReader", 'BuildInfo']
