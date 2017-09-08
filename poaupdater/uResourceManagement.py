import uSysDB
import uLogging


def _upDirection(con, source_rc, destination_rc):
    cur = con.cursor()
    cur.execute(
        "SELECT parent_path, class_id FROM resource_classes WHERE name = %s", source_rc)
    row = cur.fetchone()
    if row is None:
        uLogging.warn("source rc %s does not exist", source_rc)
        return None
    src_rc_path = "%s%dx" % (row[0], row[1])
    cur.execute(
        "SELECT parent_path, class_id FROM resource_classes WHERE name = %s", destination_rc)
    row = cur.fetchone()
    if row is None:
        uLogging.warn("destination rc %s does not exist", destination_rc)
        return None
    dst_rc_path = "%s%dx" % (row[0], row[1])

    if len(src_rc_path) < len(dst_rc_path):
        dst_rc_path = dst_rc_path[: len(src_rc_path)]
        if dst_rc_path == src_rc_path:
            return False

    if len(dst_rc_path) < len(src_rc_path):
        src_rc_path = src_rc_path[: len(dst_rc_path)]
        if dst_rc_path == src_rc_path:
            return True

    raise Exception(
        "Source RC '%s' shall be child of destination RC '%s'"
        % (source_rc, destination_rc))


def _RCHasParam(con, rc_name, param_name):
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM dual WHERE EXISTS "
        "(SELECT 1  FROM rc_activation_params WHERE name = %s "
        " AND param_name = %s)", rc_name, param_name)
    return bool(cur.fetchone())


def moveActParams(con, source_rc, destination_rc, params, name_mapping=None):
    cur = con.cursor()
    moveUp = _upDirection(con, source_rc, destination_rc)

    if moveUp is None:
        return
    if moveUp:
        uLogging.debug("move up activation parameter(s)")
        upRT = destination_rc
        downRT = source_rc
        tbl_prefix = "c"
    else:
        uLogging.debug("move down activation parameter(s)")
        downRT = destination_rc
        upRT = source_rc
        tbl_prefix = "p"

    concat_delim = uSysDB.ConcatOperator
    params_to_move = []
    for param in params:
        new_param = param
        if name_mapping and name_mapping.has_key(param):
            new_param = name_mapping[param]
        uLogging.debug(
            "move parameter '%s' to parameter '%s', "
            "source RC '%s', destination RC '%s'",
            param, new_param, source_rc, destination_rc)

        has_dest_param = _RCHasParam(con, destination_rc, new_param)
        has_src_param = _RCHasParam(con, source_rc, param)

        if has_dest_param and not has_src_param:
            # nothing to do parameter already moved
            uLogging.debug("parameter already moved, nothing to do")
            continue

        if has_dest_param and has_src_param:
            raise Exception(
                "Can not move parameter from RC '%s'. "
                "Destination RC '%s' already has parameter '%s', "
                "override not supported"
                % (source_rc, destination_rc, new_param))

        if not has_dest_param and not has_src_param:
            raise Exception(
                "Can not move parameter from RC '%s'. "
                "It does not have such parameter '%s'"
                % (source_rc, param))
        params_to_move.append(param)

    # prechecks done, move data in DB
    for param in params_to_move:
        new_param = param
        if name_mapping and name_mapping.has_key(param):
            new_param = name_mapping[param]

        uLogging.debug(
            "move parameter '%s' to parameter '%s', "
            "parent RC '%s', child RC '%s'",
            param, new_param, upRT, downRT)

        # move ST parameters
        cur.execute(
            (
                "SELECT prt.rt_id, crt.rt_id, rtap.st_id "
                "FROM "
                "resource_types prt, resource_classes prc, "
                "resource_types crt, resource_classes crc, "
                "rc_activation_params cap, st_act_params rtap, "
                "st_resources st "
                "WHERE prc.name = %%s "
                "AND prc.class_id = prt.class_id "
                "AND crc.name = %%s "
                "AND crc.class_id = crt.class_id "
                "AND crt.parent_path >= "
                "(prt.parent_path %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'x') "
                "AND crt.parent_path < "
                "(prt.parent_path %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'y') "
                "AND cap.name = %(prefix)src.name "
                "AND cap.param_name = %%s "
                "AND rtap.rt_id = %(prefix)srt.rt_id "
                "AND rtap.name = cap.param_name "
                "AND st.rt_id = prt.rt_id "
                "AND st.st_id = rtap.st_id "
                % {'delim': concat_delim, 'prefix': tbl_prefix}),
            upRT, downRT, param)
        for row in cur.fetchall():
            uLogging.debug(
                "move ST activation parameter: st_id: %s, "
                "parent_rt_id:%s, child_rt_id:%s",
                row[2], row[0], row[1])
            if moveUp:
                stmtparams = (row[0], new_param, row[1], param, row[2])
            else:
                stmtparams = (row[1], new_param, row[0], param, row[2])
            cur.execute(
                "UPDATE st_act_params SET rt_id = %s, name = %s "
                "WHERE rt_id = %s AND name = %s AND st_id = %s",
                stmtparams)

        # move RT parameters, update rt_id and name of parameter
        cur.execute(
            (
                "SELECT prt.rt_id, crt.rt_id "
                "FROM "
                "resource_types prt, resource_classes prc, "
                "resource_types crt, resource_classes crc, "
                "rc_activation_params cap, rt_act_params rtap "
                "WHERE prc.name = %%s "
                "AND prc.class_id = prt.class_id "
                "AND crc.name = %%s "
                "AND crc.class_id = crt.class_id "
                "AND crt.parent_path >= "
                "(prt.parent_path %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'x') "
                "AND crt.parent_path < "
                "(prt.parent_path %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'y') "
                "AND cap.name = %(prefix)src.name "
                "AND cap.param_name = %%s "
                "AND rtap.rt_id = %(prefix)srt.rt_id "
                "AND rtap.name = cap.param_name"
                % {'delim': concat_delim, 'prefix': tbl_prefix}),
            upRT, downRT, param)
        for row in cur.fetchall():
            uLogging.debug(
                "move RT activation parameter: "
                "parent_rt_id:%s, child_rt_id:%s",
                row[0], row[1])
            if moveUp:
                stmtparams = (row[0], new_param, row[1], param)
            else:
                stmtparams = (row[1], new_param, row[0], param)
            cur.execute(
                "UPDATE rt_act_params SET rt_id = %s, name = %s "
                "WHERE rt_id = %s AND name = %s", stmtparams)

        # move RC parameters
        cur.execute(
            "UPDATE rc_activation_params "
            "SET name = %s, param_name = %s "
            "WHERE name = %s AND param_name = %s",
            destination_rc, new_param, source_rc, param)


def removeActParams(con, resclass, param_list):
    cur = con.cursor()
    param_list_str = ", ".join(["'" + x + "'" for x in param_list])
    uLogging.debug("Removing activation parameters of resource %s: %s", resclass, param_list_str)
    for table_name in ['st_act_params', 'rt_act_params']:
        cur.execute(
            "DELETE FROM " + table_name + " WHERE rt_id IN ("
            "  SELECT rt.rt_id FROM resource_types rt JOIN"
            "    resource_classes rc ON rt.class_id=rc.class_id"
            "      WHERE rc.name='%(resclass)s')"
            "  AND name IN (%(param_list)s)"
            % {'resclass': resclass, 'param_list': param_list_str})
    cur.execute("DELETE FROM rc_activation_params WHERE name='%(resclass)s'"
                " AND param_name IN (%(param_list)s)"
                % {'resclass': resclass, 'param_list': param_list_str})


def renameActParams(con, resclass, param_map):
    err_msg = "Cannot rename activation parameter %s of resource %s: %s."
    cur = con.cursor()
    for old_name, new_name in param_map.iteritems():
        if not _RCHasParam(con, resclass, old_name):
            raise Exception(err_msg % (old_name, resclass, "parameter does not exist"))
        if _RCHasParam(con, resclass, new_name):
            raise Exception(err_msg % (old_name, resclass,
                                       "parameter with target name %s already exist" % new_name))
        uLogging.debug("Renaming activation parameter %s to %s (resource %s)", old_name, new_name, resclass)
        cur.execute("UPDATE rc_activation_params SET param_name=%s"
                    " WHERE name=%s AND param_name=%s", new_name, resclass, old_name)
        for table_name in ['rt_act_params', 'st_act_params']:
            cur.execute(
                "UPDATE " + table_name + " SET name=%(new_name)s WHERE rt_id IN ("
                "  SELECT rt.rt_id FROM resource_types rt JOIN"
                "    resource_classes rc ON rt.class_id=rc.class_id"
                "      WHERE rc.name=%(resclass)s)"
                "  AND name=%(old_name)s",
                {'resclass': resclass, 'old_name': old_name, 'new_name': new_name})


def updateActParam(con, resclass, param_name, system=None, friendly_name=None, description=None):
    err_msg = "Cannot update activation parameter %s of resource %s: %s."
    cur = con.cursor()
    if not _RCHasParam(con, resclass, param_name):
        raise Exception(err_msg % (param_name, resclass, "parameter does not exist"))

    stmt_params = []
    set_str = ""

    if system is not None:
        set_str = "system = %s"
        if system:
            stmt_params.append('Y')
        else:
            stmt_params.append('N')
    if friendly_name is not None:
        if set_str:
            set_str += ', '
        set_str += 'param_friendly_name = %s'
        stmt_params.append(friendly_name)

    if description is not None:
        if set_str:
            set_str += ', '
        set_str += 'param_desc = %s'
        stmt_params.append(description)

    if not stmt_params:
        raise Exception("Resclass %s param %s: no changes requested" % (resclass, param_name))

    stmt_params += [resclass, param_name]

    cur.execute("UPDATE rc_activation_params SET " + set_str +
                " WHERE name=%s AND param_name=%s", stmt_params)


def rebuildRCParentPaths(con):
    cur = con.cursor()
    cur.execute("UPDATE resource_classes SET parent_path = NULL")
    cur.execute(
        "UPDATE resource_classes SET parent_path = 'x' "
        "WHERE parent_class_id IS NULL")

    concat_delim = uSysDB.ConcatOperator

    for dummy in xrange(10):
        cur.execute(
            ("UPDATE resource_classes "
             "SET parent_path = "
             "(SELECT prc.parent_path FROM resource_classes prc "
             "WHERE prc.class_id = resource_classes.parent_class_id) "
             " %(delim)s CAST(parent_class_id AS VARCHAR) %(delim)s 'x' "
             "WHERE parent_path IS NULL AND parent_class_id IN "
             "(SELECT class_id FROM resource_classes "
             "WHERE parent_path IS NOT NULL)" % {'delim': concat_delim}))


def _recalculateNullRTPath(con):
    concat_delim = uSysDB.ConcatOperator
    cur = con.cursor()

    # recalculate parent_paths
    for dummy in xrange(10):
        cur.execute(
            "UPDATE resource_types SET parent_path= "
            "(SELECT rc.parent_path %(delim)s "
            "CAST(rc.rt_id AS VARCHAR)%(delim)s'x'  "
            "FROM resource_types rc  "
            "WHERE rc.rt_id = resource_types.parent_id ) "
            "WHERE parent_path IS NULL" % {'delim': concat_delim})


def _isSubRC(con, candidate, parent):
    concat_delim = uSysDB.ConcatOperator
    cur = con.cursor()

    cur.execute(
        ("SELECT rc.class_id "
         "FROM resource_classes rc, resource_classes crc "
         "WHERE "
         "rc.name = %%s "
         "AND crc.name = %%s "
         "AND crc.parent_path >= "
         "  (rc.parent_path  %(delim)s "
         "   CAST (rc.class_id AS varchar) %(delim)s 'x') "
         "AND crc.parent_path <= "
         "  (rc.parent_path %(delim)s "
         "   CAST (rc.class_id AS varchar) %(delim)s 'y') "
         % {'delim': concat_delim}), parent, candidate)
    if cur.fetchone():
        return True

    return False


def _resetRTPathUnderRC(con, rcname):
    concat_delim = uSysDB.ConcatOperator
    cur = con.cursor()
    cur.execute(
        (
            "UPDATE resource_types SET parent_path = NULL "
            "WHERE rt_id IN "
            "(SELECT rt.rt_id "
            " FROM resource_types rt, "
            " resource_types srt, resource_classes rc "
            " WHERE rt.rt_id = resource_types.rt_id "
            " AND srt.class_id = rc.class_id AND rc.name = %%s"
            " AND rt.parent_path >= "
            "   (srt.parent_path %(delim)s "
            "    CAST(srt.rt_id AS varchar)%(delim)s'x') "
            " AND rt.parent_path <= "
            "     (srt.parent_path %(delim)s "
            "    CAST(srt.rt_id AS varchar)%(delim)s'y')) "
            % {'delim': concat_delim}), rcname)
    cur.execute(
        (
            "UPDATE resource_types SET parent_path = NULL "
            "WHERE rt_id IN "
            "(SELECT rt.rt_id "
            " FROM resource_types rt, "
            " resource_types srt, resource_classes rc "
            " WHERE rt.rt_id = resource_types.rt_id "
            " AND srt.class_id = rc.class_id AND rc.name = %%s"
            " AND rt.parent_path >= "
            "   (srt.parent_path %(delim)s "
            "    CAST(srt.rt_id AS varchar)%(delim)s'x') "
            " AND rt.parent_path <= "
            "     (srt.parent_path %(delim)s "
            "    CAST(srt.rt_id AS varchar)%(delim)s'y')) "
            % {'delim': concat_delim}), rcname)


def moveSubRC(con, rctomove, newparent=None):
    # A. In case when new parent defined:
    # Applicable only for RC's in the same RC tree branch
    # which share any level parent RC scope, It means
    # that each instance of RT which moved have only ONE analog RT
    # which shallo stay parent of moved RT's.
    # In terms of Resouce Management it means that:
    # 1. both affected RC's (which moved and new parent) not customizable
    # 2. both affected RC's have common cusomizable ancestor
    #    or both does not have customizable ancestor
    #
    # B. If new parent not set (RC became root RC) applicable to ANY RC's
    # With one IMPORTANT NOTE - moved RC will implicitly inherit
    # customizable flag from ancestors - if it have customizable
    # ancestor it will become customizable too

    concat_delim = uSysDB.ConcatOperator
    cur = con.cursor()

    # prechecks
    if rctomove is None:
        return None
    if rctomove == newparent:
        return None

    root_rc = ""
    if rctomove is not None and newparent is not None:
        cur.execute(
            "SELECT 1 "
            "FROM resource_classes rc, resource_classes prc "
            "WHERE rc.parent_class_id = prc.class_id "
            "AND rc.name = %s AND prc.name = %s", rctomove, newparent)
        if cur.fetchone():
            # we already parent for RC
            return None

        cur.execute(
            "SELECT partible "
            "FROM resource_classes "
            "WHERE name = %s", rctomove)
        row = cur.fetchone()
        if row is None:
            raise Exception("RC specified not registered")
        src_partible = row[0]
        if src_partible == "y":
            raise Exception("Customizable RC can not be moved")

        cur.execute(
            "SELECT partible, class_id "
            "FROM resource_classes "
            "WHERE name = %s", newparent)
        row = cur.fetchone()
        if row is None:
            raise Exception("RC specified not registered")
        dst_partible = row[0]

        # check that we do not move under own
        # subresource to avoid recursion
        if _isSubRC(con, newparent, rctomove):
            raise Exception("Can not move to own sub resource")

        if dst_partible == "y":
            root_rc = row[1]
            # check that rctomove is subresource of root RC
            if not _isSubRC(con, rctomove, newparent):
                raise Exception(
                    "Can not move RC to customizable parent "
                    "from other branches")
        else:
            # check that they have common parent (determine it)
            # find nearest common root ancestor
            cur.execute(
                ("SELECT rc.class_id "
                 "FROM resource_classes rc, resource_classes crc, "
                 "resource_classes crc2 "
                 "WHERE "
                 "crc.name =%%s "
                 "AND crc.parent_path >= "
                 "  (rc.parent_path  %(delim)s "
                 "   CAST (rc.class_id AS varchar) %(delim)s 'x') "
                 "AND crc.parent_path <= "
                 "  (rc.parent_path %(delim)s "
                 "   CAST (rc.class_id AS varchar) %(delim)s 'y') "
                 "AND crc2.name =%%s "
                 "AND crc2.parent_path >= "
                 "  (rc.parent_path  %(delim)s "
                 "   CAST (rc.class_id AS varchar) %(delim)s 'x') "
                 "AND crc2.parent_path <= "
                 "  (rc.parent_path %(delim)s "
                 "   CAST (rc.class_id AS varchar) %(delim)s 'y') "
                 "AND crc.class_id != crc2.class_id "
                 "ORDER BY rc.parent_path DESC "
                 % {'delim': concat_delim}), rctomove, newparent)
            row = cur.fetchone()
            if row is None:
                raise Exception(
                    "affected RC's shall have common ancestor RC")
            root_rc = row[0]

    if newparent is None:
        # Update:
        # 0. Inherit customizable flag
        cur.execute(
            ("SELECT rc.name "
             "FROM resource_classes rc, resource_classes crc "
             "WHERE rc.partible = 'y' "
             "AND crc.name =%%s "
             "AND crc.parent_path >= "
             "  (rc.parent_path  %(delim)s "
             "   CAST (rc.class_id AS varchar) %(delim)s 'x') "
             "AND crc.parent_path <= "
             "  (rc.parent_path %(delim)s "
             "   CAST (rc.class_id AS varchar) %(delim)s 'y')"
             % {'delim': concat_delim}), rctomove)
        partible = 'n'
        if cur.fetchone():
            partible = 'y'
        cur.execute(
            "UPDATE resource_classes SET partible = %s "
            "WHERE name = %s", partible, rctomove)

        # 1. RC hierarchy
        #    1.1 {resource_classes:(parent_class_id, parent_path)}
        cur.execute(
            "UPDATE resource_classes SET parent_class_id = NULL "
            "WHERE name = %s", rctomove)
        rebuildRCParentPaths(con)
        # 2. RT hierarchy
        #    2.1 {resource_types:(parent_id, parent_path)}
        # reset RT paths for resource under moved
        _resetRTPathUnderRC(con, rctomove)
        # reset RT path for moved RT and set correct parent
        cur.execute(
            "UPDATE resource_types "
            "SET parent_id = -1, parent_path = 'x' "
            "WHERE class_id IN "
            "(SELECT class_id FROM resource_classes WHERE name = %s)",
            rctomove)
        _recalculateNullRTPath(con)

    else:

        # Update:
        # 1. RC hierarchy
        #    1.1 {resource_classes:(parent_class_id, parent_path)}
        cur.execute(
            "UPDATE resource_classes SET parent_class_id = "
            "(SELECT class_id FROM resource_classes WHERE name = %s) "
            "WHERE name = %s", newparent, rctomove)
        rebuildRCParentPaths(con)

        # 2. RT hierarchy
        #    2.1 {resource_types:(parent_id, parent_path)}
        # reset RT paths for resource under moved
        _resetRTPathUnderRC(con, rctomove)
        # reset path and set correct parent RT for moved
        cur.execute(
            (
                "UPDATE resource_types "
                "SET parent_path = NULL, parent_id = "
                "(SELECT newp.rt_id FROM "
                "resource_types newp, resource_classes rc1, "
                "resource_types newch, resource_classes rc2, "
                "resource_types prt "
                "WHERE prt.class_id = %%s "
                "AND rc1.name = %%s "
                "AND newp.class_id = rc1.class_id "
                "AND ((newp.parent_path >= (prt.parent_path "
                "  %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'x') "
                "AND newp.parent_path <= (prt.parent_path "
                "  %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'y')) "
                "     OR (newp.rt_id = prt.rt_id)) "
                "AND rc2.name = %%s "
                "AND newch.class_id = rc2.class_id "
                "AND newch.parent_path >= (prt.parent_path "
                "  %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'x') "
                "AND newch.parent_path <= (prt.parent_path "
                "  %(delim)s CAST(prt.rt_id AS varchar) %(delim)s 'y') "
                "AND newch.rt_id = resource_types.rt_id) "
                "WHERE class_id = "
                "(SELECT class_id FROM resource_classes "
                "WHERE name = %%s)" % {'delim': concat_delim}),
            root_rc, newparent, rctomove, rctomove)

        _recalculateNullRTPath(con)


def propagateRegisteredSubRC(con, rcname):
    # WARNING. It is time cost operation, time execution depend from
    # number of affected subscriptions.
    #
    # Propagate sub RC registered via
    # registerResourceClass OpenAPI operation
    # In that case:
    # 1. RT's structure already correct
    # 2. provider subscrption's already correct
    # 3. provider ST's (even fake) already correct
    #
    # We shall do following recursively:
    # 1. fix subscriptions which based on ST which have sub resource but
    #    subscription does not contain it
    # 2. fix ST's which contain parent RC but does contain sub resource
    #    and subscription from where parent RC got contain sub resource
    # Repeat this steps in loop until whole synchronization
    # the number of steps is equal to acount tree depth in worst case.
    concat_delim = uSysDB.ConcatOperator
    cur = con.cursor()
    for dummy in xrange(10):
        # fix subscriptions
        cur.execute(
            "INSERT INTO subs_resources(sub_id, sub_limit, rt_id, path)"
            " SELECT s.sub_id, 0, rt.rt_id, 'XXX' "
            "FROM st_resources str, resource_types rt, "
            "resource_classes rc, subscriptions s "
            "WHERE str.rt_id = rt.rt_id "
            "AND rt.class_id = rc.class_id "
            "AND rc.name = %s "
            "AND s.st_id = str.st_id "
            "AND EXISTS ( "
            " SELECT 1 FROM subs_resources psr "
            " WHERE psr.rt_id = rt.parent_id AND psr.sub_id = s.sub_id)"
            " AND NOT EXISTS ("
            "  SELECT 1 FROM subs_resources sr "
            "  WHERE sr.sub_id = s.sub_id AND sr.rt_id = str.rt_id)",
            rcname)
        # fix path in subsriptions
        cur.execute((
            "UPDATE subs_resources SET path = "
            " (SELECT parent_path %(delim)s 'x' %(delim)s "
            "  CAST(rt_instance_id AS varchar) "
            "  FROM v_sr_parents "
            "  WHERE rti_id = subs_resources.rt_instance_id) "
            "WHERE path = 'XXX'" % {'delim': concat_delim}))

        # fix ST's
        cur.execute(
            "INSERT INTO st_resources "
            "(st_id, rt_id, src_rt_instance_id, sub_limit) "
            "SELECT str.st_id, crt.rt_id, csr.rt_instance_id, 0 "
            "FROM "
            "st_resources str, resource_types rt, "
            "resource_types crt, resource_classes crc, "
            "subs_resources sr, subs_resources csr "
            "WHERE "
            "crc.name = %s "
            "AND rt.class_id = crc.parent_class_id "
            "AND rt.rt_id = str.rt_id "
            "AND crt.class_id = crc.class_id "
            "AND crt.parent_id = rt.rt_id "
            "AND sr.rt_instance_id = str.src_rt_instance_id "
            "AND csr.sub_id = sr.sub_id "
            "AND csr.rt_id = crt.rt_id "
            "AND NOT EXISTS "
            "( "
            " SELECT 1 FROM st_resources str2 "
            " WHERE str2.rt_id = crt.rt_id "
            " AND str2.st_id = str.st_id "
            ") ", rcname)


def updateResUsage(con, rti_id, usage_alter):
    cur = con.cursor()
    cur.execute(
        "SELECT path FROM subs_resources WHERE rt_instance_id = %s", rti_id)
    row = cur.fetchone()
    if row is None:
        raise Exception(
            ("can not update usage, resource not found id: %s" % rti_id))
    cur.execute(
        (
            "UPDATE subs_resources "
            "SET curr_usage = curr_usage + %s "
            "WHERE rt_instance_id IN (0 %s)"
            % (usage_alter, ",".join(row[0].split('x')))))
    cur.execute(
        "UPDATE subs_resources "
        "SET own_usage = own_usage + %s "
        "WHERE rt_instance_id  = %s", usage_alter, rti_id)


def addActParam(con, resclass, param_name, friendly_name, description, system=False):
    cur = con.cursor()
    is_system = 'N'
    if system:
        is_system = 'Y'
    cur.execute(
        "INSERT INTO rc_activation_params"
        " (name, param_name, param_desc, param_friendly_name, system)"
        " VALUES (%s, %s, %s, %s, %s)", (resclass, param_name, description, friendly_name, is_system))


def setActParamValue(con, rt_id, name=None, value=None, st_id=None):
    if rt_id is None or name is None or value is None:
        raise Exception("setActParamValue: rt_id, param_name, value parameters must be set")
    cur = con.cursor()
    if st_id is None:
        # setting act param on resource type
        cur.execute("SELECT 1 FROM rt_act_params WHERE rt_id=%s AND name=%s", (rt_id, name))
        if cur.fetchone():
            cur.execute("UPDATE rt_act_params SET value=%s WHERE rt_id=%s AND name=%s", (value, rt_id, name))
        else:
            cur.execute("INSERT INTO rt_act_params (rt_id, name, value) VALUES (%s, %s, %s)", (rt_id, name, value))
    else:
        # setting act param on service template
        cur.execute("SELECT value FROM st_act_params WHERE st_id=%s AND rt_id=%s AND name=%s", (st_id, rt_id, name))
        if cur.fetchone():
            cur.execute("UPDATE st_act_params SET value=%s WHERE st_id=%s AND rt_id=%s AND name=%s",
                        (value, st_id, rt_id, name))
        else:
            cur.execute("INSERT INTO st_act_params (st_id, rt_id, name, value) VALUES (%s, %s, %s, %s)",
                        (st_id, rt_id, name, value))


def _fakeSubscribeOnRT(con, rt_id):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO subs_resources (sub_limit, path, sub_id, rt_id) SELECT -1, '', 1, %s FROM dual WHERE NOT EXISTS (SELECT 1 FROM subs_resources WHERE sub_id = 1 AND rt_id = %s)", (rt_id, rt_id))
    cur.execute("INSERT INTO subs_resources (sub_id, rt_id, sub_limit, path) SELECT 1, rt.rt_id, -1, '' FROM resource_types rtp, resource_types rt WHERE rtp.rt_id = %s AND rt.parent_path >= (rtp.parent_path" + uSysDB.ConcatOperator +
                "CAST(rtp.rt_id AS varchar)" + uSysDB.ConcatOperator + "'x') AND rt.parent_path < (rtp.parent_path" + uSysDB.ConcatOperator + "CAST(rtp.rt_id AS varchar)" + uSysDB.ConcatOperator + "'y') AND NOT EXISTS (SELECT 1 FROM subs_resources s WHERE s.sub_id = 1 AND s.rt_id = rt.rt_id)", rt_id)
    cur.execute("UPDATE subs_resources SET path = COALESCE((SELECT psr.path FROM subs_resources psr, resource_types prt, resource_types rt  WHERE psr.sub_id = 1  AND prt.system = 'y'  AND prt.rt_id = psr.rt_id  AND prt.class_id = rt.class_id  AND rt.rt_id = subs_resources.rt_id  AND rt.system = 'n' ) , '') " +
                uSysDB.ConcatOperator + " 'x' " + uSysDB.ConcatOperator + " CAST(rt_instance_id AS varchar) WHERE sub_id = 1 AND path = ''")


def _createResourceTypeChildren(con, rt_id):
    cur = con.cursor()
    cur.execute("INSERT INTO resource_types (owner_id, parent_id, class_id, restype_name, description, parent_path) SELECT rt.owner_id, rt.rt_id, rc.class_id, rc.friendly_name, rc.description, rt.parent_path" +
                uSysDB.ConcatOperator + "CAST(rt.rt_id AS varchar)" + uSysDB.ConcatOperator + "'x' FROM resource_types rt JOIN resource_classes rc ON (rc.parent_class_id = rt.class_id) WHERE rt.rt_id = %s", rt_id)
    new_rt_id = uSysDB.get_last_inserted_value(con, "resource_types")
    if new_rt_id != rt_id:  # something inserted
        cur.execute("SELECT rt_id FROM resource_types WHERE parent_id = %s", rt_id)
        for row in cur.fetchall():
            _createResourceTypeChildren(con, row[0])


def createResourceType(con, parent_class, name, description):
    cur = con.cursor()
    cur.execute("SELECT class_id FROM resource_classes WHERE name= %s", parent_class)
    row = cur.fetchone()
    if not row:
        raise Exception("%s: no such resource class", parent_class)

    rc_id = row[0]

    cur.execute("INSERT INTO resource_types (owner_id, restype_name, description, class_id, parent_path, parent_id) SELECT 1, %s, %s, %s, 'x', -1  FROM dual WHERE NOT EXISTS   (SELECT 1 FROM    resource_types rt, resource_classes rc    WHERE rt.restype_name = %s  AND rt.parent_id = -1 AND rt.class_id = rc.class_id AND (rc.partible = 'n' OR (rc.partible = 'y' AND rt.system='n')))",
                name, description, rc_id, name)
    rt_id = uSysDB.get_last_inserted_value(con, "resource_types")

    _createResourceTypeChildren(con, rt_id)
    _fakeSubscribeOnRT(con, rt_id)

    return rt_id


def deleteResourceClass(con, class_name):
    cur = con.cursor()
    cur.execute("DELETE FROM rc_dependency WHERE %s IN (rc1_name, rc2_name)", class_name)
    cur.execute("DELETE FROM resource_classes WHERE name = %s", class_name)


def resetUsage(resclass_name, con):
    cur = con.cursor()
    cur.execute(
        "UPDATE subs_resources SET curr_usage = 0, own_usage = 0 WHERE rt_id IN (SELECT rt_id FROM resource_types rt JOIN resource_classes rc ON (rc.class_id = rt.class_id) WHERE name = %s)", resclass_name)


class ResType:

    def __init__(self, rt_id, restype_name, friendly_name):
        self.rt_id = rt_id
        self.restype_name = restype_name
        self.friendly_name = friendly_name

    def __str__(self):
        return "%d-%s-%s" % (self.rt_id, self.restype_name, self.friendly_name)

    def __eq__(self, other):
        return self.rt_id == other.rt_id and \
            self.restype_name == other.restype_name and \
            self.friendly_name == other.friendly_name

    def __hash__(self):
        return hash(str(self))


def resetUsageAPI(resclass_name, con):
    from openapi import OpenAPI
    api = OpenAPI()

    cur = con.cursor()
    cur.execute("""
		SELECT sr.sub_id, sr.rt_id, rt.restype_name, rc.friendly_name 
		FROM resource_types rt 
			JOIN resource_classes rc ON rt.class_id = rc.class_id
			JOIN subs_resources sr ON rt.rt_id = sr.rt_id
		WHERE rc.name = '%s';
	""" % resclass_name)

    res_types = set()
    for sub_id, rt_id, restype_name, friendly_name in cur.fetchall():
        uLogging.debug("reset resource usage for subscription id %s resource type id %s", sub_id, rt_id)
        api.pem.resetResourceUsage(subscription_id=sub_id, resource_type_ids=(rt_id,))
        res_types.add(ResType(rt_id, restype_name, friendly_name))

    resetUsage(resclass_name, con)
    return res_types


def fixCurrUsage(resclass_name, con):
    cur = con.cursor()
    cur.execute(("UPDATE subs_resources SET curr_usage = (SELECT SUM(own_usage) FROM subs_resources child_res WHERE child_res.path BETWEEN subs_resources.path %(concat)s 'x' AND subs_resources.path %(concat)s 'y' OR child_res.path = subs_resources.path) WHERE rt_id IN (SELECT rt_id FROM resource_types rt JOIN resource_classes rc ON (rc.class_id = rt.class_id) WHERE name = %%s)" % {
                'concat': uSysDB.ConcatOperator}), resclass_name)

__all__ = ['moveActParams', 'removeActParams', 'renameActParams', 'updateActParam', 'rebuildRCParentPaths',
           'addActParam', 'setActParamValue', 'moveSubRC', 'propagateRegisteredSubRC', 'createResourceType']


def setLicenseItemUsage(license_item_name, license_item_desc, usage, con):
    cur = con.cursor()
    cur.execute("DELETE FROM openfusion_license_usage WHERE name = %s", license_item_name)
    cur.execute("INSERT INTO openfusion_license_usage (name, description, usage) VALUES (%s, %s, %s)",
                license_item_name, license_item_desc, usage)


def mapResourceClassToLicenseItem(resclass_name, license_item_name, license_item_desc, con):
    cur = con.cursor()

    cur.execute("UPDATE resource_classes SET license_item = %s WHERE name = %s",
                license_item_name, resclass_name)

    cur.execute(
        "SELECT curr_usage FROM subs_resources WHERE sub_id = 1 AND rt_id IN (SELECT rt_id FROM resource_types rt JOIN resource_classes rc ON (rt.class_id = rc.class_id) WHERE rc.name = %s)",
        resclass_name)

    usage = sum([x[0] for x in cur.fetchall()])
    if usage > 0:
        setLicenseItemUsage(license_item_name, license_item_desc, usage, con)


def _getServiceTemplatesUpgradePath(con, resource_classes):
    """return list of service template ids as upgrade path"""
    cur = con.cursor()
    placeholders = ",".join(["%s"] * len(resource_classes))

    query = """
SELECT DISTINCT a.path, str.st_id
FROM subs_resources sr
JOIN subscriptions s ON sr.sub_id = s.sub_id
JOIN st_resources str ON sr.rt_id = str.rt_id AND sr.rt_instance_id = str.src_rt_instance_id
JOIN service_templates st ON str.st_id = st.st_id
JOIN accounts a ON st.owner_id = a.account_id
JOIN resource_types rt ON sr.rt_id = rt.rt_id
JOIN resource_classes rc ON rt.class_id = rc.class_id
WHERE rc.name IN (%s)
ORDER BY a.path ASC, str.st_id ASC;
""" % (placeholders,)
    cur.execute(query, *resource_classes)
    data = cur.fetchall()

    if not data:
        return []

    return [st_id for account_path, st_id in data]


def _getSubscriptionsUpgradePath(con, st_id, resource_classes):
    """return list subscriptions ids as upgrade path"""
    cur = con.cursor()
    placeholders = ",".join(["%s"] * len(resource_classes))
    query = """
SELECT DISTINCT sbr.sub_id 
FROM subs_resources sbr 
JOIN st_resources sr ON sr.src_rt_instance_id = sbr.rt_instance_id 
JOIN resource_types rt ON rt.rt_id = sr.rt_id 
JOIN resource_classes rc ON rc.class_id = rt.class_id 
WHERE st_id = %s
	AND rc.name IN (%s)
""" % ("%s", placeholders)
    cur.execute(query, st_id, *resource_classes)
    data = cur.fetchall()
    if not data:
        return []

    return [sub_id for sub_id, in data]


def fixServiceTemplates(con, resource_classes, upgradeSubscriptionCallable, fixServiceTemplateCallable):
    """fix service templates and upgrade subscriptions(to fix reseller's service templates)"""
    cur = con.cursor()

    st_upgrade_path = _getServiceTemplatesUpgradePath(con, resource_classes)

    for st_id in st_upgrade_path:
        subscription_upgrade_path = _getSubscriptionsUpgradePath(con, st_id, resource_classes)
        for sub_id in subscription_upgrade_path:
            if sub_id != 1:
                upgradeSubscriptionCallable(con, sub_id)
        fixServiceTemplateCallable(con, st_id)
