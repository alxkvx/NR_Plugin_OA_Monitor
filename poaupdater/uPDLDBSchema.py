from xml.dom import minidom as dom

import uDBSchema
import uDBTypes
import uLogging
import uSysDB
import uDLModel
import uUtil


__rcs_id__ = """$Id$"""

__pychecker__ = "unusednames=__rcs_id__,dummy"

from uManifestParser import nodeTextContent, unsign_package

unsized_type_mapping = {
    "INT"		: uDBTypes.IntType,
    "BIGINT"	: uDBTypes.BigIntType,
    "BLOB"		: uDBTypes.BlobType,
    "DOUBLE"	: uDBTypes.DoubleType,
    "TIMESTAMP"	: uDBTypes.TimestampType
}

sized_type_mapping = {
    "CHAR"		: uDBTypes.CharType,
    "VARCHAR"	: uDBTypes.VarcharType
}


def getColumnType(cnode):
    typename = cnode.getAttribute("type")
    if typename in sized_type_mapping:
        return sized_type_mapping[typename](int(cnode.getAttribute("size")))
    elif typename in unsized_type_mapping:
        return unsized_type_mapping[typename]()
    else:
        raise Exception, "Unknown PDL type %s" % typename

keytype_mapping = {
    "INDEX"		: uDBSchema.Index,
    "PRIMARYKEY"	: uDBSchema.PrimaryKey,
    "UNIQUE"	: uDBSchema.UniqueKey
}


def getKeyDef(knode):
    columns = [col.getAttribute("name") for col in knode.childNodes if col.nodeType ==
               dom.Node.ELEMENT_NODE and col.tagName == "COLUMNNAME"]
    return keytype_mapping[knode.tagName](knode.getAttribute("name"), columns)


act_type_mapping = {
    "cascade":	"CASCADE",
    "setdef":	"SET DEFAULT",
    "setnull":	"SET NULL",
    "noaction":	"NO ACTION",
    "":		"NO ACTION"
}


def getFKDef(knode):
    refs = [(node.getAttribute("colname"), node.getAttribute("references"))
            for node in knode.childNodes if node.nodeType == dom.Node.ELEMENT_NODE and node.tagName == "REFERENCE"]

    return uDBSchema.ForeignKey(knode.getAttribute("name"), knode.getAttribute("reftable"), ([x[0] for x in refs], [x[1] for x in refs]), act_type_mapping[knode.getAttribute("ondelete")], act_type_mapping[knode.getAttribute("onupdate")])


def getCheckDef(knode):
    return uDBSchema.Check(knode.getAttribute("name"), nodeTextContent(knode).strip('\n').strip('  ').strip('\t'))


def getColumnDefault(cnode):
    def_nodes = [node for node in cnode.childNodes if node.nodeType ==
                 dom.Node.ELEMENT_NODE and node.tagName in ('DEFSYSDATE', 'DEFVAL')]

    if not def_nodes:
        return None
    elif def_nodes[0].tagName == 'DEFSYSDATE':
        return uDBTypes.DefSysDate()
    else:
        rv = def_nodes[0].getAttribute('value')
        try:
            rv = int(rv)
        except:
            if not rv.startswith("'") or not rv.endswith("'") or len(rv) == 1:
                rv = "'" + rv + "'"

        return rv


def getTableFromDOMElement(elem):
    name = elem.getAttribute("name")
    columns, constraints, indexes = [], [], []

    for child in [node for node in elem.childNodes if node.nodeType == dom.Node.ELEMENT_NODE]:
        if child.tagName == "SERIAL":
            startswith = 1
            try:
                startswith = int(child.getAttribute("startwith"))
            except:
                pass
            columns.append(
                uDBSchema.Column(child.getAttribute("name").lower(), uDBTypes.SerialType(startswith), False, None))
        elif child.tagName == "COLUMN":
            columns.append(uDBSchema.Column(child.getAttribute("name").lower(), getColumnType(
                child), not (child.getAttribute("nullable") == "no"), getColumnDefault(child)))
        elif child.tagName in ("PRIMARYKEY", "UNIQUE"):
            constraints.append(getKeyDef(child))
        elif child.tagName == "INDEX":
            indexes.append(getKeyDef(child))
        elif child.tagName == "FOREIGNKEY":
            fk = getFKDef(child)
            constraints.append(fk)
            if child.getAttribute("index") == "yes":
                indexes.append(uDBSchema.Index(fk.name + '_idx', fk.columns[0]))
        elif child.tagName == "CHECK":
            constraints.append(getCheckDef(child))

    return uDBSchema.Table(name, columns, constraints, indexes)


def getSchemaFromXML(doc):
    return [getTableFromDOMElement(elem) for elem in doc.getElementsByTagName("CREATETABLE")]


def getSchemaFromString(xml_text):
    doc = dom.parseString(xml_text)

    return getSchemaFromXML(doc)


def getSchemaFromFile(source):
    uLogging.debug("reading %s", source)
    xml_text = unsign_package(source)

    return getSchemaFromString(xml_text)


def createSchemaFromString(source, con):
    tables = getSchemaFromString(source)
    for tab in tables:
        tab.create(con)

known_rdbmss = {
    uSysDB.PgSQL: "PostgreSQL",
    uSysDB.MSSQL: "MSSQL"
}

proc_type_names = {
    uSysDB.PgSQL: {'INT': 'int', 'STRING': 'text', 'TIMESTAMP': 'timestamptz', "VOID": "void", "BLOB": "bytea"},
    uSysDB.MSSQL: {'INT': 'int', 'STRING': 'varchar(4000)', 'TIMESTAMP': 'datetime', "VOID": "void", "BLOB": "blob"}
}


class Procedure:

    def __init__(self, node):
        self.name = node.getAttribute("name")
        self.returntype = node.getAttribute("returnType") or "VOID"
        self.params = []
        self.language = self.code = None
        for child in node.childNodes:
            if child.nodeType != dom.Node.ELEMENT_NODE:
                pass
            elif child.tagName == "SPPARAMETER":
                self.params.append((child.getAttribute("name"), child.getAttribute("type")))

            elif child.tagName == "PROCBODY":
                dbtype = child.getAttribute("dbtype")
                if dbtype != known_rdbmss[uSysDB.DBType]:
                    continue

                self.language = child.getAttribute("language")
                self.code = nodeTextContent(child)

    def create(self, con):
        if not self.code:
            uLogging.debug("Wont create procedure %s", self.name)
            return

        if uSysDB.DBType == uSysDB.PgSQL:
            statement = "CREATE OR REPLACE FUNCTION "
            statement += self.name
            statement += '('
            statement += ", ".join([proc_type_names[uSysDB.DBType][x[1]] for x in self.params])
            statement += ") RETURNS "
            statement += proc_type_names[uSysDB.DBType][self.returntype]
            statement += " AS %s LANGUAGE %s"
            cur = con.cursor()
            cur.execute(statement, self.code, self.language)
        elif uSysDB.DBType == uSysDB.MSSQL:
            statement = "CREATE "
            if self.returntype == "VOID":
                statement += " PROC "
                statement += self.name
            else:
                statement += "FUNCTION "
                statement += self.name
                statement += " ( "

            statement += ", ".join(["@%s %s" % (x[0], proc_type_names[uSysDB.DBType][x[1]]) for x in self.params])

            if self.returntype != "VOID":
                statement += ") RETURNS "
                statement += proc_type_names[uSysDB.DBType][self.returntype]

            statement += " AS "
            statement += self.code
            print statement
            cur = con.cursor()
            cur.execute(statement)


def createDBStructureByManifest(manifest, counter=None):
    xml_text = unsign_package(manifest)
    doc = dom.parseString(xml_text)
    tables = getSchemaFromXML(doc)
    con = uSysDB.connect()
    view_nodes = doc.getElementsByTagName("CREATEVIEW")
    procedure_nodes = doc.getElementsByTagName("CREATEPROCEDURE")

    if counter:
        counter.set_total(len(tables) + len(view_nodes) + len(procedure_nodes) + 1)

    for tab in tables:
        if counter:
            counter.new_item(("table", tab.name))
        tab.create(con)

    cur = con.cursor()
    for node in view_nodes:
        view_name = node.getAttribute("name")
        if counter:
            counter.new_item(("view", view_name))
        sql = nodeTextContent(node)
        cur.execute("CREATE VIEW %s AS %s" % (view_name, sql))

    if counter:
        counter.new_item(("dml", "dml"))
    for node in doc.getElementsByTagName("DML"):
        sql = nodeTextContent(node)
        for stmt in uUtil.stmt_parser(sql, uSysDB.ConcatOperator, uSysDB.nowfun):
            cur.execute(stmt)

    for node in procedure_nodes:

        procedure = Procedure(node)
        if counter:
            counter.new_item(("procedure", procedure.name))
        procedure.create(con)

    con.commit()

__all__ = ["getSchemaFromFile", "getSchemaFromXML", "getSchemaFromString", "getTableFromDOMElement"]
