import uSysDB
import uLogging


def drop_extension(con, service_type):
    uLogging.debug("Dropping extension %s ...", service_type)

    cur = con.cursor()
    cur.execute(
        "DELETE FROM openapi_methods WHERE extension_id IN "
        "(SELECT extension_id FROM openapi_extensions WHERE service_type = %s)",
        (service_type))

    cur.execute(
        "DELETE FROM openapi_extensions WHERE service_type = %s", (service_type))


def register_extension(con, service_type, prefix, allowed_for):
    uLogging.debug("Registering extension %s", service_type)

    cur = con.cursor()
    cur.execute(
        "SELECT sc_id FROM sc_instances si "
        "join components c on si.component_id=c.component_id "
        "join packages p on c.pkg_id=p.pkg_id "
        "join package_interfaces pi on p.pkg_id=pi.pkg_id "
        "JOIN interfaces i ON (pi.interface_id = i.interface_id) "
        "WHERE i.service_type= %s",
        (service_type))

    sc_id_row = cur.fetchall()
    if not sc_id_row:
        raise Exception("Please insert missing interface: " + service_type)

    sc_id = sc_id_row[0][0]
    cur.execute(
        "INSERT INTO openapi_extensions (sc_id, service_type, prefix, allowed_for) VALUES "
        "(%s, %s, %s, %s)", (int(sc_id), service_type, prefix, allowed_for))


def register_method(con, method, service_type):
    method_name = method.getAttribute('name')
    signature = method.toxml()

    uLogging.debug("Registering method: %s", method_name)

    cur = con.cursor()
    cur.execute(
        "INSERT INTO openapi_methods (extension_id, method_name, signature) "
        "SELECT extension_id, %s, %s FROM openapi_extensions WHERE "
        "service_type = %s",
        (method_name, uSysDB.toRaw(str(signature)), service_type))


def process_api_extension(con, ext):
    service_type = ext.getAttribute('service_type')
    prefix = ext.getAttribute('prefix')
    allowed_for = ext.getAttribute("allowed_for")
    if allowed_for == "resellers":
        allowed_for = 'r'
    elif allowed_for == "system":
        allowed_for = 's'
    else:
        allowed_for = 'p'

    uLogging.debug("Processing Open API extension: %s", service_type)

    drop_extension(con, service_type)
    register_extension(con, service_type, prefix, allowed_for)

    methods = ext.getElementsByTagName('API:METHOD')
    for m in methods:
        register_method(con, m, service_type)


def update_extensions_from_xml(doc, con):
    api_exts = doc.getElementsByTagName('API:EXTENSION')

    if not api_exts:
        uLogging.info("Nothing to do: document does not describe any API Extensions.")
        return

    for ext in api_exts:
        process_api_extension(con, ext)

__all__ = ['update_extensions_from_xml']
