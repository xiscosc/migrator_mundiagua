from utils import make_connection_postgres, make_connection_mysql, close_connection_postgres, close_connection_mysql


def migrate_clients():
    print("MIGRATING CLIENTS...")
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT c_id, c_nombre, c_email, c_cod, c_dni FROM cliente"

    cursor_m.execute(query)

    for (c_id, c_nombre, c_email, c_cod, c_dni) in cursor_m:
        cursor_p.execute(
            'INSERT INTO client_client (id, name, email, intern_code, dni) VALUES (%s, %s, %s, %s, %s)',
            (c_id, c_nombre, c_email, c_cod, c_dni))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)
    print("MIGRATING CLIENT-PHONES...")
    migrate_phones()
    print("MIGRATING CLIENTS-ADDRS...")
    migrate_addresses()


def migrate_phones():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT t_id, t_c_id, t_telefono, t_alias from telefono_cliente"

    cursor_m.execute(query)

    for (t_id, t_c_id, t_telefono, t_alias) in cursor_m:
        trim_phone = t_telefono.replace(" ", "")
        cursor_p.execute(
            'INSERT INTO client_phone (id, client_id, phone, alias) VALUES (%s, %s, %s, %s)',
            (t_id, t_c_id, trim_phone, t_alias))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_addresses():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT d_id, d_c_id, d_z_id, d_direccion, d_alias, d_latitud, d_longitud from direccion_cliente"

    cursor_m.execute(query)

    for (d_id, d_c_id, d_z_id, d_direccion, d_alias, d_latitud, d_longitud) in cursor_m:
        cursor_p.execute(
            'INSERT INTO client_address (id, client_id, default_zone_id, address, alias, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s)',
            (d_id, d_c_id, d_z_id, d_direccion, d_alias, d_latitud, d_longitud))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)