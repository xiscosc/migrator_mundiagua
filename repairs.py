from utils import make_connection_postgres, make_connection_mysql, close_connection_postgres, close_connection_mysql


def migrate_repairs():
    print("MIGRATING REPAIRS-ATH...")
    migrate_repairs_ath()
    print("MIGRATING REPAIRS-IDEGIS...")
    migrate_repairs_idegis()
    print("MIGRATING REPAIRS-ATH-LOGS...")
    migrate_logs_ath()
    print("MIGRATING REPAIRS-IDEGIS-LOGS...")
    migrate_logs_idegis()


def migrate_repairs_ath():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT ra_id, ra_estado, ra_sn, ra_modelo, ra_presupuesto, ra_d_id, ra_fecha, ra_creado_id, ra_garantia," \
            " ra_bypass, ra_conector, ra_transformador, ra_nota, ra_interno, ra_anoequipo, ra_fabricante, ra_online " \
            "from reparacion_ath ORDER by ra_id asc"

    cursor_m.execute(query)

    last_id = 0
    for (ra_id, ra_estado, ra_sn, ra_modelo, ra_presupuesto, ra_d_id, ra_fecha, ra_creado_id, ra_garantia, ra_bypass,
         ra_conector, ra_transformador, ra_nota, ra_interno, ra_anoequipo, ra_fabricante, ra_online) in cursor_m:
        b_online = "A"+ra_online
        b_transformer = bool(int(ra_transformador))
        b_warranty = bool(int(ra_garantia))
        b_bypass = bool(int(ra_bypass))
        b_connector = bool(int(ra_conector))
        cursor_p.execute(
            'INSERT INTO repair_athrepair (id, status_id, serial_number, model, budget_id, address_id, date, created_by_id, warranty, bypass, connector, transformer, description, intern_description, year, notice_maker_number, online_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (ra_id, ra_estado, ra_sn, ra_modelo, ra_presupuesto, ra_d_id, ra_fecha, ra_creado_id, b_warranty, b_bypass,
         b_connector, b_transformer, ra_nota, ra_interno, ra_anoequipo, ra_fabricante, b_online))
        last_id = ra_id
    last_id += 1
    cursor_p.execute('ALTER SEQUENCE repair_athrepair_id_seq RESTART WITH ' + str(last_id))
    print("\tUPDATED LAST ID REPAIR-ATH " + str(last_id))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_repairs_idegis():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT ri_id, ri_estado, ri_sn, ri_modelo, ri_presupuesto, ri_d_id, ri_fecha, ri_creado_id, ri_garantia," \
            " ri_sondaph, ri_orp, ri_electrodo, ri_nota, ri_interno, ri_anoequipo, ri_fabricante, ri_online " \
            "from reparacion_idegis ORDER by ri_id asc"

    cursor_m.execute(query)
    last_id = 0
    for (ri_id, ri_estado, ri_sn, ri_modelo, ri_presupuesto, ri_d_id, ri_fecha, ri_creado_id, ri_garantia, ri_sondaph,
         ri_orp, ri_electrodo, ri_nota, ri_interno, ri_anoequipo, ri_fabricante, ri_online) in cursor_m:
        b_online = "X"+ri_online
        b_orp = bool(int(ri_orp))
        b_warranty = bool(int(ri_garantia))
        b_electrode = bool(int(ri_electrodo))
        b_ph = bool(int(ri_sondaph))
        cursor_p.execute(
            'INSERT INTO repair_idegisrepair (id, status_id, serial_number, model, budget_id, address_id, date, created_by_id, warranty, ph, electrode, orp, description, intern_description, year, notice_maker_number, online_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (ri_id, ri_estado, ri_sn, ri_modelo, ri_presupuesto, ri_d_id, ri_fecha, ri_creado_id, b_warranty, b_ph,
         b_electrode, b_orp, ri_nota, ri_interno, ri_anoequipo, ri_fabricante, b_online))
        last_id = ri_id
    last_id += 1
    cursor_p.execute('ALTER SEQUENCE repair_idegisrepair_id_seq RESTART WITH ' + str(last_id))
    print("\tUPDATED LAST ID REPAIR-IDEGIS " + str(last_id))
    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_logs_ath():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "Select la_ra_id, la_er_id, la_fecha from log_ath"

    cursor_m.execute(query)

    for (la_ra_id, la_er_id, la_fecha) in cursor_m:
        cursor_p.execute(
            'INSERT INTO repair_athrepairlog (repair_id, status_id, date) VALUES (%s, %s, %s)',
            (la_ra_id, la_er_id, la_fecha))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_logs_idegis():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "Select li_ri_id, li_er_id, li_fecha from log_idegis"

    cursor_m.execute(query)

    for (li_ri_id, li_er_id, li_fecha) in cursor_m:
        cursor_p.execute(
            'INSERT INTO repair_idegisrepairlog (repair_id, status_id, date) VALUES (%s, %s, %s)',
            (li_ri_id, li_er_id, li_fecha))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)