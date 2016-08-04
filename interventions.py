from utils import make_connection_postgres, make_connection_mysql, close_connection_postgres, close_connection_mysql


def migrate_interventions():
    print("MIGRATING INTERVENTIONS...")
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT a_id, a_d_id, a_z_id, a_e_id, a_crea_id, a_asignado_id, a_nota, a_fecha, a_notaoperario from aviso"

    cursor_m.execute(query)

    for (a_id, a_d_id, a_z_id, a_e_id, a_crea_id, a_asignado_id, a_nota, a_fecha, a_notaoperario) in cursor_m:
        cursor_p.execute(
            'INSERT INTO intervention_intervention (id, address_id, zone_id, status_id, created_by_id, assigned_id, description, date, note, starred) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)',
            (a_id, a_d_id, a_z_id, a_e_id, a_crea_id, a_asignado_id, a_nota, a_fecha, a_notaoperario))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)
    print("MIGRATING INTERVENTIONS-LOGS...")
    migrate_intervention_logs()
    print("MIGRATING INTERVENTIONS-MODIFICATIONS...")
    migrate_interventions_modifications()


def migrate_interventions_modifications():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT m_id, m_a_id, m_u_id, m_modificacion, m_fecha from modificacion_aviso"

    cursor_m.execute(query)

    for (m_id, m_a_id, m_u_id, m_modificacion, m_fecha) in cursor_m:
        cursor_p.execute(
            'INSERT INTO intervention_interventionmodification (id, intervention_id, created_by_id, note, date) VALUES (%s, %s, %s, %s, %s)',
            (m_id, m_a_id, m_u_id, m_modificacion, m_fecha))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_intervention_logs():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT lav_id, lav_a_id, lav_e_id, lav_asignado_id, lav_u_id, lav_fecha from log_aviso"

    cursor_m.execute(query)

    for (lav_id, lav_a_id, lav_e_id, lav_asignado_id, lav_u_id, lav_fecha) in cursor_m:
        cursor_p.execute(
            'INSERT INTO intervention_interventionlog (id, intervention_id, status_id, assigned_id, created_by_id, date) VALUES (%s, %s, %s, %s, %s, %s)',
            (lav_id, lav_a_id, lav_e_id, lav_asignado_id, lav_u_id, lav_fecha))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)