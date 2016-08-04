from utils import make_connection_postgres, make_connection_mysql, close_connection_postgres, close_connection_mysql


def migrate_messages():
    print("MIGRATING MESSAGES...")
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "Select me_id, me_origen_id, me_destino_id, me_mensaje, me_fecha, me_asunto from mensaje"

    cursor_m.execute(query)

    for (me_id, me_origen_id, me_destino_id, me_mensaje, me_fecha, me_asunto) in cursor_m:
        cursor_p.execute(
            'INSERT INTO core_message (id, from_user_id, to_user_id, body, date, subject) VALUES (%s, %s, %s, %s, %s, %s)',
            (me_id, me_origen_id, me_destino_id, me_mensaje, me_fecha, me_asunto))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)