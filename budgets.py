from utils import make_connection_postgres, make_connection_mysql, close_connection_postgres, close_connection_mysql
import json


def migrate_budgets():
    print("MIGRATING BUDGETS...")
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT p_id, p_d_id, p_u_id, p_fecha, p_intro, p_condiciones, p_impuesto, p_nulo from presupuesto"

    cursor_m.execute(query)

    for (p_id, p_d_id, p_u_id, p_fecha, p_intro, p_condiciones, p_impuesto, p_nulo) in cursor_m:
        nulo = bool(int(p_nulo))
        cursor_p.execute(
            'INSERT INTO budget_budgetstandard (id, address_id, created_by_id, date, introduction, conditions, tax, invalid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
            (p_id, p_d_id, p_u_id, p_fecha, p_intro, p_condiciones, p_impuesto, nulo))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)
    print("ADDING BUDGET LINES...")
    migrate_lines_budget()


def migrate_lines_budget():
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT p_id, p_contenido from presupuesto"

    cursor_m.execute(query)

    for (p_id, p_contenido) in cursor_m:
        lines = json.loads(p_contenido)
        try:
            count = len(lines['nombre'])
        except:
            count = 0
        for x in xrange(count):
            try:
                lines['precio'][x] = float(lines['precio'][x])
            except ValueError:
                lines['precio'][x] = 0

            try:
                lines['unidades'][x] = float(lines['unidades'][x])
            except ValueError:
                lines['unidades'][x] = 0

            try:
                lines['dto'][x] = float(lines['dto'][x])
            except ValueError:
                lines['dto'][x] = 0

            cursor_p.execute(
                'INSERT INTO budget_budgetlinestandard (budget_id, product, unit_price, quantity, discount) VALUES (%s, %s, %s, %s, %s)',
                (p_id, lines['nombre'][x], lines['precio'][x], lines['unidades'][x], lines['dto'][x]))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)


def migrate_budgets_repair():
    print("MIGRATING BUDGETS-REPAIR...")
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    query = "SELECT pr_id, pr_u_id, pr_fecha, pr_intro, pr_condiciones, pr_impuesto from presupuesto_reparacion"

    cursor_m.execute(query)
    data = []
    for row in cursor_m:
        data.append(row)

    cursor_m.close()
    cursor_m = cnx_m.cursor()
    final_data = []
    for d in data:
        id_repair = d[0][1:]
        prefix = d[0][:1].encode('UTF-8')

        if prefix is 'x' or prefix is 'X':
            is_ath = False
            query2 = "SELECT ri_id, ri_d_id from reparacion_idegis where ri_id = " + id_repair
        else:
            is_ath = True
            query2 = "SELECT ra_id, ra_d_id from reparacion_ath where ra_id = " + id_repair

        cursor_m.execute(query2)
        ids = cursor_m.fetchall()
        final_data.append({'add': ids[0][1], 'data': d, 'id_repair': id_repair, 'is_ath': is_ath})

    lines_data = []
    for fd in final_data:
        tax = float(fd['data'][5])
        if fd['is_ath']:
            cursor_p.execute(
                'INSERT INTO budget_budgetrepair (address_id, created_by_id, date, introduction, conditions, tax, invalid, ath_repair_id, intern_id ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
                (fd['add'], fd['data'][1], fd['data'][2], fd['data'][3], fd['data'][4], tax, False, fd['id_repair'], 1))
        else:
            cursor_p.execute(
                'INSERT INTO budget_budgetrepair (address_id, created_by_id, date, introduction, conditions, tax, invalid, idegis_repair_id, intern_id ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
                (fd['add'], fd['data'][1], fd['data'][2], fd['data'][3], fd['data'][4], tax, False, fd['id_repair'], 1))

        id_insert = cursor_p.fetchone()[0]
        lines_data.append({'id_budget': id_insert, 'id_complete': fd['data'][0]})


    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)
    print("ADDING BUDGET-REPAIR LINES...")
    migrate_lines_budget_repair(lines_data)


def migrate_lines_budget_repair(data):
    cnx_m, cursor_m = make_connection_mysql()
    cnx_p, cursor_p = make_connection_postgres()

    for d in data:
        query = "SELECT pr_contenido from presupuesto_reparacion where pr_id='" + d['id_complete'] + "'"
        cursor_m.execute(query)
        lines_srt = cursor_m.fetchall()[0][0]
        lines = json.loads(lines_srt)
        try:
            count = len(lines['nombre'])
        except:
            count = 0
        for x in xrange(count):
            try:
                lines['precio'][x] = float(lines['precio'][x])
            except ValueError:
                lines['precio'][x] = 0

            try:
                lines['unidades'][x] = float(lines['unidades'][x])
            except ValueError:
                lines['unidades'][x] = 0

            try:
                lines['dto'][x] = float(lines['dto'][x])
            except ValueError:
                lines['dto'][x] = 0

            cursor_p.execute(
                'INSERT INTO budget_budgetlinerepair (budget_id, product, unit_price, quantity, discount) VALUES (%s, %s, %s, %s, %s)',
                (d['id_budget'], lines['nombre'][x], lines['precio'][x], lines['unidades'][x], lines['dto'][x]))

    close_connection_mysql(cnx_m, cursor_m)
    close_connection_postgres(cnx_p, cursor_p)
