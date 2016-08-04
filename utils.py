import psycopg2
import mysql.connector


def make_connection_mysql():
    cnx = mysql.connector.connect(user='root', database='db', password="123456")
    cursor = cnx.cursor()
    return cnx, cursor


def close_connection_mysql(cnx, cursor):
    cursor.close()
    cnx.close()


def make_connection_postgres():
    conn = psycopg2.connect(user="postgres", password="123456", database="db", host="localhost")
    cur = conn.cursor()
    return conn, cur


def close_connection_postgres(cnx, cur):
    cnx.commit()
    cur.close()
    cnx.close()