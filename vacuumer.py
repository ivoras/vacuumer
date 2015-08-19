#!/usr/bin/python
import sys, os
from getopt import getopt, GetoptError
import getpass
import psycopg2
from time import time

PG_DB = 'template1'
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = getpass.getuser()
PG_PASSWORD = None
DO_IT = True

def help():
    print "usage: %s [-d database] [-h host] [-p port] [-U user] [-W password] [-n]" % sys.argv[0]
    print "-d\tdatabase\tConnect to a database (doesn't matter which)"
    print "-h\thost\t\tPostgreSQL host"
    print "-p\tport\t\tPort"
    print "-U\tuser\t\tUsername"
    print "-W\tpassword\tPassword"
    print "-n\tDo not do anything which changes database state or data"

def pg_database_size(db, dbname):
    cur = db.cursor()
    cur.execute('SELECT pg_database_size(%s)', (dbname,) )
    return cur.fetchone()[0]

def main():
    global PG_DB, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD

    try:
        opts, args = getopt(sys.argv[1:], 'd:h:p:U:W:n')
    except GetoptError:
        help()
        sys.exit(1)

    for o, a in opts:
        if o == '-d':
            PG_DB = a
        elif o == '-h':
            PG_HOST = a
        elif o == '-p':
            PG_PORT = int(a)
        elif o == '-U':
            PG_USER = a
        elif o == '-W':
            PG_PASSWORD = a
        elif o == '-n':
            DO_IT = False

    db = psycopg2.connect(database = PG_DB, host = PG_HOST, port = PG_PORT, user = PG_USER, password = PG_PASSWORD)
    db.autocommit = True

    print "Connected to %s at %s:%d" % (PG_DB, PG_HOST, PG_PORT)

    cur = db.cursor()
    cur.execute('SELECT usesysid FROM pg_user WHERE usename=%s', (PG_USER, ))
    user_id = cur.fetchone()[0]

    cur.execute('SELECT datname FROM pg_database WHERE datdba=%s', (user_id, ))
    databases = [row[0] for row in cur.fetchall()]

    print "Discovered %d databases: %s" % (len(databases), ", ".join(databases))

    max_dbname_col = max([len(n) for n in databases]) + 2
    sys.stdout.write(("%" + str(max_dbname_col) + "s | %15s | %8s | %15s |\n") % ('Database', 'Old size (MiB)', 'V.time', 'New size (MiB)'))
    sys.stdout.write('-' * (max_dbname_col + 49) + "\n")

    t0 = time()
    for dbname in databases:
        size = pg_database_size(db, dbname)
        sys.stdout.write(("%" + str(max_dbname_col) + "s | %15.1f ") % (dbname, size / (1024.0 * 1024.0)))
        sys.stdout.flush()
        t1 = time()
        db2 = psycopg2.connect(database = dbname, host = PG_HOST, port = PG_PORT, user = PG_USER, password = PG_PASSWORD)
        db2.autocommit = True
        cur = db2.cursor()
        cur.execute('VACUUM FULL ANALYZE')
        tm = time() - t1
        new_size = pg_database_size(db2, dbname)
        sys.stdout.write("| %8.0f | %15.1f |\n" % (tm, new_size / (1024.0 * 1024.0)))
        sys.stdout.flush()

    print "Total time: %0.1fs" % (time() - t0)

if __name__ == '__main__':
    main()
