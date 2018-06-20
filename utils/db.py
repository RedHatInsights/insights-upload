"""
Intialize or Connect to a sqlite database to store file and hash info
"""
import sqlite3

from pathlib import Path

db_path = Path("/datastore/uploader.db")


def createdb(path):

    if db_path.is_file():
        return
    else:
        conn = sqlite3.connect(path)
        c = conn.cursor()

        c.execute('''CREATE TABLE uploads
                  (id integer primary key autoincrement,
                   hash varchar(34) not null,
                   principle varchar(30) not null,
                   rh_account int(20) not null,
                   status varchar(20) not null,
                   time datetime default current_timestamp not null)''')

        conn.commit()


def create_connection(path):
    try:
        conn = sqlite3.connect(path)
        return conn
    except Error as e:
        print(e)

    return None


def write_to_db(vals):
    conn = create_connection("/datastore/uploader.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO uploads (hash, principle, rh_account, status) VALUES ('{hash}', '{principle}', '{rh_account}', '{status}')".format(**vals))
    conn.commit()
    conn.close()


def update_status(hash_, status):
    conn = create_connection("/datastore/uploader.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE uploads SET status='%s' WHERE hash='%s'" % (status, hash_))
    conn.commit()
    conn.close()

def upload_status(hash_):
    conn = create_connection("/datastore/uploader.db")
    cursor = conn.cursor()
    response = cursor.execute("SELECT status, time FROM uploads WHERE hash='%s'" % hash_).fetchone()
    conn.close()
    if response:
        return response
    else:
        return 'Unique id does not exist'
