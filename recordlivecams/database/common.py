from pathlib import Path
import sqlite3

_conn = None
db_path = None


def set_db_path(db_path_in: Path):
    global db_path
    db_path = db_path_in


def get_conn():
    global _conn
    if not _conn:
        _conn = sqlite3.Connection(
            db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        # Apparently you need to enable this pragma per connection:
        _conn.cursor().execute("PRAGMA foreign_keys = ON;")
        _conn.row_factory = sqlite3.Row
        # No idea what the default db busy timeout is, but apparently it's super strict:
        # got BusyError almost consistently when clicking "finish" on latest chapter,
        # making FE send both a "read" and "get title" request at roughly the same time.
        # It still makes no sense though, since "get title" is supposedly a read-only
        # operation and only "read" is a write. According to docs, WAL mode allows 1
        # writer and unlimited readers at the same time. WTF guys?
        # But anyway, until I can get to the bottom of it, let's slap on a band-aid:
        # _conn.setbusytimeout(1000)
        # _conn.setrowtrace(_row_trace)
    return _conn


def run_sql(*args, return_num_affected=False, **kwargs):
    cursor = run_sql_on_demand(*args, **kwargs)
    if return_num_affected:
        return cursor.execute("select changes();").fetchone()
    return list(cursor)


def run_sql_on_demand(*args, **kwargs):
    return get_conn().cursor().execute(*args, **kwargs)


def run_sql_many(*args, **kwargs):
    return get_conn().cursor().executemany(*args, **kwargs)
