# https://github.com/nhanb/pytaku/blob/master/src/pytaku/database/migrator.py

import datetime
import subprocess
from importlib import resources
from pathlib import Path

# TODO: Fix import
from . import migrations
from recordlivecams.database.common import get_conn, run_sql, set_db_path

"""
Forward-only DB migration scheme held together by duct tape.

- Uses `user_version` pragma to figure out what migrations are pending.
- Migrations files are in the form `./migrations/mXXXX.sql`.
"""


def _get_current_version():
    return run_sql("PRAGMA user_version;")[0][0]


def _get_version(migration: Path):
    return int(migration.name[len("m") : -len(".sql")])


def _get_pending_migrations(migrations_dir: Path):
    current_version = _get_current_version()
    migrations = sorted(migrations_dir.glob("m*.sql"))
    return [
        migration
        for migration in migrations
        if _get_version(migration) > current_version
    ]


def _read_migrations(paths):
    """Returns list of (version, sql_text) tuples"""
    results = []
    for path in paths:
        with open(path, "r") as sql_file:
            results.append((_get_version(path), sql_file.read()))
    return results


def _write_db_schema_script(migrations_dir: Path, db_path: Path):
    print(str(db_path))
    schema = subprocess.run(
        ["sqlite3", db_path, ".schema"], capture_output=True, check=True
    ).stdout
    with open(migrations_dir / Path("latest_schema.sql"), "wb") as f:
        f.write(b"-- This file is auto-generated by the migration script\n")
        f.write(b"-- for reference purposes only. DO NOT EDIT.\n\n")
        f.write(schema)


def migrate(logger, db_path: Path, overwrite_latest_schema=False):
    set_db_path(db_path)

    # If there's no existing db, create one with the correct pragmas
    if not db_path.is_file():
        run_sql("PRAGMA journal_mode = WAL;")

    with resources.path(migrations, "__init__.py") as migrations_dir:
        migrations_dir = migrations_dir.parent
        pending_migrations = _get_pending_migrations(migrations_dir)
        if not pending_migrations:
            logger.info("Nothing to migrate.")
            return
        logger.info(f"There are {len(pending_migrations)} pending migrations.")
        migration_contents = _read_migrations(pending_migrations)

        conn = get_conn()
        cursor = conn.cursor()

        # Backup first
        current_version = _get_current_version()
        if current_version != 0:
            now = datetime.datetime.now().strftime("%y-%m-%d-%H%M")
            backup_filename = f"db_backup_v{current_version}_{now}.sqlite3"
            logger.info(f"Backup up to {backup_filename}...")
            cursor.execute("VACUUM main INTO ?;", (backup_filename,))
            logger.info("done")

        # Start migrations
        # NOTE: this is NOT done in a transaction.
        # You'll need to do transactions inside your sql scripts.
        # This is to allow for drastic changes that require temporarily turning off the
        # foreign_keys pragma, which doesn't work inside transactions.
        # If anything goes wrong here, let it abort the whole script. You can always
        # restore from the backup file.
        cursor = conn.cursor()
        for version, sql in migration_contents:
            logger.info(f"Migrating version {version}...")
            cursor.executescript(sql)
            cursor.execute(f"PRAGMA user_version = {version};")

        if overwrite_latest_schema:
            _write_db_schema_script(migrations_dir, db_path)

        logger.info(f"All done. Current version: {_get_current_version()}")
