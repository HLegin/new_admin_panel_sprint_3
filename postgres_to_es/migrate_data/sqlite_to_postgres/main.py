import argparse
import os
import sqlite3
from contextlib import closing

import psycopg
from data_from_sqlite import data_from_sqlite_table

SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
TABLE_NAMES = os.environ.get("TABLE_NAMES_SQLITE_TO_POSTGRES").split(",")

DB_POSTGRESQL = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "options": "-c search_path={}".format(SCHEMA_NAME),
}

PATH_TO_DDL = os.path.join(os.path.dirname(__file__), ".ddl")


def main(lines_in_response: int):
    try:
        with closing(sqlite3.connect(DB_SQLITE_PATH)) as conn_sqlite, closing(
            psycopg.connect(
                **DB_POSTGRESQL,
                row_factory=psycopg.rows.dict_row,
                cursor_factory=psycopg.ClientCursor,
            )
        ) as conn_postgre:

            cursor_postgre_check = conn_postgre.cursor()

            with open(PATH_TO_DDL, "r", encoding="utf-8") as file:
                split_commands = file.read().split("-- CREATE INDEX")

                create_shema_tables = split_commands[0]
                create_index = split_commands[1]

                cursor_postgre_check.execute("""{}""".format(create_shema_tables.strip()))
                conn_postgre.commit()

            stop_migrate = False
            for table_name in TABLE_NAMES:

                check_command = f"SELECT count(*) FROM {SCHEMA_NAME}.{table_name}"
                count_lines = cursor_postgre_check.execute(check_command).fetchone()

                if count_lines["count"] != 0:
                    stop_migrate = True
                    break

            if stop_migrate is False:
                for table_name in TABLE_NAMES:
                    data_from_sqlite_table(
                        table_name,
                        lines_in_response,
                        conn_sqlite,
                        conn_postgre,
                        SCHEMA_NAME,
                    )

                cursor_postgre_check.execute("""{}""".format(create_index.strip()))
                conn_postgre.commit()
    except Exception:
        pass


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--pathsqlite", type=str)
    args = parser.parse_args()

    DB_SQLITE_PATH = args.pathsqlite

    lines_in_response = 100

    main(lines_in_response)
