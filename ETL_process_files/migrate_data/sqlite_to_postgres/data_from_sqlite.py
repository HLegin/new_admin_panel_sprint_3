import sqlite3

import psycopg
from data_to_postgresql import data_to_postgresql_table
from dataclass_table import TABLE_DATACLASSES


def data_from_sqlite_table(
    table_name: str,
    lines_in_response: int,
    conn_sqlite: sqlite3.Connection,
    conn_postgre: psycopg.Connection,
    schema_name: str,
):
    cursor_sqlite = conn_sqlite.cursor()

    count_lines_in_table = cursor_sqlite.execute("SELECT count(*) FROM {}".format(table_name)).fetchone()[0]

    data_table_class = TABLE_DATACLASSES[table_name]

    data_table_class_info = dict(data_table_class.__annotations__)

    table_info = cursor_sqlite.execute("PRAGMA table_info({})".format(table_name)).fetchall()

    table_colummns = [item[1] for item in table_info]
    table_colummns_for_command = [item[1] for item in table_info]

    for i, field_in_table_class in enumerate(data_table_class_info.keys()):
        try:
            index_column = table_colummns_for_command.index(field_in_table_class)
        except ValueError:
            table_colummns_for_command.insert(i, "NULLIF(NULL,1)")
            table_colummns.insert(i, field_in_table_class)
            continue

        if index_column != i:
            table_colummns.insert(i, table_colummns.pop(index_column))
            table_colummns_for_command.insert(i, table_colummns_for_command.pop(index_column))

    for i in range(count_lines_in_table):
        cursor_sqlite.execute(
            "SELECT {} FROM {} LIMIT {} OFFSET {}".format(
                ",".join(table_colummns_for_command),
                table_name,
                lines_in_response,
                i * lines_in_response,
            )
        )

        read_rows = cursor_sqlite.fetchmany(lines_in_response)

        if len(read_rows) == 0:
            break

        data_from_sqlite = []
        for read_row in read_rows:
            data_from_sqlite.append(data_table_class(*read_row))

        data_to_postgresql_table(data_from_sqlite, conn_postgre, table_colummns, schema_name, table_name, i)
