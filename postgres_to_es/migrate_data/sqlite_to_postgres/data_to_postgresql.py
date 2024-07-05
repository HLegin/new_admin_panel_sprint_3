from dataclasses import astuple

import psycopg


def data_to_postgresql_table(
    data_table_from_sqlite: list,
    conn_postgre: psycopg.Connection,
    table_colummns: str,
    schema_name: str,
    table_name: str,
    i: int,
):

    cursor_postgre = conn_postgre.cursor()

    col_count = ", ".join(["%s"] * len(table_colummns))
    table_colummns = ",".join(table_colummns)

    bind_values = ",".join(cursor_postgre.mogrify(f"({col_count})", astuple(data)) for data in data_table_from_sqlite)

    command_insert = (
        f"INSERT INTO {schema_name}.{table_name} ({table_colummns}) VALUES {bind_values}" f"ON CONFLICT (id) DO NOTHING"
    )

    cursor_postgre.execute(command_insert)

    conn_postgre.commit()
