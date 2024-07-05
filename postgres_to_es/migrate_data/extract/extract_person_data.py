import datetime
from contextlib import closing
from typing import Tuple

import psycopg


def extract_persons_genres_data(
    last_updated_time_person: str | None,
    lines_in_response: int,
    conn_postgre: psycopg.Connection,
    table_dataclass,
    table_name: str,
) -> Tuple[datetime.datetime | None, tuple]:

    try:

        with closing(conn_postgre.cursor()) as cursor_postgre:

            if last_updated_time_person is None:
                command = f"""SELECT {', '.join(tuple(table_dataclass.__annotations__.keys()))} 
                                FROM {table_name} 
                                ORDER BY updated_at 
                                LIMIT {lines_in_response}"""

                data_persons_or_genre = cursor_postgre.execute(command).fetchall()
            else:
                command = f"""SELECT {', '.join(tuple(table_dataclass.__annotations__.keys()))}
                                FROM {table_name} 
                                WHERE updated_at > '{datetime.datetime.fromisoformat(last_updated_time_person)}' 
                                ORDER BY updated_at 
                                LIMIT {lines_in_response}"""

                data_persons_or_genre = cursor_postgre.execute(command).fetchall()

            if len(data_persons_or_genre) != 0:
                n = 0
                while n < len(data_persons_or_genre):
                    data = data_persons_or_genre.pop(0)

                    data_persons_or_genre.append(table_dataclass(**data))

                    n += 1

                last_extract_updated_at = data_persons_or_genre[len(data_persons_or_genre) - 1].updated_at
            else:
                last_extract_updated_at = None

        return (last_extract_updated_at, tuple(data_persons_or_genre))

    except Exception:
        raise
