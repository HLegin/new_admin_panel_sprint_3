import datetime
import logging
from contextlib import closing
from typing import Tuple

import psycopg
from extract.dataclass_table import FullFilm


def extract_film_work_data(
    last_updated_time_film_work: str | None,
    lines_in_response: int,
    conn_postgre: psycopg.Connection,
    table_dataclass,
    table_name: str,
) -> Tuple[datetime.datetime | None, tuple]:

    with closing(conn_postgre.cursor()) as cursor_postgre:

        if last_updated_time_film_work is None:
            command = f"""SELECT {', '.join(tuple(table_dataclass.__annotations__.keys()))} 
                            FROM {table_name} 
                            ORDER BY updated_at 
                            LIMIT {lines_in_response}"""

            data_film_work = cursor_postgre.execute(command).fetchall()
        else:
            command = f"""SELECT {', '.join(tuple(table_dataclass.__annotations__.keys()))}
                            FROM {table_name} 
                            WHERE updated_at > '{datetime.datetime.fromisoformat(last_updated_time_film_work)}' 
                            ORDER BY updated_at 
                            LIMIT {lines_in_response}"""

            data_film_work = cursor_postgre.execute(command).fetchall()

        if len(data_film_work) != 0:
            ids_films = tuple([item.get("id", None) for item in data_film_work])

            command = f"""SELECT p.full_name, pfw.role, p.id, pfw.film_work_id
                            FROM person p
                            JOIN person_film_work pfw ON p.id = pfw.person_id
                            WHERE pfw.film_work_id IN ({', '.join(f"'{str(uuid)}'" for uuid in ids_films)})"""

            data_persons = cursor_postgre.execute(command).fetchall()

            command = f"""SELECT g.name, g.id ,gfw.film_work_id
                            FROM genre g
                            JOIN genre_film_work gfw ON g.id = gfw.genre_id
                            WHERE gfw.film_work_id IN ({', '.join(f"'{str(uuid)}'" for uuid in ids_films)})"""

            data_genres = cursor_postgre.execute(command).fetchall()

            for i, ids in enumerate(ids_films):

                data_names_persons = tuple(
                    filter(
                        None,
                        list(
                            map(
                                lambda x: (
                                    (x["full_name"], x["role"], x["id"]) if x.get("film_work_id") == ids else None
                                ),
                                data_persons,
                            )
                        ),
                    )
                )

                data_names_genres = tuple(
                    filter(
                        None,
                        list(
                            map(lambda x: (x["name"], x["id"]) if x.get("film_work_id") == ids else None, data_genres)
                        ),
                    )
                )

                temp = data_film_work.pop(i)

                temp.update({"persons": data_names_persons, "genres": data_names_genres})

                data_film_work.insert(i, temp)

            n = 0
            while n < len(data_film_work):
                data = data_film_work.pop(0)
                data_film_work.append(FullFilm(**data))

                n += 1

            last_extract_updated_at = data_film_work[len(data_film_work) - 1].updated_at
        else:
            last_extract_updated_at = None

    return last_extract_updated_at, tuple(data_film_work)
