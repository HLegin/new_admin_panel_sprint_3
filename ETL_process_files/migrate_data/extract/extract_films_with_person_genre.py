from contextlib import closing
from typing import Tuple

import psycopg
from extract.dataclass_table import TABLE_DATACLASSES


def extract_films_with_person_genre(
    conn_postgre: psycopg.Connection,
    table_dataclass,
    data_person_or_genre: tuple,
    lines_in_response: int,
    offset: int,
    person_genre_film_work_all_extract: bool | str,
) -> Tuple[int, tuple]:

    name_dataclass_data_person_or_genre = "".join(data_person_or_genre.keys())

    data = tuple(
        [
            TABLE_DATACLASSES[name_dataclass_data_person_or_genre](**item)
            for item in data_person_or_genre.get(name_dataclass_data_person_or_genre, tuple())
        ]
    )

    if isinstance(person_genre_film_work_all_extract, bool | None):
        table_name = table_dataclass.__name__
        columns = ", ".join(list(table_dataclass.__annotations__.keys()))
        select_dataclass = table_dataclass
    else:
        table_name = person_genre_film_work_all_extract
        columns = ", ".join(list(TABLE_DATACLASSES[person_genre_film_work_all_extract].__annotations__.keys()))
        select_dataclass = TABLE_DATACLASSES[person_genre_film_work_all_extract]

    if len(data) != 0:

        persons_genres_ids = tuple(item.id for item in data)

        command_read = f"""SELECT {columns} 
                            FROM {table_name}
                            WHERE {name_dataclass_data_person_or_genre}_id IN ({','.join(['%s'] * len(persons_genres_ids))}) 
                            LIMIT {lines_in_response} 
                            OFFSET {offset * lines_in_response}
                        """

        with closing(conn_postgre.cursor()) as cursor_postgre:

            data_read = cursor_postgre.execute(command_read, persons_genres_ids).fetchall()

            if len(data_read) != 0:
                n = 0
                while n < len(data_read):
                    film_work_id = data_read.pop(0)

                    data_read.append(select_dataclass(**film_work_id))
                    n += 1

                offset += 1
                person_genre_film_work_all_extract = str(select_dataclass.__name__)
            else:
                offset = 0
                person_genre_film_work_all_extract = True

            return offset, person_genre_film_work_all_extract, tuple(data_read)

    person_genre_film_work_all_extract = True
    offset = 0

    return offset, person_genre_film_work_all_extract, tuple()
