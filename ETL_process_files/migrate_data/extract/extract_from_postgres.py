import psycopg
from extract.dataclass_table import TABLE_DATACLASSES
from extract.extract_film_work_data import extract_film_work_data
from extract.extract_films_with_person_genre import extract_films_with_person_genre
from extract.extract_person_data import extract_persons_genres_data
from settings.config import TABLE_NAMES
from settings.etl_states import RedisState

def extract_from_postgres(lines_in_response: int, redis_state: RedisState, conn_postgre: psycopg.Connection):
    
    result_persons_genres_in_film = {}
    for i, table_name in enumerate(TABLE_NAMES):

        table_dataclass = TABLE_DATACLASSES[table_name]
        
        (person_genre_film_work_all_extract,) = redis_state.get_state(("person_genre_film_work_all_extract",))

        if (i == 0) and (person_genre_film_work_all_extract is True or person_genre_film_work_all_extract is None):

            (last_updated_time_film_work,) = redis_state.get_state((f"{table_name}_updated_at",))

            last_extract_updated_at, data_film_work = extract_film_work_data(
                last_updated_time_film_work, lines_in_response, conn_postgre, table_dataclass, table_name
            )

            if last_extract_updated_at is not None:
                redis_state.save_state({f"{table_name}_updated_at": last_extract_updated_at})
                redis_state.save_state({"films_all_extract": False})
            else:
                redis_state.save_state({"films_all_extract": True})

            result_persons_genres_in_film = {}

        all_films_extract, person_genre_film_work_all_extract = redis_state.get_state(
            ("films_all_extract", "person_genre_film_work_all_extract")
        )

        if (
            (i == 1 or i == 3)
            and (all_films_extract is True)
            and ((person_genre_film_work_all_extract is True) or (person_genre_film_work_all_extract is None))
        ):
            (last_updated_time_person,) = redis_state.get_state((f"{table_name}_updated_at",))

            last_extract_updated_at, data_person_or_genre = extract_persons_genres_data(
                last_updated_time_person, lines_in_response, conn_postgre, table_dataclass, table_name
            )

            if last_extract_updated_at is not None:
                redis_state.save_state({f"{table_name}_updated_at": last_extract_updated_at})

            redis_state.save_state({"person_or_genre_data": {table_dataclass.__name__: data_person_or_genre}})

        all_films_extract, person_genre_film_work_all_extract = redis_state.get_state(
            ("films_all_extract", "person_genre_film_work_all_extract")
        )


        (data_person_or_genre,) = redis_state.get_state(("person_or_genre_data",))

        if (
            (i == 2 or i == 4)
            and (all_films_extract is True)
            and (
                (person_genre_film_work_all_extract is True)
                or (person_genre_film_work_all_extract is None)
                or (person_genre_film_work_all_extract == table_dataclass.__name__)
            )
        ):
            (offset,) = redis_state.get_state((f"{table_name}_offset",))

            if offset is None:
                raise Exception(f"Смещение для таблицы {table_name} не найдено! Проверьте состояния!")

            (
                offset,
                person_genre_film_work_all_extract_update,
                persons_genres_in_film,
            ) = extract_films_with_person_genre(
                conn_postgre,
                table_dataclass,
                data_person_or_genre,
                lines_in_response,
                offset,
                person_genre_film_work_all_extract,
            )

            redis_state.save_state({f"{table_name}_offset": offset})
            redis_state.save_state({"person_genre_film_work_all_extract": person_genre_film_work_all_extract_update})

            if isinstance(person_genre_film_work_all_extract, bool | None):
                key_name = table_dataclass.__name__
            else:
                key_name = person_genre_film_work_all_extract

            result_persons_genres_in_film[key_name] = tuple(persons_genres_in_film)

            data_film_work = tuple()

    return data_film_work, result_persons_genres_in_film
