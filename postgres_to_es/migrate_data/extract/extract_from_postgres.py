import logging
import time
from contextlib import closing
from functools import wraps

import psycopg
from extract.dataclass_table import TABLE_DATACLASSES
from extract.extract_film_work_data import extract_film_work_data
from extract.extract_films_with_person_genre import extract_films_with_person_genre
from extract.extract_person_data import extract_persons_genres_data
from settings.config import DB_POSTGRESQL, TABLE_NAMES
from settings.etl_states import State


def backoff(start_sleep_time: int, factor: int, border_sleep_time: int):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ** n), если t < border_sleep_time
        t = border_sleep_time, иначе

    Args:
        start_sleep_time: начальное время ожидания
        factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
        border_sleep_time: максимальное время ожидания
    Return:
        Результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            log = logging.getLogger("debug_error.log")
            start_sleep_time_local = start_sleep_time

            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (psycopg.OperationalError,) as e:

                    log.error(f"{e}. Retrying in {start_sleep_time_local} seconds...")

                    time.sleep(start_sleep_time_local)
                    n += 1

                    start_sleep_time_local = min(start_sleep_time_local * (factor**n), border_sleep_time)

        return inner

    return func_wrapper


@backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
def extract_from_postgres(lines_in_response: int, file_with_states: State):
    """Функция получения данных из БД.
    Представляет собой конечный автомат, в который включено:
        1) Подключение к БД с использованием техники backoff.
        
        2) Выгрузка всех полных данных о фильмах. Пока все фильмы не будут выгружены из БД,
            выгрузка данных по персоналиям или по жанрам не начнётся.
        
        3) После выгрузки данных о фильмах. начинается выгрузка данных по персоналиям и жанрам.
    
    Движение всего конечного автомата происходит в строгом порядке с константой TABLE_NAMES. 
    Менять порядок таблиц в TABLE_NAMES нельзя!

    Args:
        lines_in_response (int): Число строк, получаемое из БД.
        file_with_states (State): Файл хранения состояний.

    Returns:
        data_film_work (tuple): Кортеж с полными данными фильмов.
        result_persons_genres_in_film (dict): Словарь с данными по персоналиям или с данными по жанрам.
    """
    try:

        with closing(
            psycopg.connect(
                **DB_POSTGRESQL,
                row_factory=psycopg.rows.dict_row,
                cursor_factory=psycopg.ClientCursor,
            )
        ) as conn_postgre:

            result_persons_genres_in_film = {}
            for i, table_name in enumerate(TABLE_NAMES):

                extract_states = file_with_states.get_state("extract")

                table_dataclass = TABLE_DATACLASSES[table_name]

                # значение, которое говорит нам, закончили ли мы выгружать все полные данные о фильмах или нет
                all_films_extract = file_with_states.get_state("extract").get("films_all_extract", None)
                
                # значение, которое говорит нам, закончили ли мы выгружать таблицы по персоналиям/жанрам с привязкой к фильмам
                person_genre_film_work_all_extract = file_with_states.get_state("extract").get(
                    "person_genre_film_work_all_extract", None
                )

                # условие для выгрузки информации о фильмах
                # таблица в БД о фильмах идёт первой в конечном автомате
                # данные о персоналиях или жанрах полностью выгружены, или ещё не начинали выгружаться (первое включение)
                if (i == 0) and (
                    (person_genre_film_work_all_extract) is True or (person_genre_film_work_all_extract is None)
                ):
                    # время обновления последнего выгруженного из БД фильма, 
                    # является значением по которому происходит сортировка фильмов                   
                    last_updated_time_film_work = extract_states.get(f"{table_name}_updated_at", None)

                    # last_extract_updated_at - время последнего извлеченного значения, 
                    # которое будет занесено в файл хранения состояния. 
                    # data_film_work - данные о фильмах из БД, которые будем заносить в Elasticsearch 
                    last_extract_updated_at, data_film_work = extract_film_work_data(
                        last_updated_time_film_work, lines_in_response, conn_postgre, table_dataclass, table_name
                    )

                    # Последнее извлеченное время может быть равно None в том случае, 
                    # если не проводилось новых обновлений в таблице фильмов в БД,
                    # соответственно нет последнего извлеченного времени, значит данные все актуальные.
                    if last_extract_updated_at is not None:
                        extract_states.update(
                            {
                                f"{table_name}_updated_at": last_extract_updated_at,
                                "films_all_extract": False,
                            }
                        )
                    else:
                        extract_states.update({"films_all_extract": True})

                    file_with_states.set_state("extract", extract_states)

                    # в этой функции мы всегда возвращаем 2-е переменные, и всегда одна из них пуста.
                    result_persons_genres_in_film = {}

                # значение, которое говорит нам, закончили ли мы выгружать все полные данные о фильмах или нет
                all_films_extract = file_with_states.get_state("extract").get("films_all_extract", None)
                
                # значение, которое говорит нам, закончили ли мы выгружать таблицы по персоналиям/жанрам с привязкой к фильмам
                person_genre_film_work_all_extract = file_with_states.get_state("extract").get(
                    "person_genre_film_work_all_extract", None
                )

                # условие для выгрузки информации о персонах/жанрах, но без привязки к конкретным фильмам
                # выполняется только если таблицы person/genre
                # проверяется выгружены ли все фильмы 
                # данные о персоналиях или жанрах полностью выгружены, или ещё не начинали выгружаться (первое включение)
                if (
                    (i == 1 or i == 3)
                    and (all_films_extract is True)
                    and ((person_genre_film_work_all_extract is True) or (person_genre_film_work_all_extract is None))
                ):
                    # время обновления последнего выгруженного из БД персоналия/жанра, 
                    # является значением по которому происходит сортировка персоналиев/жанров
                    last_updated_time_person = extract_states.get(f"{table_name}_updated_at", None)

                    # last_extract_updated_at - время последнего извлеченного значения, 
                    # которое будет занесено в файл хранения состояния. 
                    # data_person_or_genre - данные о персоналиях/жанрах из БД, без привязки к фильмам.
                    last_extract_updated_at, data_person_or_genre = extract_persons_genres_data(
                        last_updated_time_person, lines_in_response, conn_postgre, table_dataclass, table_name
                    )
                    
                    # твист в том, что название переменной в файле хранения, 
                    # включает в себя название текущей таблицы - person/genre
                    if last_extract_updated_at is not None:
                        extract_states.update({f"{table_name}_updated_at": last_extract_updated_at})

                    # мы заносим выгруженные данные о персоналиях/жанрах в файл хранения состояния,
                    # это нужно для выгрузки данных по персоналиям/жанрам с привязкой к фильмам, 
                    # то есть для таблиц person_film_work/genre_film_work
                    # когда мы будем обрабатывать таблицы person_film_work/genre_film_work, данные о персоналиях пропадут,
                    # тк обработка таблиц, где указана привязка персон/жанров к фильмам происходит на i + 1 итерации относительно текущей
                    extract_states.update({"person_or_genre_data": {table_dataclass.__name__: data_person_or_genre}})

                    file_with_states.set_state("extract", extract_states)

                # значение, которое говорит нам, закончили ли мы выгружать все полные данные о фильмах или нет
                all_films_extract = file_with_states.get_state("extract").get("films_all_extract", None)
                
                # значение, которое говорит нам, закончили ли мы выгружать таблицы по персоналиям/жанрам с привязкой к фильмам
                person_genre_film_work_all_extract = file_with_states.get_state("extract").get(
                    "person_genre_film_work_all_extract", None
                )
                
                # получаем ранее сохраненные данные о персоналиях/жанрах без привязки к фильму
                data_person_or_genre = file_with_states.get_state("extract").get("person_or_genre_data", None)

                # условие для выгрузки информации о персонах/жанрах с привязкой к конкретным фильмам
                # выполняется только если таблицы person_film_work/genre_film_work
                # проверяется выгружены ли все фильмы 
                # данные о персоналиях/жанрах полностью выгружены, или ещё не начинали выгружаться (первое включение), 
                # или данные еще в процессе выгрузки для конкретной персоны/жанра, так как выгрузка идет с применением limit/offset
                if (
                    (i == 2 or i == 4)
                    and (all_films_extract is True)
                    and (
                        (person_genre_film_work_all_extract is True)
                        or (person_genre_film_work_all_extract is None)
                        or (person_genre_film_work_all_extract == table_dataclass.__name__)
                    )
                ):
                    # получаем текущий offset
                    offset = extract_states.get(f"{table_name}_offset", None)

                    if offset is None:
                        raise Exception(f"Смещение для таблицы {table_name} не найдено! Проверьте состояния!")

                    # offset - получаем новое значение смещения, которое будет использовать при следующей итерации в этой функции
                    # persons_genres_in_film - получаем данные о персоналиях/жанрах с привязкой к фильмам
                    # person_genre_film_work_all_extract_update - получаем значение для записи в файл хранения состояния.
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

                    extract_states.update(
                        {
                            f"{table_name}_offset": offset,
                            "person_genre_film_work_all_extract": person_genre_film_work_all_extract_update,
                        }
                    )

                    file_with_states.set_state("extract", extract_states)

                    if isinstance(person_genre_film_work_all_extract, bool | None):
                        key_name = table_dataclass.__name__
                    else:
                        key_name = person_genre_film_work_all_extract

                    result_persons_genres_in_film[key_name] = tuple(persons_genres_in_film)

                    data_film_work = tuple()

            return data_film_work, result_persons_genres_in_film

    except Exception:
        raise
