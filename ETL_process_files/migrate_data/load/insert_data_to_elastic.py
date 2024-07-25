import os
from typing import Dict, Tuple

import elasticsearch
import orjson
from settings.config import INDEX_NAME_GENRES, INDEX_NAME_MOVIES, INDEX_NAME_PERSONS
from settings.etl_states import RedisState


def load_data_index_persons_genres(
    number_index: int, data_to_elastic: Tuple[Dict], es: elasticsearch.Elasticsearch
):

    if number_index == 0:
        index_for_interaction = INDEX_NAME_PERSONS
    elif number_index == 1:
        index_for_interaction = INDEX_NAME_GENRES
    else:
        raise Exception(f"Получен недопустимый номер для индекса {number_index}")

    with open(os.path.join(os.path.dirname(__file__), "es_schema_persons_genres.json"), "rb") as file:
        data_index = orjson.loads(file.read())

    if not (es.indices.exists(index=index_for_interaction)):
        es.indices.create(index=index_for_interaction, body=data_index)

    identificate_persons_or_genres = lambda x: (
        (True, "full_name")
        if tuple(x.keys())[0] == "person_film_work"
        else (False, "name") if tuple(x.keys())[0] == "genre_film_work" else None
    )
    persons_or_genres = identificate_persons_or_genres(data_to_elastic[0])

    bulk_data = []
    full_data_to_insert = []
    for item in data_to_elastic:
        if persons_or_genres is None:

            if number_index == 0:
                directors_data = item.get("directors", None)
                actors_data = item.get("actors", None)
                writers_data = item.get("writers", None)

                data_for_sort = (directors_data, actors_data, writers_data)
            elif number_index == 1:
                data_for_sort = (tuple(item.get("genres", None)),)

            for data in data_for_sort:
                for id_name in data:
                    if id_name not in full_data_to_insert:
                        full_data_to_insert.append(id_name)

    if len(full_data_to_insert) != 0:
        for insert_data in full_data_to_insert:
            bulk_data.append({"index": {"_index": index_for_interaction, "_id": insert_data.get("id", None)}})
            bulk_data.append(insert_data)

    if len(bulk_data) != 0:
        answer = es.bulk(body=bulk_data)
        if answer.get("errors", None) is not None:
            if answer.get("errors", None) is True:
                raise Exception(
                    f"BULK operation failed!\nFeedback: {(orjson.dumps(dict(answer), option=orjson.OPT_INDENT_2)).decode('utf-8')}"
                )
        else:
            raise Exception(f"The response after the BULK operation is of type None!")

        es.indices.flush(index=index_for_interaction)


def load_data_index_movies(data_to_elastic: Tuple[Dict], redis_state: RedisState, es: elasticsearch.Elasticsearch):

    with open(os.path.join(os.path.dirname(__file__), "es_schema.json"), "rb") as file:
        data_index = orjson.loads(file.read())

    if not (es.indices.exists(index=INDEX_NAME_MOVIES)):
        es.indices.create(index=INDEX_NAME_MOVIES, body=data_index)

    try:
        person_or_genre_data = tuple(redis_state.get_state(("person_or_genre_data",))[0].values())[0]
    except AttributeError:
        person_or_genre_data = None

    identificate_persons_or_genres = lambda x: (
        (True, "full_name")
        if tuple(x.keys())[0] == "person_film_work"
        else (False, "name") if tuple(x.keys())[0] == "genre_film_work" else None
    )
    persons_or_genres = identificate_persons_or_genres(data_to_elastic[0])

    data_to_insert = []
    bulk_data = []
    for item in data_to_elastic:

        if persons_or_genres is None:
            bulk_data.append({"index": {"_index": INDEX_NAME_MOVIES, "_id": item["id"]}})
            bulk_data.append(item)
            continue

        for key, data in item.items():
            film_work_id = str(data["film_work_id"])

            if persons_or_genres[0]:
                data_id = str(data.get("person_id", None))
            else:
                data_id = str(data.get("genre_id", None))

            for check_data in person_or_genre_data:
                check_id = str(check_data["id"])

                if check_id == data_id:
                    name_person_or_genre = check_data[persons_or_genres[1]]
                    data_to_insert.append((film_work_id, data_id, name_person_or_genre))

    if len(data_to_insert) != 0:
        response = es.mget(index=INDEX_NAME_MOVIES, body={"ids": [data[0] for data in data_to_insert]})["docs"]

        if persons_or_genres[0]:
            fields = ["actors", "actors_names", "writers", "writers_names", "directors", "directors_names"]
        else:
            fields = ["genres", "genres_names"]

        for film in response:
            if film["found"] is True:
                film_id_from_es = film.get("_id", None)
                film_data = film.get("_source", None)

                data_fields = list(map(lambda field: film_data.get(field, None), fields))

                for new_data in data_to_insert:
                    data_id = new_data[1]
                    new_full_name_or_name_genre = new_data[2]

                    for i, old_data in enumerate(data_fields):
                        if i == 0 or i == 2 or i == 4:
                            n = False
                            for c, data_to_change in enumerate(old_data):

                                id_old = data_to_change["id"]

                                if data_id == id_old:
                                    old_full_name_or_name_genre = data_to_change["name"]
                                    data_to_change.update({"name": new_full_name_or_name_genre})
                                    old_data.pop(c)
                                    old_data.insert(c, data_to_change)
                                    n = True

                            if n is False:
                                old_full_name_or_name_genre = None

                            data_fields.pop(i)
                            data_fields.insert(i, old_data)

                        elif i == 1 or i == 3 or i == 5:
                            if old_full_name_or_name_genre is not None:
                                old_full_name_indices = [
                                    index
                                    for index, value in enumerate(old_data)
                                    if value == old_full_name_or_name_genre
                                ]

                                for temp_index in old_full_name_indices:
                                    old_data.pop(temp_index)
                                    old_data.insert(temp_index, new_full_name_or_name_genre)

                            data_fields.pop(i)
                            data_fields.insert(i, old_data)

                new_film_data = dict(zip(fields, data_fields))

                film_data.update(new_film_data)

                bulk_data.append({"update": {"_id": film_data.get("id", None), "_index": INDEX_NAME_MOVIES}})
                bulk_data.append({"doc": film_data})

    if len(bulk_data) != 0:
        answer = es.bulk(body=bulk_data)
        if answer.get("errors", None) is not None:
            if answer.get("errors", None) is True:
                raise Exception(
                    f"BULK operation failed!\nFeedback: {(orjson.dumps(dict(answer), option=orjson.OPT_INDENT_2)).decode('utf-8')}"
                )
        else:
            raise Exception(f"The response after the BULK operation is of type None!")

        es.indices.flush(index=INDEX_NAME_MOVIES)
