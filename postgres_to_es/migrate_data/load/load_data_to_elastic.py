import logging
import os
from contextlib import closing
from typing import Dict, Tuple

import elasticsearch
import orjson
from settings.etl_states import State
from tenacity import retry, stop_never, wait_random_exponential

INDEX_NAME = "movies"


@retry(wait=wait_random_exponential(min=1, max=30), stop=stop_never, reraise=True)
def connect_to_es_and_load_data(data_to_elastic: Tuple[Dict], file_with_states: State):

    try:
        log = logging.getLogger("debug_error.log")

        with closing(elasticsearch.Elasticsearch([{"host": "elasticsearch", "port": 9200, "scheme": "http"}])) as es:

            # Проверка готовности
            cluster_health = es.cluster.health()

            if (cluster_health["status"] == "green") or (cluster_health["status"] == "yellow"):
                load_data(data_to_elastic, file_with_states, es)
            elif cluster_health["status"] == "red":
                raise elasticsearch.ConnectionError(
                    f"Cluster is not healthy, status red, {(orjson.dumps(dict(cluster_health), option=orjson.OPT_INDENT_2)).decode('utf-8')}"
                )
            else:
                raise Exception(
                    f"Cluster is not healthy, {(orjson.dumps(dict(cluster_health), option=orjson.OPT_INDENT_2)).decode('utf-8')}"
                )

    except elasticsearch.ConnectionError as e:
        log.exception(f"Error connecting to Elasticsearch: {e}")
        raise
    except Exception as e:
        log.exception(f"\nADDITIONAL ERROR: {e}")
        raise


def load_data(data_to_elastic: Tuple[Dict], file_with_states: State, es: elasticsearch.Elasticsearch):

    with open(os.path.join(os.path.dirname(__file__), "es_schema.json"), "rb") as file:
        data_index = orjson.loads(file.read())

    if not (es.indices.exists(index=INDEX_NAME)):
        es.indices.create(index=INDEX_NAME, body=data_index)

    try:
        person_or_genre_data = tuple(
            file_with_states.get_state("extract").get("person_or_genre_data", None).values()
        )[0]
    except AttributeError:
        person_or_genre_data = None

    data_to_insert_persons = []
    data_to_insert_genres = []
    bulk_data = []
    for item in data_to_elastic:
        
        if tuple(item.keys())[0] == "person_film_work":

            for key, data in item.items():
                person_id = str(data["person_id"])
                film_work_id = str(data["film_work_id"])

                for check_data in person_or_genre_data:
                    check_id = str(check_data["id"])

                    if check_id == person_id:
                        full_name = check_data["full_name"]

                        data_to_insert_persons.append((film_work_id, person_id, full_name))
                        break
                    
        elif tuple(item.keys())[0] == "genre_film_work":
            for key, data in item.items():
                genre_id = str(data["genre_id"])
                film_work_id = str(data["film_work_id"])

                for check_data in person_or_genre_data:
                    check_id = str(check_data["id"])

                    if check_id == genre_id:
                        genre_name = check_data["name"]

                        data_to_insert_genres.append((film_work_id, genre_id, genre_name))
                        break
        else:
            bulk_data.append({"index": {"_index": INDEX_NAME, "_id": item["id"]}})
            bulk_data.append(item)

    if len(data_to_insert_persons) != 0:

        response = es.mget(index=INDEX_NAME, body={"ids": [data[0] for data in data_to_insert_persons]})["docs"]

        for film in response:
            if film["found"] is True:
                film_data = film["_source"]

                fields = ["actors", "actors_names", "writers", "writers_names", "directors", "directors_names"]
                data_fields = list(map(lambda field: film_data.get(field, None), fields))

                for new_data in data_to_insert_persons:
                    person_id = new_data[1]
                    new_full_name = new_data[2]

                    for i, old_data in enumerate(data_fields):

                        if i == 0 or i == 2 or i == 4:
                            n = False
                            for c, data_to_change in enumerate(old_data):
                                id_old = data_to_change["id"]

                                if person_id == id_old:
                                    old_full_name = data_to_change["name"]
                                    data_to_change.update({"name": new_full_name})
                                    old_data.pop(c)
                                    old_data.insert(c, data_to_change)
                                    n = True

                            if n is False:
                                old_full_name = None

                            data_fields.pop(i)
                            data_fields.insert(i, old_data)

                        elif i == 1 or i == 3 or i == 5:
                            if old_full_name is not None:
                                old_full_name_indices = [
                                    index for index, value in enumerate(old_data) if value == old_full_name
                                ]

                                for temp_index in old_full_name_indices:
                                    old_data.pop(temp_index)
                                    old_data.insert(temp_index, new_full_name)

                            data_fields.pop(i)
                            data_fields.insert(i, old_data)

                new_film_data = dict(zip(fields, data_fields))

                film_data.update(new_film_data)

                bulk_data.append({"update": {"_id": film_data["id"], "_index": INDEX_NAME}})
                bulk_data.append({"doc": film_data})
    elif len(data_to_insert_genres) != 0:

        response = es.mget(index=INDEX_NAME, body={"ids": [data[0] for data in data_to_insert_genres]})["docs"]

        for film in response:
            if film["found"] is True:
                film_data = film["_source"]

                fields = ["genres"]
                data_fields = list(map(lambda field: film_data.get(field, None), fields))
                data_fields = data_fields[0]

                for new_data in data_to_insert_genres:
                    new_genre = new_data[2]

                    if new_genre not in data_fields:
                        data_fields.append(new_genre)

                film_data.update({"genres": data_fields})

                bulk_data.append({"update": {"_id": film_data["id"], "_index": INDEX_NAME}})
                bulk_data.append({"doc": film_data})

    if len(bulk_data) != 0:
        answer = es.bulk(body=bulk_data)
        if answer.get("errors", None) is not None:
            if answer.get("errors", None) is True:
                raise Exception(
                    f"BULK операция прошла с ошибкой!\nОбратная связь: {(orjson.dumps(dict(answer), option=orjson.OPT_INDENT_2)).decode('utf-8')}"
                )
        else:
            raise Exception(f"Ответ после BULK операции имеет тип None!")

        es.indices.flush(index=INDEX_NAME)
