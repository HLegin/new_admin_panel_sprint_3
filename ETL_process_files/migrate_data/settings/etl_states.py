import os
from contextlib import closing
from typing import Any, Dict, Tuple, Union

import orjson
from settings.connect_redis import connect_redis


class RedisState:

    def __init__(self):
        redis_obj = connect_redis()

        name_hash_field = "ETL"
        existing_keys = redis_obj.hgetall(name_hash_field).keys()

        keys_value_default: Dict[str, Union[bool, str, int, None, Dict[str, Tuple[Dict[str, str | None]]]]] = {
            "films_all_extract": None,  # bool | None
            "person_genre_film_work_all_extract": None,  # str | bool | None
            "genre_film_work_offset": 0,  # int
            "person_film_work_offset": 0,  # int
            "film_work_updated_at": None,  # str | None
            "person_updated_at": None,  # str | None
            "genre_updated_at": None,  # str | None
            "person_or_genre_data": None,  # Dict[str, Tuple[Dict[str, str | None]] | None
        }

        with closing(redis_obj.pipeline()) as pipe:

            not_find_key = False
            for key_default in keys_value_default.keys():
                if f"{name_hash_field}:{key_default}" not in existing_keys:
                    pipe.hset(
                        name=name_hash_field, key=key_default, value=orjson.dumps(keys_value_default[key_default])
                    )
                    not_find_key = True

            if not_find_key:
                pipe.execute()

        self.redis_obj = redis_obj
        self.name_hash_field = name_hash_field
        self.existing_keys = tuple(str(item, encoding="utf-8") for item in redis_obj.hgetall(name_hash_field).keys())

    def save_state(self, state_to_save: Dict[str, Any]) -> None:
        for key in state_to_save.keys():
            if key not in self.existing_keys:
                raise Exception(f"Attempt to load value rejected because the key {key} is missing in Redis.")

        self.redis_obj.hset(
            self.name_hash_field,
            key=tuple(state_to_save.keys())[0],
            value=orjson.dumps(tuple(state_to_save.values())[0]),
        )

    def get_state(self, keys_to_read: list | tuple) -> tuple:

        data = self.redis_obj.hmget(name=self.name_hash_field, keys=keys_to_read)

        return tuple(orjson.loads(item) for item in data)


class JsonFileStorage:

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

        try:
            os.makedirs(os.path.dirname(file_path))
        except Exception:
            pass

        if os.path.exists(file_path) is False:
            with open(file_path, "wb") as file:
                file.write(
                    orjson.dumps(
                        {
                            "extract": {
                                "films_all_extract": None,
                                "person_genre_film_work_all_extract": None,
                                "genre_film_work_offset": 0,
                                "person_film_work_offset": 0,
                                "film_work_updated_at": None,
                                "person_updated_at": None,
                                "genre_updated_at": None,
                                "person_or_genre_data": None,
                            }
                        },
                        option=orjson.OPT_INDENT_2,
                    )
                )

    def save_state(self, state: Dict[str, Any]) -> None:

        try:
            dump_data_state = orjson.dumps(state, option=orjson.OPT_INDENT_2)
        except Exception:
            raise

        with open(self.file_path, "wb") as file:
            file.write(dump_data_state)

    def retrieve_state(self) -> Dict[str, Any]:

        with open(self.file_path, "rb") as file:
            read_data_state = file.read()

        try:
            load_data_sate = orjson.loads(read_data_state)
        except Exception:
            raise

        if isinstance(load_data_sate, dict):
            return load_data_sate
        else:
            return {}


class State:

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:

        saved_states = self.storage.retrieve_state()

        new_state_dict = saved_states | {key: value}

        self.storage.save_state(new_state_dict)

    def get_state(self, key: str) -> Any:

        saved_states = self.storage.retrieve_state()

        value_from_key = saved_states.get(key, None)

        return value_from_key
