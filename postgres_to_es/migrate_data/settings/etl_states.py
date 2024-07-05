import abc
import os
from typing import Any, Dict

import orjson

# class BaseStorage(abc.ABC):
#     """Абстрактное хранилище состояния.

#     Позволяет сохранять и получать состояние.
#     Способ хранения состояния может варьироваться в зависимости
#     от итоговой реализации. Например, можно хранить информацию
#     в базе данных или в распределённом файловом хранилище.
#     """

#     @abc.abstractmethod
#     def save_state(self, state: Dict[str, Any]) -> None:
#         """Сохранить состояние в хранилище."""

#         dump_data_state = json.dumps(state)

#         with open(self.file_path, "w") as file:
#             file.write(dump_data_state)

#     @abc.abstractmethod
#     def retrieve_state(self) -> Dict[str, Any]:
#         """Получить состояние из хранилища."""

#         with open(self.file_path, "r") as file:
#             read_data_state = file.read()

#         load_data_sate = json.loads(read_data_state)

#         if isinstance(load_data_sate, dict):
#             return load_data_sate
#         else:
#             return {}


# class JsonFileStorage(BaseStorage):
class JsonFileStorage:
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

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
        """Сохранить состояние в хранилище."""

        try:
            dump_data_state = orjson.dumps(state, option=orjson.OPT_INDENT_2)
        except Exception:
            raise

        with open(self.file_path, "wb") as file:
            file.write(dump_data_state)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""

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
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""

        saved_states = self.storage.retrieve_state()

        new_state_dict = saved_states | {key: value}

        self.storage.save_state(new_state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""

        saved_states = self.storage.retrieve_state()

        value_from_key = saved_states.get(key, None)

        return value_from_key
