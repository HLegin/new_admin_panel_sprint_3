import logging

from extract.extract_from_postgres import extract_from_postgres
from load.load_data_to_elastic import connect_to_es_and_load_data
from settings.config import PATH_ETL_STATE_JSON, config_log
from settings.etl_states import JsonFileStorage, State
from transform.transform_data import transform_data


def main(lines_in_response: int):
    try:
        log = logging.getLogger("debug_error.log")

        file_with_states = State(JsonFileStorage(PATH_ETL_STATE_JSON))

        while True:
            data_from_postrgre, data_person_or_genre = extract_from_postgres(lines_in_response, file_with_states)

            data_to_elastic = transform_data(data_from_postrgre, data_person_or_genre)

            if len(data_to_elastic) != 0:
                connect_to_es_and_load_data(data_to_elastic, file_with_states)

    except Exception as error:
        log.exception(error)


if __name__ == "__main__":
    config_log()

    lines_in_response = 50

    main(lines_in_response)
