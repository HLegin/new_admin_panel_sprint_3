import logging
from contextlib import closing

import elasticsearch
import psycopg
from extract.extract_from_postgres import extract_from_postgres
from load.insert_data_to_elastic import load_data_index_movies, load_data_index_persons_genres
from settings.config import DB_POSTGRESQL, ELASTIC, config_log
from settings.etl_states import RedisState
from tenacity import retry, stop_never, wait_exponential
from transform.transform_data import transform_data


@retry(wait=wait_exponential(min=5, max=30), stop=stop_never, reraise=False)
def main(lines_in_response: int):
    try:

        log = logging.getLogger("root")

        with closing(
            psycopg.connect(**DB_POSTGRESQL, row_factory=psycopg.rows.dict_row, cursor_factory=psycopg.ClientCursor)
        ) as conn_postgres:
            with closing(elasticsearch.Elasticsearch(ELASTIC)) as conn_elastisearch:

                if not (conn_elastisearch.ping()):
                    raise Exception("Error connecting to Elasticsearch!")

                redis_state = RedisState()

                while True:
                    data_from_postrgre, data_person_or_genre = extract_from_postgres(
                        lines_in_response, redis_state, conn_postgres
                    )

                    data_to_elastic = transform_data(data_from_postrgre, data_person_or_genre)

                    if len(data_to_elastic) != 0:
                        load_data_index_movies(data_to_elastic, redis_state, conn_elastisearch)

                        for i in range(2):
                            load_data_index_persons_genres(i, data_to_elastic, conn_elastisearch)
    except (psycopg.OperationalError, Exception) as error:
        if isinstance(error, psycopg.OperationalError):
            log.exception(f"\nError connecting to Postgres DB: {error}")
        else:
            log.exception(f"\nGeneral error: {error}")

        raise


if __name__ == "__main__":
    config_log()

    lines_in_response = 100

    main(lines_in_response)
