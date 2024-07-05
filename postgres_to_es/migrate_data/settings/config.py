import os
from logging.config import dictConfig

LVL_LOGS = os.environ.get("LVL_LOGS", "WARNING")

SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
TABLE_NAMES = 'film_work,person,person_film_work,genre,genre_film_work'.split(",")

DB_POSTGRESQL = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "options": "-c search_path={}".format(SCHEMA_NAME),
}

PATH_ETL_STATE_JSON = os.path.join(os.path.dirname(os.path.dirname(__file__)), "etl_state_file", "etl_state.json")


def config_log():
    path_to_errors_log = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "debug_error.log")
    list_paths_files_create = [path_to_errors_log]

    for path in list_paths_files_create:
        if (os.path.exists(path) and os.path.isfile(path)) is False:
            path_to_errors_log_dirname = os.path.dirname(path)

            if os.path.exists(path_to_errors_log_dirname) is False:
                os.makedirs(path_to_errors_log_dirname)

            with open(path, "a") as file:
                file.write("\n")

    dictConfig(
        {
            "version": 1,
            "formatters": {
                "default": {
                    "format": '\n[%(asctime)s] %(levelname)s File "%(pathname)s", line %(lineno)d, in %(funcName)s: %(message)s',
                    "exc_info": True,
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "default",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": path_to_errors_log,
                    "formatter": "default",
                    "level": LVL_LOGS,
                    "encoding": "UTF-8",
                    "maxBytes": (2 * 1024 * 1024),
                    "backupCount": 3,
                },
            },
            "root": {"level": LVL_LOGS, "handlers": ["file"]},
        }
    )
