import logging
from contextlib import closing
from typing import Union

import redis
import redis.exceptions
from settings.config import REDIS_DATABASE, REDIS_HOST, REDIS_PORT
from tenacity import retry, stop_never, wait_random_exponential


@retry(wait=wait_random_exponential(min=1, max=30), stop=stop_never, reraise=True)
def connect_redis() -> Union[redis.Redis, None]:

    log = logging.getLogger("root")

    try:
        with closing(redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=REDIS_DATABASE)) as redis_obj:
            if redis_obj.ping():
                return redis_obj
        return None
    except (redis.exceptions.ConnectionError, Exception) as error:
        if isinstance(error, redis.exceptions.ConnectionError):
            log.exception(f"\nError connecting to Redis: {error}")
        else:
            log.exception(f"\nGeneral error: {error}")

        raise
