import mysql.connector
from os import getenv as env
import urllib.parse as urlparse

import logging
from common.common import setup_logging
logger = logging.getLogger()

OPENCAST_DB_URL = env("OPENCAST_DB_URL");
OPENCAST_RUNNING_JOB_STATUS = 2


def parse_db_url():
    db_url = urlparse.urlparse(OPENCAST_DB_URL);
    return {
        "host": db_url.hostname,
        "port": db_url.port,
        "user": db_url.username,
        "password": db_url.password,
        "database": db_url.path[1:]
    }


@setup_logging
def handler(event, context):

    query = """
        SELECT
            operation, count(*) as cnt
        FROM oc_job
        WHERE
            status = {}
        GROUP BY
            operation
    """.format(OPENCAST_RUNNING_JOB_STATUS)

    try:
        cnx_params = parse_db_url()
        cnx = mysql.connector.connect(**cnx_params)
        cnx.raise_on_warnings = True
        cursor = cnx.cursor()
        cursor.execute(query)
        res = dict(cursor.fetchall())
        return res
    except mysql.connector.Error as e:
        logger.exception("Error communicating with the db: {}".format(str(e)))
    except Exception as e:
        logger.exception("Error establishing connection to the db: {}".format(str(e)))
    finally:
        logger.info({ "operation_counts": res})
        cursor.close()
        cnx.close()



