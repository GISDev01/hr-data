from oura import OuraClient
import json
import os
import datetime
import dateutil.parser
from pytz import timezone

import sys
import logging
from logging import handlers

import yaml

from psycopg2 import sql, connect

if not os.path.isfile(os.path.join('config', 'config.yml')):
    print('config.yml was not found. You probably need to rename the config.yml.template to config.yml ' +
          'and insert your credentials in this config file')
    sys.exit()


def get_logger():
    logs_dir_name = 'log'
    if not os.path.exists(logs_dir_name):
        os.makedirs(logs_dir_name)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    LOG_FORMAT = logging.Formatter('%(asctime)-15s %(levelname)s: %(message)s')

    stdout_logger = logging.StreamHandler(sys.stdout)
    stdout_logger.setFormatter(LOG_FORMAT)
    logger.addHandler(stdout_logger)

    file_logger = handlers.RotatingFileHandler(os.path.join(logs_dir_name, 'hr-data.log'),
                                               maxBytes=(1048576 * 5),
                                               backupCount=3)
    file_logger.setFormatter(LOG_FORMAT)
    logger.addHandler(file_logger)
    return logger


def get_config():
    try:
        with open(os.path.join("config", "config.yml"), "r") as yaml_config_file:
            _config = yaml.load(yaml_config_file, Loader=yaml.SafeLoader)
        return _config
    except:
        logger.exception('config.yml file cannot be found or read. '
                         'You might need to fill in the the config.yml.template and then rename it to config.yml')


logger = get_logger()
config = get_config()

oura = OuraClient(personal_access_token=config['oura_token'])

OURA_ACTIVITY_TABLE_NAME = "oura_activity"
OURA_READINESS_TABLE_NAME = "oura_readiness"
OURA_SLEEP_TABLE_NAME = "oura_sleep"

# Connect once at the beginning of running the script
PG_CONN = connect(
    user=config['DB_USER'],
    password=config['DB_PASSWORD'],
    host=config['DB_HOST'],
    port=config['DB_PORT'],
    database=config['DB_DATABASE'])


def get_activity():
    activity_list = oura.activity_summary(start='2021-11-28', end='2021-11-30')["activity"]
    for activity in update_activity_data(activity_list):
        logger.info(json.dumps(activity,
                               sort_keys=True, indent=4))


def get_oura_sleep_and_store_in_pg():
    sleep_list = oura.sleep_summary()["sleep"]
    for sleep in update_sleep_data(sleep_list):
        write_row_to_pg(OURA_SLEEP_TABLE_NAME, sleep)


def get_readiness():
    readiness_list = oura.readiness_summary(start='2021-11-28', end='2021-11-30')["readiness"]
    for readiness in readiness_list:
        logger.info(json.dumps(readiness,
                               sort_keys=True, indent=4))


def update_sleep_data(sleep_data):
    for row in sleep_data:
        row['summary_date'] = dateutil.parser.parse(row['summary_date']).date()
        row['bedtime_start'] = dateutil.parser.parse(row['bedtime_start']).astimezone(timezone(config["TIMEZONE"]))
        row['bedtime_end'] = dateutil.parser.parse(row['bedtime_end']).astimezone(timezone(config["TIMEZONE"]))

        # map the hypnogram data to a more readable format
        # 'D' = deep sleep
        # 'L' = light sleep
        # 'R' = REM sleep
        # 'A' = awake
        row['hypnogram_5min'] = ['DLRA'[int(c) - 1] for c in row['hypnogram_5min']]
        row['is_longest'] = bool(row['is_longest'])

    return sleep_data


def update_activity_data(activity_data):
    for row in activity_data:
        row['day_start'] = dateutil.parser.parse(row['day_start'])
        row['day_end'] = dateutil.parser.parse(row['day_end'])
        row['summary_date'] = dateutil.parser.parse(row['summary_date']).date()
        row['class_5min'] = list(map(int, row['class_5min']))

    return activity_data


def get_yesterday_iso_date():
    yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
    return yesterday.isoformat()


def write_row_to_pg(table, row):
    cursor = PG_CONN.cursor()

    # TODO: Replace with ORM
    keys = sorted(row.keys())
    values = [row[k] for k in keys]
    key_count = len(keys)
    keys_template = ', '.join(['{}' for _ in range(key_count)])
    vals_template = ', '.join(['%s' for _ in range(key_count)])
    sql_ids = [sql.Identifier(table)] + [sql.Identifier(k) for k in keys]
    raw_sql = sql.SQL(f'INSERT INTO {{}} ({keys_template}) VALUES ({vals_template})').format(*sql_ids)

    try:
        cursor.execute(raw_sql, values)
        PG_CONN.commit()
    except Exception as e:
        logger.exception("Unable to insert row into PG: ", e)
        sys.exit(1)


if __name__ == "__main__":
    get_oura_sleep_and_store_in_pg()
