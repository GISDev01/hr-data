from oura import OuraClient
import json
import os
from datetime import datetime

import logging
import os.path
import sys

from logging import handlers

import yaml

# config.yml should exist in the same directory as this file
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

oura.user_info()
oura.sleep_summary(start='2021-11-01', end='2021-11-30')
oura.activity_summary(start='2021-11-20')
