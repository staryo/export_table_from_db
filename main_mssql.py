import json
from argparse import ArgumentParser
from logging import basicConfig, DEBUG, INFO
from os import getcwd
from os.path import join
from urllib import parse

import pandas as pd
from sqlalchemy import create_engine
from yaml import SafeLoader, load
import pyodbc


def read_config(config_filepath):
    with open(config_filepath, 'r', encoding="utf-8") as f:
        return load(f, Loader=SafeLoader)


def script(db_config, query):
    engine = create_engine(
        'mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(
            db_config['database_login'],
            parse.quote_plus(db_config['database_password']),
            db_config['database_server'],
            db_config['database']
        ))

    result = pd.read_sql(
        query,
        engine
    )

    return result


if __name__ == '__main__':

    parser = ArgumentParser(
        description='Инструмент консольной выгрузки таблиц из MSSSQL.'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)

    args = parser.parse_args()

    basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    result = script(config['db'], config['query'])

    result.to_csv('result.csv')
    result.to_excel('result.xlsx')
    result.to_json('result.json', orient='records', date_format='iso')
    if 'key' in config:
        with open('result_key.json', 'w') as f:
            json.dump(result.to_dict(orient='list')[config['key']], f)
