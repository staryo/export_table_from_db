from argparse import ArgumentParser
from logging import basicConfig, DEBUG, INFO
from os import getcwd
from os.path import join

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from yaml import SafeLoader, load
import sqlalchemy.sql.default_comparator
import psycopg2


def read_config(config_filepath):
    with open(config_filepath, 'r', encoding="utf-8") as f:
        return load(f, Loader=SafeLoader)


def script(db_config, query):
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        db_config['database_login'],
        db_config['database_password'],
        db_config['database_server'],
        db_config['database_port'],
        db_config['database']
    ))

    result = pd.read_sql(
        query,
        engine
    )

    return result


def save_to_pg(db_config, df, name, replace):
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        db_config['database_login'],
        db_config['database_password'],
        db_config['database_server'],
        db_config['database_port'],
        db_config['database']
    ))
    print('Database session created')
    chunksize = 100
    df = pd.DataFrame(df)
    for column in ['date', 'start_date', 'stop_date',
                   'date_from', 'date_to']:
        try:
            df[column] = pd.to_datetime(
                df[column].str[:19] + df[column].str[-6:],
                format='%Y-%m-%dT%H:%M:%S%z')
        except KeyError:
            pass
    with tqdm(total=len(df), desc=name) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            replace = replace if i == 0 else "append"
            cdf.to_sql(
                name,
                engine,
                if_exists=replace,
                index=False,
                method='multi'
            )
            pbar.update(chunksize)


if __name__ == '__main__':

    parser = ArgumentParser(
        description='Инструмент получения данных из БД и '
                    'сохранения куда-то еще'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)
    parser.add_argument('-p', '--params', required=False, default='')

    args = parser.parse_args()

    basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    if args.params:
        if ',' in args.params:
            new_df = script(
                config['db'],
                config['query'].format(*args.params.split(','))
            )
        else:
            new_df = script(
                config['db'],
                config['query'].format(args.params)
            )
    else:
        new_df = script(
            config['db'],
            config['query']
        )

    if 'output_db' in config:
        save_to_pg(
            config['output_db'],
            new_df,
            config['output_db']['table'],
            config['output_db']['replace'],
        )

    try:
        tqdm.write(f"Сохраняем в файл {config['output_file']}.xml")
        new_df.to_xml(f"{config['output_file']}.xml")
    except ValueError:
        tqdm.write('В XML сохранить не получилось -- '
                   'возможно поля на русском языке')

    tqdm.write(f"Сохраняем в файл {config['output_file']}.csv")
    new_df.to_csv(f"{config['output_file']}.csv")
    tqdm.write(f"Сохраняем в файл {config['output_file']}.xlsx")
    new_df.to_excel(f"{config['output_file']}.xlsx")
    tqdm.write(f"Сохраняем в файл {config['output_file']}.json")
    new_df.to_json(f"{config['output_file']}.json")

