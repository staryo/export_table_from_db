from argparse import ArgumentParser
from logging import basicConfig, DEBUG, INFO
from os import getcwd
from os.path import join

import pandas as pd
import urllib3
from pandas import DataFrame
from sqlalchemy import create_engine
from tqdm import tqdm
from yaml import SafeLoader, load
import sqlalchemy.sql.default_comparator
import psycopg2

from ia_api.iaimportexport import IAImportExport


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

    tqdm.write('Отправляем запрос')
    result = pd.read_sql(
        query,
        engine
    )
    tqdm.write('Получили ответ')

    return result


if __name__ == '__main__':
    parser = ArgumentParser(
        description='Инструмент консольного импорта данных в систему IA.'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)

    args = parser.parse_args()

    basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    result = script(config['db'], config['query'])

    with IAImportExport.from_config(config['IA']) as ia:
        urllib3.disable_warnings()
        final_result = []
        # new_df = DataFrame()
        for row in tqdm(result.iterrows(), desc='Разбор НЗП'):
            if 'VPSK' in row[1]['operation_id']:
                row[1]['operation_id'] = ''
                row[1]['operation_progress'] = 100
                row[1]['batch_id'] = f"{row[1]['batch_id']}_done"
            if row[1]['#operation_name'] == 'ERP_FINISHED':
                row[1]['operation_id'] = ia.get_last_phase_operation(row[1]['#route_phase'])
                row[1]['operation_progress'] = 100
                try:
                    if row[1]['#route_phase'] == ia.get_entity_first_phase(
                            ia.get_entity_id(row[1]['code'])
                    ):
                        row[1]['PROVIDED'] = 1
                        # row[1]['PROVIDED'] = 0
                except KeyError:
                    pass
            if row[1]['#operation_name'] == 'STOCK':
                row[1]['operation_id'] = ia.get_first_phase_operation(row[1]['#route_phase'])
                try:
                    if row[1]['#route_phase'] == ia.get_entity_first_phase(
                            ia.get_entity_id(row[1]['code'])
                    ):
                        row[1]['PROVIDED'] = 1
                        # row[1]['PROVIDED'] = 0
                except KeyError:
                    pass

            if row[1]['amount'] > 0:
                final_result.append(row[1])
        final_result = [
            {k.upper(): v for k, v in row.items()}
            for row in final_result
        ]
        new_df = DataFrame(final_result)

    tqdm.write(f"Сохраняем в файл {config['output_file']}.csv")
    new_df.to_csv(f"{config['output_file']}.csv")
    tqdm.write(f"Сохраняем в файл {config['output_file']}.xlsx")
    new_df.to_excel(f"{config['output_file']}.xlsx")
    tqdm.write(f"Сохраняем в файл {config['output_file']}.json")
    new_df.to_json(f"{config['output_file']}.json")
    tqdm.write(f"Сохраняем в файл {config['output_file']}.xml")
    new_df.to_xml(f"{config['output_file']}.json")
