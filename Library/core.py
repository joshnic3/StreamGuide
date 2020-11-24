import argparse
import csv
import datetime
import logging
import os
import sqlite3
from hashlib import md5

import yaml

from Library import constants


def write_rows_to_csv(csv_file_path, rows, headers=None):
    rows_to_write = [headers] + rows if headers else rows
    with open(csv_file_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        for row in rows_to_write:
            writer.writerow(row)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configs', type=str, required=True)
    arguments = parser.parse_args()
    return arguments


class Logger:

    @staticmethod
    def new(log_file_path, production=True, debug=False):
        # Setup logging to file.
        logging.root.handlers = []
        log_format = '%(asctime)s|%(levelname)s : %(message)s'
        if debug:
            logging.basicConfig(level='DEBUG', format=log_format, filename=log_file_path)
        else:
            logging.basicConfig(level='INFO', format=log_format, filename=log_file_path)
        log = logging.getLogger('')

        # Setup logging to console.
        if not production:
            console = logging.StreamHandler()
            formatter = logging.Formatter(log_format)
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
        return log

    @staticmethod
    def generate_log_path(sc, script_name):
        date_string = sc.get_runtime(as_string=True)[:-6]
        time_string = sc.get_runtime(as_string=True)[8:]
        log_file_name = '{}_{}_{}.log'.format(script_name[:-3], date_string, time_string)
        return os.path.join(sc.paths.get('log'), log_file_name)


class ScriptConfiguration:

    def __init__(self, config_file_path=None):
        self._runtime = datetime.datetime.now()
        self.environment = None
        self.paths = {}
        self.params = {}

        # Read configs from YAML file if provided.
        if config_file_path:
            self._read_yaml_config_file(config_file_path)

    def _read_yaml_config_file(self, config_file_path):
        with open(config_file_path) as yaml_file:
            global_configs = yaml.load(yaml_file, Loader=yaml.FullLoader)
            self.environment = global_configs.get('environment', self.environment)
            self.paths = global_configs.get('paths', self.paths)
            self.params = global_configs.get('params', self.params)

    def pp_params(self, script_name=None):
        template = '"{}" started with parameters: {}'
        default = {'environment': self.environment}
        params = {**default, **self.params}
        params_as_string = ', '.join(['{}: {}'.format(p, params.get(p)) for p in params])
        return template.format(script_name if script_name else 'Script', params_as_string)

    def is_production(self):
        return True if self.environment == 'production' else False

    def get_runtime(self, as_string=False):
        if as_string:
            return self._runtime.strftime(constants.DATETIME.FORMAT)
        return self._runtime


class Database:
    ALL_CHAR = '*'
    SEPARATOR_CHAR = ','
    CONDITION_TEMPLATE = ' WHERE {}'
    QUERY_TEMPLATES = {
        'select': 'SELECT {} FROM {}{};',
        'select_distinct': 'SELECT DISTINCT {} FROM {}{};',
        'insert': 'INSERT INTO {} VALUES ({});',
        'create': 'CREATE TABLE {} ({});',
        'update': 'UPDATE {} SET {}{};',
        'delete': 'DELETE FROM {}{}'
    }

    def __init__(self, database_file_path):
        self.file_path = database_file_path

    @staticmethod
    def _generate_condition(condition):
        return Database.CONDITION_TEMPLATE.format(condition)

    @staticmethod
    def _clean_string(dirty_string):
        if dirty_string is None:
            dirty_string = ''
        else:
            dirty_string = str(dirty_string)
        clean_string = dirty_string.replace('"', "'")
        return clean_string

    @staticmethod
    def unique_id(salt=None):
        to_hash = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        to_hash = to_hash + str(salt) if salt is not None else to_hash
        hash_obj = md5(to_hash.encode())
        return hash_obj.hexdigest()

    def create_table(self, name, schema):
        data_type = 'TEXT'
        columns = ','.join(['{} {}'.format(c, data_type) for c in schema])
        sql = self.QUERY_TEMPLATES.get('create').format(name, columns)
        self.execute_sql(sql_query=sql)

    def execute_sql(self, sql_query=None, sql_query_list=None):
        if sql_query is None and sql_query_list is None:
            raise Exception('execute_sql can only take either sql_query or sql_query_list, not both.')
        with sqlite3.connect(self.file_path) as connection:
            cursor = connection.cursor()
            if sql_query:
                cursor.execute(sql_query)
                return cursor.fetchall()
            else:
                results = []
                for sql_query in sql_query_list:
                    cursor.execute(sql_query)
                    results.append(cursor.fetchall())
                return results

    def select(self, table, columns=None, condition=None, distinct=False, return_sql=False):
        columns_str = self.SEPARATOR_CHAR.join(columns) if columns else Database.ALL_CHAR
        condition_str = self.CONDITION_TEMPLATE.format(condition) if condition else ''
        query_template = self.QUERY_TEMPLATES.get('select_distinct') if distinct else self.QUERY_TEMPLATES.get('select')
        sql = query_template.format(columns_str, table, condition_str)
        return sql if return_sql else self.execute_sql(sql_query=sql)

    def insert(self, table, values, return_sql=False):
        values = values if isinstance(values, list) else [values]
        values_str = self.SEPARATOR_CHAR.join(['"{}"'.format(self._clean_string(v)) for v in values])
        sql = self.QUERY_TEMPLATES.get('insert').format(table, values_str)
        return sql if return_sql else self.execute_sql(sql_query=sql)

    def update(self, table, values_dict, condition, return_sql=False):
        condition_str = self.CONDITION_TEMPLATE.format(condition)
        values_str = ','.join(['{}={}'.format(vm, self._clean_string(values_dict[vm])) for vm in values_dict])
        sql = self.QUERY_TEMPLATES.get('update').format(table, values_str, condition_str)
        return sql if return_sql else self.execute_sql(sql_query=sql)

    def insert_multiple(self, table, rows):
        sql_queries = [self.insert(table, values, return_sql=True) for values in rows]
        self.execute_sql(sql_query_list=sql_queries)

    def delete(self, table, condition, return_sql=False):
        condition_str = self.CONDITION_TEMPLATE.format(condition)
        sql = self.QUERY_TEMPLATES.get('delete').format(table, condition_str)
        return sql if return_sql else self.execute_sql(sql_query=sql)
