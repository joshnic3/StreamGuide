import datetime
import os
import shutil
import sqlite3
import sys

from Library import constants
from Library.api import API
from Library.core import Database, ScriptConfiguration, Logger, parse_arguments
from Library.data import TitlesDAO, ListingsDAO, ServicesDAO, RequestsDAO, ListingServiceMapping, RecommendationScoresDAO


def create_new_database_file(file_path, daos):
    # Open new database file.
    if not os.path.isfile(file_path):
        with open(file_path, 'w+'):
            pass

    # Create tables.
    for dao in daos:
        dao(file_path).create_table()
        print('Created table "{}" in "{}".'.format(dao.TABLE, file_path))


def copy_tables(source_file_path, destination_file_path, daos):
    table_rows = {}
    source_database = Database(source_file_path)
    for dao in daos:
        try:
            table_rows[dao.TABLE] = source_database.select(dao.TABLE, columns=dao.SCHEMA)[1:]
        except sqlite3.OperationalError as e:
            pass

    create_new_database_file(destination_file_path, daos)
    destination_database = Database(destination_file_path)
    for table in table_rows:
        rows = [list(r) for r in table_rows.get(table)]
        destination_database.insert_multiple(table, rows)


def reset_database(data_path):
    # Generate directory paths.
    file_name = 'data.db'
    current_file_path = os.path.join(data_path, file_name)

    backup_directory = os.path.join(data_path, 'backups')
    backup_file_name = '{}_{}'.format(datetime.datetime.now().strftime(constants.DATETIME_FORMAT), file_name)
    backup_file_path = os.path.join(backup_directory, backup_file_name)

    temp_file_name = 'temp_{}'.format(file_name)
    temp_file_path = os.path.join(data_path, temp_file_name)

    # Create new temporary database.
    daos_to_create = [TitlesDAO, ListingsDAO, ListingServiceMapping]
    create_new_database_file(temp_file_path, daos_to_create)

    # Copy over static data.
    daos_to_copy = [RequestsDAO, RecommendationScoresDAO, ServicesDAO]
    copy_tables(current_file_path, temp_file_path, daos_to_copy)

    # Ensure backup path exists.
    if not os.path.isdir(backup_directory):
        os.mkdir(backup_directory)

    # Backup current database file.
    if os.path.isfile(current_file_path):
        shutil.copy(current_file_path, backup_file_path)
        print('Backed up "{}" to "{}".'.format(file_name, backup_file_path))

    # Rename temporary file to main.
    os.rename(temp_file_path, current_file_path)


def do_work(sc, log):
    reset_database(sc.paths.get('data'), sc.params.get('service_rows'))
    api = API(sc.paths.get('data'), mode=constants.API.SCRAPE)
    api.refresh_listings(limit=int(sc.params.get('load_limit')))
    return constants.JOB.FINISHED_SUCCESSFULLY, [], []


def main():
    arguments = parse_arguments()
    sc = ScriptConfiguration(config_file_path=arguments.configs)
    script_name = os.path.basename(__file__)
    log = Logger.new(Logger.generate_log_path(sc, script_name), production=sc.is_production())
    log.info(sc.pp_params(script_name=script_name))
    status, warnings, errors = do_work(sc, log)
    for warning in warnings:
        log.warning(warning)
    for error in errors:
        log.error(error)
    if warnings:
        log.info('"{}" finished with warnings.'.format(script_name))
    elif errors:
        log.info('"{}" finished with errors.'.format(script_name))
    else:
        log.info('"{}" finished successfully.'.format(script_name))
    return status


if __name__ == '__main__':
    sys.exit(main())


# TODO Dont think we are using venv in interpreter.

