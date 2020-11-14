import datetime
import os
import shutil
import sys

from Library import constants
from Library.api import API
from Library.core import Database, ScriptConfiguration, Logger, parse_arguments
from Library.data import TitlesDAO, ListingsDAO, ServicesDAO


def reset_database(data_path, service_rows, log):
    # Generate directory paths.
    file_name = 'data.db'
    current_file_path = os.path.join(data_path, file_name)
    backup_directory = os.path.join(data_path, 'backups')
    backup_file_name = '{}_{}'.format(datetime.datetime.now().strftime(constants.DATETIME_FORMAT), file_name)
    backup_file_path = os.path.join(backup_directory, backup_file_name)

    # Ensure backup path exists.
    if not os.path.isdir(backup_directory):
        os.mkdir(backup_directory)

    # Backup database file.
    if os.path.isfile(current_file_path):
        shutil.copy(current_file_path, backup_file_path)
        log.info('Backed up {} to "{}".'.format(file_name, backup_file_path))

    # Open new database file.
    with open(current_file_path, 'w'):
        pass

    # Create tables.
    daos = [TitlesDAO, ListingsDAO, ServicesDAO]
    for dao in daos:
        dao(current_file_path).create_table()
        log.info('Created table "{}" in database.'.format(dao.TABLE))

    # Add services.
    Database(current_file_path).insert_multiple(ServicesDAO.TABLE, service_rows)


def do_work(sc, log):
    reset_database(sc.paths.get('data'), sc.params.get('service_rows'), log)
    api = API(sc.paths.get('data'))
    # TODO should only call API functions.
    # TODO maybe log should be a api member.
    api.catalog.scrape_listings_from_source(limit=int(sc.params.get('load_limit')), log=log)
    api.catalog.save_listings_to_database(log=log)
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




