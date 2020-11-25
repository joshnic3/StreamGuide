import datetime
import os
import shutil
import sys

from Library import constants
from Library.api import API
from Library.core import ScriptConfiguration, Logger, parse_arguments, Database
from Library.data import DatabaseInitiator, ServicesDAO


def reset_database(data_path, service_rows):
    file_name = 'data.db'

    # Generate file paths.
    current_file_path = os.path.join(data_path, file_name)
    temp_file_path = os.path.join(data_path, 'temp_{}'.format(file_name))

    # Create new database and copy over non-volatile data.
    DatabaseInitiator.create_tables(temp_file_path)
    DatabaseInitiator.copy_data(current_file_path, temp_file_path)
    Database(temp_file_path).insert_multiple(ServicesDAO.TABLE, service_rows)

    # Ensure backup path exists.
    backup_directory = os.path.join(data_path, 'backups')
    if not os.path.isdir(backup_directory):
        os.mkdir(backup_directory)

    # Backup current database file.
    if os.path.isfile(current_file_path):
        backup_file_name = '{}_{}'.format(datetime.datetime.now().strftime(constants.DATETIME_FORMAT), file_name)
        backup_file_path = os.path.join(backup_directory, backup_file_name)
        shutil.copy(current_file_path, backup_file_path)
        print('Backed up "{}" to "{}".'.format(file_name, backup_file_path))

    # Switchover temporary file to current.
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

