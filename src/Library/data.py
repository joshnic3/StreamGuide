import json
import os

from Library.core import Database
from Library import constants

import sqlite3


class DAO:
    TABLE = None
    SCHEMA = None

    def __init__(self, database_file_path, volatile=False):
        self._database = Database(database_file_path)
        self.volatile = volatile
        self.database_file_path = database_file_path

    def create_table(self, table=None, schema=None):
        table = table if table else self.TABLE
        schema = schema if schema else self.SCHEMA
        if table is not None and schema is not None:
            self._database.create_table(table, schema)

    def get_column_index(self, column_name):
        if column_name in self.SCHEMA:
            return self.SCHEMA.index(column_name)
        return None


class TitlesDAO(DAO):
    TABLE = 'Titles'
    SCHEMA = ['id', 'listing_id', 'title_string']

    def __init__(self, database_file_path):
        super().__init__(database_file_path, volatile=True)

    def write(self, listing_id, title_string):
        title_id = Database.unique_id()
        self._database.insert(self.TABLE, [title_id, listing_id, title_string])
        return title_id

    def write_multiple(self, rows):
        rows_with_ids = [[Database.unique_id(), *r] for r in rows]
        self._database.insert_multiple(self.TABLE, rows_with_ids)

    def read_like_string(self, title_string):
        condition = 'title_string like "{}%" OR title_string like "% {}%"'.format(
            title_string.lower(), title_string.lower())
        listing_id_index = self.get_column_index('listing_id')
        return [r[0] for r in self._database.select(self.TABLE, [self.SCHEMA[listing_id_index]], condition)]


class ListingsDAO(DAO):
    TABLE = 'Listings'
    SCHEMA = ['id', 'display_title', 'named_info']

    def __init__(self, database_file_path):
        super().__init__(database_file_path, volatile=True)

    @staticmethod
    def _parse_named_info_to_dict(named_info_string):
        named_info_string = named_info_string.replace("'", '"')
        return json.loads(named_info_string)

    @staticmethod
    def _parse_named_info_to_string(named_info_dict):
        dict_string = json.dumps(named_info_dict)
        return dict_string.replace('"', "'")

    def write(self, display_title, named_info_dict):
        listing_id = Database.unique_id()
        row = [listing_id, display_title, self._parse_named_info_to_string(named_info_dict)]
        self._database.insert(self.TABLE, row)
        return listing_id

    def read_all(self):
        rows = self._database.select(self.TABLE, columns=self.SCHEMA)
        if rows:
            named_info_index = self.get_column_index('named_info')
            return [[*r[:-1], self._parse_named_info_to_dict(r[named_info_index])] for r in rows]
        return []


class ServicesDAO(DAO):
    TABLE = 'Services'
    SCHEMA = ['id', 'name', 'scraping_url', 'icon_url']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def read_all(self):
        result = self._database.select(self.TABLE, columns=self.SCHEMA, distinct=True)
        return result if result else None


class ListingServiceMappingDAO(DAO):
    TABLE = 'ListingServiceMapping'
    SCHEMA = ['id', 'listing_id', 'service_id']

    def __init__(self, database_file_path):
        super().__init__(database_file_path, volatile=True)

    def write(self, listing_id, service_id):
        mapping_id = Database.unique_id()
        self._database.insert(self.TABLE, [mapping_id, listing_id, service_id])
        return mapping_id

    def read(self, listing_id):
        condition = 'listing_id="{}"'.format(listing_id)
        rows = self._database.select(self.TABLE, ['service_id'], condition)
        return [r[0] for r in rows]


class RequestsDAO(DAO):
    TABLE = 'Requests'
    SCHEMA = ['id', 'user_identifier', 'datetime', 'method', 'data', 'response_time']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def write(self, user_identifier, request_datetime, method, data):
        request_id = Database.unique_id()
        values = [request_id, user_identifier, request_datetime.strftime(constants.DATETIME.FORMAT), method,
                  json.dumps(data), None]
        self._database.insert(self.TABLE, values)
        return request_id

    def update_response_time(self, request_id, response_time):
        condition = 'id="{}"'.format(request_id)
        self._database.update(self.TABLE, {'response_time': float(response_time)}, condition)


class RecommendationScoresDAO(DAO):
    TABLE = 'RecommendationScores'
    SCHEMA = ['id', 'listing_id', 'score']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def write(self, listing_id, score=0):
        recommendation_score_id = Database.unique_id()
        self._database.insert(self.TABLE, [recommendation_score_id, listing_id, score])

    def update(self, listing_id, score):
        self._database.update(self.TABLE, {'score': score}, 'listing_id="{}"'.format(listing_id))

    def read(self, listing_id):
        condition = 'listing_id="{}"'.format(listing_id)
        result = self._database.select(self.TABLE, ['score'], condition, distinct=True)[0]
        return result if result else None


class DatabaseInitiator:
    DAOS = [TitlesDAO, ListingsDAO, ListingServiceMappingDAO, RequestsDAO, RecommendationScoresDAO, ServicesDAO]

    @staticmethod
    def create_tables(database_file_path):
        # Open new database file.
        if not os.path.isfile(database_file_path):
            with open(database_file_path, 'w+'):
                pass
            print('Created database file "{}".'.format(database_file_path))

        # Create tables.
        for dao in DatabaseInitiator.DAOS:
            try:
                dao(database_file_path).create_table()
                print('Created table "{}" in "{}".'.format(dao.TABLE, database_file_path))
            except sqlite3.OperationalError as e:
                print('WARNING: Could not create table "{}". SQL Error: "{}"'.format(dao.TABLE, e))

    @staticmethod
    def copy_data(source_file_path, destination_file_path, force_all=False):
        if force_all:
            daos_to_copy = DatabaseInitiator.DAOS
        else:
            daos_to_copy = [dao for dao in DatabaseInitiator.DAOS if not dao(source_file_path).volatile]

        table_rows = {}
        source_database = Database(source_file_path)
        for dao in daos_to_copy:
            try:
                table_rows[dao.TABLE] = source_database.select(dao.TABLE, columns=dao.SCHEMA)[1:]
                print('Copied "{}" data to "{}".'.format(dao.TABLE, destination_file_path))
            except sqlite3.OperationalError as e:
                print('WARNING: Could not copy "{}" data. SQL Error: "{}"'.format(dao.TABLE, e))

        destination_database = Database(destination_file_path)
        for table in table_rows:
            rows = [list(r) for r in table_rows.get(table)]
            destination_database.insert_multiple(table, rows)

