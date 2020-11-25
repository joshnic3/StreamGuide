import json

from Library.core import Database
from Library import constants


class DAO:
    TABLE = None
    SCHEMA = None

    def __init__(self, database_file_path):
        self._database = Database(database_file_path)
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
        super().__init__(database_file_path)

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
        super().__init__(database_file_path)

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
        if result:
            return result
        return None


class ListingServiceMapping(DAO):
    TABLE = 'ListingServiceMapping'
    SCHEMA = ['id', 'listing_id', 'service_id']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

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
    SCHEMA = ['id', 'listing_id', 'watch_list', 'click_through']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def write(self, listing_id):
        recommendation_score_id = Database.unique_id()
        self._database.insert(self.TABLE, [recommendation_score_id, listing_id, None, None])

    def update(self, listing_id, watch_list_score=None, click_through_score=None):
        condition = 'listing_id="{}"'.format(listing_id)
        values_dict = {}
        if watch_list_score is not None:
            values_dict['watch_list'] = watch_list_score
        if click_through_score is not None:
            values_dict['watch_list'] = click_through_score
        if values_dict:
            self._database.update(self.TABLE, values_dict, condition)
