import json

from Library.core import Database


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

    def get_values(self, element_id, columns):
        condition = 'id="{}"'.format(str(element_id))
        if all([True for c in columns if c in self.SCHEMA]):
            return self._database.select(self.TABLE, columns, condition)
        return None


class TitlesDAO(DAO):
    TABLE = 'titles'
    SCHEMA = ['id', 'listing_id', 'title_string']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def write(self, listing_id, title_string):
        title_id = Database.unique_id()
        self._database.insert(self.TABLE, [title_id, listing_id, title_string])
        return title_id

    def read_like_string(self, title_string):
        condition = 'title_string like "{}%" OR title_string like "% {}%"'.format(
            title_string.lower(), title_string.lower())
        title_sting_index = self.get_column_index('title_string')
        return [r[0] for r in self._database.select(self.TABLE, [self.SCHEMA[title_sting_index]], condition)]


class ListingsDAO(DAO):
    TABLE = 'listings'
    SCHEMA = ['id', 'service_id', 'display_title', 'named_info']

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

    def write(self, service_id, display_title, named_info_dict):
        listing_id = Database.unique_id()
        row = [listing_id, service_id, display_title, self._parse_named_info_to_string(named_info_dict)]
        self._database.insert(self.TABLE, row)
        return listing_id

    def read_all(self):
        rows = self._database.select(self.TABLE, columns=self.SCHEMA)
        if rows:
            return [[*r[:-1], self._parse_named_info_to_dict(r[3])] for r in rows]
        return None


class ServicesDAO(DAO):
    TABLE = 'services'
    SCHEMA = ['id', 'name', 'scraping_url', 'icon_url']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)

    def read_all(self):
        result = self._database.select(self.TABLE, columns=self.SCHEMA, distinct=True)
        if result:
            return result
        return None

    def read_service_name(self, service_id):
        return self.get_values(service_id, ['name'])[0][0]


