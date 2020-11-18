import pandas

from Library.data import ServicesDAO, TitlesDAO, ListingsDAO
from Library.scraping import Scraper


class Catalog:
    DATA_MAP = {'display_title': 0, 'service': -1, 'named_data': {'imdb': 3, 'rating': 2, 'year': 1}}
    DATAFRAME_COLUMNS = ['title', 'service', 'named_info']

    def __init__(self, database_file_path):
        self._services_dao = ServicesDAO(database_file_path)
        self._titles_dao = TitlesDAO(database_file_path)
        self._listing_dao = ListingsDAO(database_file_path)

        self.dataframe = None
        self.service_name_mapping = self._get_service_name_mapping()

    def _get_service_name_mapping(self):
        rows = self._services_dao.read_all()
        id_index = self._services_dao.get_column_index('id')
        name_index = self._services_dao.get_column_index('name')
        return {r[id_index]: r[name_index] for r in rows}

    def _build_dataframe_from_raw_data(self, raw_data):
        data_rows = []
        named_data_map = Catalog.DATA_MAP.get('named_data')
        for row in raw_data:
            named_info = {k: row[i] for k, i in named_data_map.items() if i <= len(row) - 1 and not row[i] == 'N/A'}
            data_rows.append([
                row[Catalog.DATA_MAP.get('display_title')],
                row[Catalog.DATA_MAP.get('service')],
                named_info
            ])
        return pandas.DataFrame(data_rows, columns=Catalog.DATAFRAME_COLUMNS)

    def _build_dataframe_from_database_rows(self, rows):
        # TODO Merge listings with the same title.
        # merging service names into comma seperated list should work with existing search.
        # as we are using icon images (more user friendly) and client has client rows we can use client id here
        data_rows = []
        for row in rows:
            data_rows.append([
                row[self._listing_dao.get_column_index('display_title')],
                row[self._listing_dao.get_column_index('service_id')],
                row[self._listing_dao.get_column_index('named_info')]
            ])
        if data_rows:
            dataframe = pandas.DataFrame(data_rows, columns=Catalog.DATAFRAME_COLUMNS)
            return self._group_by_services_in_dataframe(dataframe)
        return None

    def _group_by_services_in_dataframe(self, dataframe):
        return dataframe.groupby(['title'])['service'].apply(lambda x: ','.join(x)).reset_index()

    def scrape_listings_from_source(self, limit=1000, log=None):
        # Get scraping details for each source.
        service_rows = self._services_dao.read_all()
        service_name_index = self._services_dao.get_column_index('name')
        scraping_url_index = self._services_dao.get_column_index('scraping_url')

        # Scrape data from each source.
        raw_data = []
        for service in service_rows:
            if log:
                log.info('Scraping listings for "{}".'.format(service[service_name_index]))
            urls = Scraper.generate_urls(service[scraping_url_index], limit)
            for url in urls:
                if log:
                    log.info('Scraping URL "{}".'.format(url))
                service_raw_data = Scraper.scrape_page(url)

                # Append service name to each row.
                for row in service_raw_data:
                    row.append(service[service_name_index])

                raw_data.extend(service_raw_data)

        # Load raw data into dataframe.
        if raw_data:
            self.dataframe = self._build_dataframe_from_raw_data(raw_data)
        else:
            if log:
                log.warn('No rows found.'.format(len(raw_data)))
            self.dataframe = None

    def save_listings_to_database(self, log=None):
        # Get service name mapping.
        service_rows = self._services_dao.read_all()
        service_name_index = self._services_dao.get_column_index('name')
        service_scraping_details = {r[service_name_index]: r[0] for r in service_rows}

        # Write each dataframe row to database.
        if self.dataframe is not None:
            for row in self.dataframe.values.tolist():
                display_title = row[self.DATAFRAME_COLUMNS.index('title')]
                service_ids = service_scraping_details.get(row[self.DATAFRAME_COLUMNS.index('service')]).split(',')
                named_info = row[self.DATAFRAME_COLUMNS.index('named_info')]
                for service_id in service_ids:
                    listing_id = self._listing_dao.write(service_id, display_title, named_info)
                self._titles_dao.write(listing_id, display_title.lower())
            if log:
                log.info('Saved {} rows to database.'.format(len(self.dataframe)))

    def fetch_listings_from_database(self):
        rows = self._listing_dao.read_all()
        if rows:
            self.dataframe = self._build_dataframe_from_database_rows(rows)
        else:
            self.dataframe = None

    def get_listings_for_title(self, title_string, service_filter=None):
        if self.dataframe is not None:
            df = self.dataframe
            if service_filter:
                pattern = '|'.join([s for s in service_filter])
                df = df.loc[(df['title'].str.lower() == title_string) & (df['service']).str.contains(pattern)]
                return df
            else:
                return df.loc[df['title'].str.lower() == title_string]
        return None
