from Library.data import ServicesDAO, TitlesDAO, ListingsDAO
from Library.scraping import Scraper


class Catalog:
    DATA_MAP = {'display_title': 0, 'service': -1, 'named_data': {'imdb': 3, 'rating': 2, 'year': 1}}
    LISTING_KEYS = ['id', 'title', 'services', 'named_info']

    def __init__(self, database_file_path):
        self._services_dao = ServicesDAO(database_file_path)
        self._titles_dao = TitlesDAO(database_file_path)
        self._listing_dao = ListingsDAO(database_file_path)

        self.listings_dict = {}
        self.service_name_mapping = self._get_service_name_mapping()

    def _get_service_name_mapping(self):
        rows = self._services_dao.read_all()
        id_index = self._services_dao.get_column_index('id')
        name_index = self._services_dao.get_column_index('name')
        return {r[id_index]: r[name_index] for r in rows}

    def scrape_listings_from_source(self, limit=1000, log=None):
        # Get scraping details for each source.
        service_rows = self._services_dao.read_all()
        service_name_index = self._services_dao.get_column_index('name')
        scraping_url_index = self._services_dao.get_column_index('scraping_url')

        # Scrape data from each source.
        raw_data = []
        for service in service_rows:
            # Prepare list of URLs to scrape.
            if log:
                log.info('Scraping listings for "{}".'.format(service[service_name_index]))
            urls = Scraper.generate_urls(service[scraping_url_index], limit)
            for url in urls:
                # Scrape listing from URL.
                if log:
                    log.info('Scraping URL "{}".'.format(url))
                service_raw_data = Scraper.scrape_page(url)

                # Append service name to each row.
                for row in service_raw_data:
                    row.append(service[service_name_index])

                raw_data.extend(service_raw_data)

        # Load format raw data.
        if raw_data:
            listing_rows = []
            named_data_map = Catalog.DATA_MAP.get('named_data')
            for row in raw_data:
                named_info = {k: row[i] for k, i in named_data_map.items() if i <= len(row) - 1 and not row[i] == 'N/A'}
                listing_rows.append({
                    'title': row[Catalog.DATA_MAP.get('display_title')],
                    'services': row[Catalog.DATA_MAP.get('service')],
                    'named_info': named_info
                })
            return listing_rows
        else:
            if log:
                log.warn('No rows found.'.format(len(raw_data)))
            return None

    @staticmethod
    def generate_searchable_titles(display_title):
        display_title = display_title.lower()
        searchable_titles = [display_title]
        replace_char_with_space = ['-', '_']
        remove_char = ['.', ',']
        for char in replace_char_with_space:
            if char in display_title:
                searchable_titles.append(display_title.replace(char, ' '))
        for char in remove_char:
            if char in display_title:
                searchable_titles.append(display_title.replace(char, ''))
        return searchable_titles

    def save_listing_rows_to_database(self, listing_rows, log=None):
        # Get service name mapping.
        service_rows = self._services_dao.read_all()
        service_name_index = self._services_dao.get_column_index('name')
        service_scraping_details = {r[service_name_index]: r[0] for r in service_rows}

        # Write each row to database.
        if listing_rows is not None:
            for listing in listing_rows:
                service_ids = service_scraping_details.get(listing.get('services')).split(',')
                display_title = listing.get('title')
                named_info = listing.get('named_info')
                for service_id in service_ids:
                    # Write listing.
                    listing_id = self._listing_dao.write(service_id, display_title, named_info)

                    # Link all searchable variations of the display title to the listing.
                    searchable_title_rows = [[listing_id, t] for t in self.generate_searchable_titles(display_title)]
                    self._titles_dao.write_multiple(searchable_title_rows)
            if log:
                log.info('Saved {} rows to database.'.format(len(listing_rows)))

    def fetch_listings_from_database(self):
        rows = self._listing_dao.read_all()
        listings_dict = {}
        if rows:
            for row in rows:
                row_as_dict = dict(zip(self._listing_dao.SCHEMA, row))
                listings_dict[row_as_dict.get('id')] = {
                    'title': row_as_dict.get('display_title'),
                    'services': row_as_dict.get('service_id'),
                    'named_info': row_as_dict.get('named_info')
                }

            # TODO Merge listings with the same title and group services.
            # will need listing service mapping table.

        self.listings_dict = listings_dict

    def get_listings(self, listing_id, service_filter=None):
        if self.listings_dict is not None:
            listing = self.listings_dict.get(listing_id, None)
            if listing is not None and listing.get('services') in service_filter:
                return listing
        return None
