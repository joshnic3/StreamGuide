from Library.data import ServicesDAO, TitlesDAO, ListingsDAO, ListingServiceMappingDAO
from Library.scraping import Scraper


class Catalog:
    DATA_MAP = {'display_title': 0, 'service': -1, 'named_data': {'imdb': 3, 'rating': 2, 'year': 1}}
    LISTING_KEYS = ['id', 'title', 'services', 'named_info']

    def __init__(self, database_file_path):
        # Initiate data access objects.
        self._services_dao = ServicesDAO(database_file_path)
        self._titles_dao = TitlesDAO(database_file_path)
        self._listing_dao = ListingsDAO(database_file_path)
        self._listing_service_mapping = ListingServiceMappingDAO(database_file_path)

        # Initiate static data.
        self.listings_dict = {}  # id: { display_title: '', services: '', named_info: {} }

    def get_service_rows(self):
        service_rows = []
        for raw_service_row in self._services_dao.read_all():
            service_rows.append({ServicesDAO.SCHEMA[i]: r for i, r in enumerate(raw_service_row)})
        return service_rows

    def scrape_listings_from_source(self, limit=1000, log=None):
        # Get scraping details for each source.
        service_rows = self._services_dao.read_all()
        service_name_index = self._services_dao.get_column_index('name')
        scraping_url_index = self._services_dao.get_column_index('scraping_url')

        # Scrape data from each source.
        raw_data = []
        for service in service_rows:
            # Prepare list of URLs to scrape.
            print('Scraping listings for "{}".'.format(service[service_name_index]))
            urls = Scraper.generate_urls(service[scraping_url_index], limit)
            for url in urls:
                # Scrape listing from URL.
                print('Scraping URL "{}".'.format(url))
                service_raw_data = Scraper.scrape_page(url)

                # Append service name to each row.
                for row in service_raw_data:
                    row.append(service[service_name_index])

                raw_data.extend(service_raw_data)

        # Format raw data.
        if raw_data:
            listing_rows = []
            named_data_map = Catalog.DATA_MAP.get('named_data')
            for row in raw_data:
                named_info = {k: row[i] for k, i in named_data_map.items() if i <= len(row) - 1 and not row[i] == 'N/A'}
                listing_rows.append({
                    'title': row[Catalog.DATA_MAP.get('display_title')],
                    'service': row[Catalog.DATA_MAP.get('service')],
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
        service_id_map = {r[service_name_index]: r[0] for r in service_rows}

        # Write each row to database.
        listing_display_titles = {}
        if listing_rows is not None:
            for listing in listing_rows:
                # Gather data.
                display_title = listing.get('title')
                named_info = listing.get('named_info')
                service_id = service_id_map.get(listing.get('service'))

                # Add listing if display title has not yet been listed.
                if display_title in listing_display_titles:
                    listing_id = listing_display_titles.get(display_title)
                    # TODO Update existing listing row with any extra named info.
                else:
                    listing_id = self._listing_dao.write(display_title, named_info)
                    listing_display_titles[display_title] = listing_id

                # Add service mapping.
                self._listing_service_mapping.write(listing_id, service_id)

                # Add searchable titles.
                title_rows = [[listing_id, t] for t in self.generate_searchable_titles(display_title)]
                self._titles_dao.write_multiple(title_rows)

            if log:
                log.info('Saved {} listings to database.'.format(len(listing_display_titles)))

    def fetch_listings_from_database(self):
        self.listings_dict = {}
        for row in self._listing_dao.read_all():
            row_as_dict = dict(zip(self._listing_dao.SCHEMA, row))
            self.listings_dict[row_as_dict.get('id')] = {
                'title': row_as_dict.get('display_title'),
                'services': self._listing_service_mapping.read(row_as_dict.get('id')),
                'named_info': row_as_dict.get('named_info')
            }

    def get_listings(self, listing_id, service_filter=None):
        listing = self.listings_dict.get(listing_id, None)
        if listing is not None:
            for service_id in listing.get('services'):
                if service_id in service_filter:
                    return listing
        return None
