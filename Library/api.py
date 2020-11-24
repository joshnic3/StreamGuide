import datetime
import os

from Library import constants
from Library.catalog import Catalog
from Library.data import TitlesDAO


def performance():
    def decorator(function):
        def wrapper(*args, **kwargs):
            start_time = datetime.datetime.now()
            result = function(*args, **kwargs)
            time_elapsed_seconds = (datetime.datetime.now() - start_time).total_seconds()
            return result, time_elapsed_seconds
        return wrapper
    return decorator


class API:

    def __init__(self, data_path, mode=constants.API.READ_ONLY):
        self.read_only = True if mode == constants.API.READ_ONLY else False
        self.database_file_path = os.path.join(data_path, 'data.db')
        self.service_rows = []
        self.catalog = Catalog(self.database_file_path)
        if mode != constants.API.SCRAPE:
            self._pre_load_all_data()

    def _pre_load_all_data(self):
        # load service rows.
        self.service_rows = self.catalog.get_service_rows()

        # Log pre-load listings.
        self.catalog.fetch_listings_from_database()
        if self.catalog.listings_dict is not None:
            print('Pre-loaded {} listings {} services into memory.'.format(
                len(self.catalog.listings_dict),
                len(self.service_rows)
            ))

    @performance()
    def search_listings(self, search_string, service_filter=None):
        # Fetch set of listing ids with similar titles.
        listing_ids = list(set(TitlesDAO(self.database_file_path).read_like_string(search_string)))

        # Search listing dictionary for listing IDs.
        listings = []
        for listing_id in listing_ids:
            listing = self.catalog.get_listings(listing_id, service_filter)
            if listing is not None:
                listings.append(listing)

        # Return listings.
        return listings if listings else None

    def refresh_listings(self, limit=1000):
        if not self.read_only:
            listing_rows = self.catalog.scrape_listings_from_source(limit=limit)
            self.catalog.save_listing_rows_to_database(listing_rows)



