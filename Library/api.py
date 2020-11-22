import datetime
import os
from multiprocessing import Pool

import pandas

from Library import constants
from Library.catalog import Catalog
from Library.data import TitlesDAO, ServicesDAO, RequestsDAO


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
        if mode != constants.API.DRY:
            self.catalog = Catalog(self.database_file_path)
            self.requests_dao = RequestsDAO(self.database_file_path)
            self._pre_load_all_data()

    def _pre_load_all_data(self):
        # load service rows.
        for raw_service_row in ServicesDAO(self.database_file_path).read_all():
            self.service_rows.append({ServicesDAO.SCHEMA[i]: r for i, r in enumerate(raw_service_row)})

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
        listings = {}
        for listing_id in listing_ids:
            listing = self.catalog.get_listings(listing_id, service_filter)
            if listing is not None:
                listings[listing_id] = listing

        # Return listings.
        if listings:
            return listings
        return None

    def refresh_listings(self):
        if not self.read_only:
            listing_rows = self.catalog.scrape_listings_from_source()
            self.catalog.save_listing_rows_to_database(listing_rows)
            if self.catalog.listings_dict is not None:
                print('Loaded {} listings.'.format(len(self.catalog.listings_dict)))

    def track_request(self, user_identifier, method, parameters=None, data=None):
        if method == constants.SERVER.GET and parameters is not None:
            self.requests_dao.write(user_identifier, datetime.datetime.now(), method, parameters)
        elif method == constants.SERVER.POST and data is not None:
            self.requests_dao.write(user_identifier, datetime.datetime.now(), method, data)
        else:
            self.requests_dao.write(user_identifier, datetime.datetime.now(), method, None)
