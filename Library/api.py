import datetime
import os
from multiprocessing import Pool

import pandas

from Library import constants
from Library.catalog import Catalog
from Library.data import TitlesDAO, ServicesDAO


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
        self.services_rows = []
        if mode != constants.API.DRY:
            self.catalog = Catalog(self.database_file_path)
            self._pre_load_all_data()

    def _pre_load_all_data(self):
        raw_service_rows = ServicesDAO(self.database_file_path).read_all()
        for raw_service_row in raw_service_rows:
            self.services_rows.append({ServicesDAO.SCHEMA[i]: r for i, r in enumerate(raw_service_row)})
        self.catalog.fetch_listings_from_database()
        if self.catalog.dataframe is not None:
            print('Pre-loaded {} listings.'.format(len(self.catalog.dataframe)))

    @staticmethod
    def dataframe_to_dict(dataframe):
        if isinstance(dataframe, pandas.DataFrame):
            return dataframe.to_dict(orient='records')
        return None

    @performance()
    def get_suggested_titles(self, search_string):
        return sorted(list(set(TitlesDAO(self.database_file_path).read_like_string(search_string))))

    @performance()
    def search_listings(self, search_strings, service_filter=None):
        dataframes = []
        if not isinstance(search_strings, list):
            search_strings = [search_strings]
        for search_string in search_strings:
            dataframes.append(self.catalog.get_listings_for_title(search_string, service_filter))
        if dataframes:
            return pandas.concat(dataframes)
        return None

    def refresh_listings(self):
        if not self.read_only:
            self.catalog.scrape_listings_from_source()
            self.catalog.save_listings_to_database()
            if self.catalog.dataframe is not None:
                print('Loaded {} listings.'.format(len(self.catalog.dataframe)))

    # TODO only have to load from db async, scraping will be done by seperate job
    def async_refresh_listings(self):
        pool = Pool(processes=1)
        pool.apply_async(self.refresh_listings)
