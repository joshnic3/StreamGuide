DATETIME_FORMAT = '%Y%m%d%H%M%S'


class JOB:
    FINISHED_SUCCESSFULLY = 0
    FINISHED_WITH_WARNINGS = 1
    FINISHED_WITH_ERRORS = 2


class DATETIME:
    FORMAT = '%Y%m%d%H%M%S'
    FORMAT_2 = '%Y-%m-%d %H:%M:%S'
    PP_FORMAT = '%d/%m/%Y %H:%M:%S'


class ServiceDetails:
    ROWS = [
            ['netflix', 'Netflix', 'https://reelgood.com/movies/source/netflix', 'https://img.reelgood.com/service-logos/netflix.svg'],
            ['amazon', 'Prime Video', 'https://reelgood.com/movies/source/amazon', 'https://img.reelgood.com/service-logos/amazon_prime.svg'],
            ['nowtv', 'NowTV', 'https://reelgood.com/movies/source/nowtv', 'https://img.reelgood.com/service-logos/nowtv.svg'],
        ]


class API:
    READ_ONLY = 0
    WRITE = 1
    SCRAPE = 2


class SERVER:
    GET = 'get'
    POST = 'post'


class SCORE:
    WATCH_LIST_ADD = 2
    WATCH_LIST_REMOVE = -1
    CLICK_THROUGH = 1
