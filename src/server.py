import datetime
import json
import os
import sys
from http import HTTPStatus

from flask import Flask, request, Response

from Library import constants
from Library.api import API
from Library.core import parse_arguments, ScriptConfiguration, Logger
from Library.data import RequestsDAO

app = Flask(__name__)
api = None
web_server_ip = None


def build_response(response_dict, stats=None):
    if response_dict:
        status_code = HTTPStatus.OK
    else:
        response_dict = None
        status_code = HTTPStatus.NO_CONTENT
    to_send = {'data': response_dict}
    if stats:
        to_send.update({'stats': stats})
    response = Response(json.dumps(to_send), status=status_code, mimetype='application/json')
    response.headers.add("Access-Control-Allow-Origin", 'http://{}'.format(web_server_ip))
    response.headers.add("Access-Control-Allow-Credentials", "true")
    return response


def track_request(user_identifier, method, parameters=None, data=None):
    if method == constants.SERVER.GET and parameters is not None:
        request_data = parameters
    elif method == constants.SERVER.POST and data is not None:
        request_data = data
    else:
        request_data = None
    request_id = RequestsDAO(api.database_file_path).write(user_identifier, datetime.datetime.now(), method, request_data)
    return request_id


def add_request_response_time(request_id, response_time):
    RequestsDAO(api.database_file_path).update_response_time(request_id, response_time)


@app.route('/listings', methods=['GET'])
def listings():
    # Process request.
    search_string = request.args.get('search', default='', type=str).lower().replace('+', ' ')
    filter_list = request.cookies.get('service_filter').split(',')
    request_id = track_request(request.cookies.get('uid'), constants.SERVER.GET, parameters={'search': search_string})

    # Get requested data from API.
    if filter_list:
        results, time_elapsed = api.search_listings(search_string, filter_list)
    else:
        results, time_elapsed = api.search_listings(search_string)

    # Add response time to request tracking row.
    add_request_response_time(request_id, time_elapsed)

    # Send response.
    if results is not None:
        return build_response(results, stats={'elapsed': time_elapsed, 'count': len(results)})
    return build_response(None)


@app.route('/services', methods=['GET'])
def services():
    if api.service_rows:
        return build_response(api.service_rows)
    return build_response(None)


@app.route('/score/click_through', methods=['POST'])
def click_through():
    listing_id = request.args.post('listing_id', default=None, type=str)
    user_id = request.cookies.get('uid')
    track_request(user_id, constants.SERVER.POST, data={'click_through': listing_id})
    if listing_id:
        api.record_click_through(listing_id)


@app.route('/score/add_to_watch_list', methods=['POST'])
def add_to_watch_list():
    listing_id = request.args.post('listing_id', default=None, type=str)
    user_id = request.cookies.get('uid')
    track_request(user_id, constants.SERVER.POST, data={'add_to_watch_list': listing_id})
    if listing_id:
        api.add_to_watch_list(listing_id, user_id)


@app.route('/score/remove_from_watch_list', methods=['POST'])
def remove_from_watch_list():
    listing_id = request.args.post('listing_id', default=None, type=str)
    user_id = request.cookies.get('uid')
    track_request(user_id, constants.SERVER.PSOT, data={'add_to_watch_list': listing_id})
    if listing_id:
        api.remove_from_watch_list(listing_id, user_id)


def do_work(sc, log):
    global api, web_server_ip
    web_server_ip = sc.params.get('web_server_ip')
    api = API(sc.paths.get('data'), mode=constants.API.READ_ONLY)
    app.run('0.0.0.0', port=sc.params.get('port'))


def main():
    arguments = parse_arguments()
    sc = ScriptConfiguration(config_file_path=arguments.configs)
    script_name = os.path.basename(__file__)
    log = Logger.new(Logger.generate_log_path(sc, script_name), production=sc.is_production())
    log.info(sc.pp_params(script_name=script_name))
    do_work(sc, log)


if __name__ == '__main__':
    print('running')
    sys.exit(main())
