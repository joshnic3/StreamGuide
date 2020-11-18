import json
import os
import sys
from http import HTTPStatus

from flask import Flask, request, Response

from Library import constants
from Library.api import API
from Library.core import parse_arguments, ScriptConfiguration, Logger

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
    print(to_send)
    response = Response(json.dumps(to_send), status=status_code, mimetype='application/json')
    'http://{}'.format(web_server_ip)
    response.headers.add("Access-Control-Allow-Origin", 'http://{}'.format(web_server_ip))
    response.headers.add("Access-Control-Allow-Credentials", "true")
    return response


@app.route('/listings', methods=['GET'])
def listings():
    # Process request.
    search_string = request.args.get('search', default='', type=str).lower().replace('+', ' ')
    # TODO, can and should get this from cookies.
    filter_list = request.args.get('filter', default='', type=str).replace('+', ' ').split(',')
    api.track_request(request.cookies.get('uid'), constants.SERVER.GET, parameters={'search': search_string, 'filter': filter_list})

    suggestions, suggestion_response_time = api.get_suggested_titles(search_string)
    if filter_list:
        results, listing_response_time = api.search_listings(suggestions, filter_list)
    else:
        results, listing_response_time = api.search_listings(suggestions)
    if results is not None:
        time_elapsed = suggestion_response_time + listing_response_time
        return build_response(api.dataframe_to_dict(results), stats={'elapsed': time_elapsed, 'count': len(results)})
    return build_response(None)


@app.route('/services', methods=['GET'])
def services():
    if api.services_rows:
        return build_response(api.services_rows)
    return build_response(None)


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
    sys.exit(main())
