from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher

import argparse

def porcodio(s):
    return  "Porco dio " + s

@Request.application
def application(request):
    # Dispatcher is dictionary {<method_name>: callable}
    dispatcher["diocane"] = porcodio

    response = JSONRPCResponseManager.handle(request.data, dispatcher)
    return Response(response.json, mimetype='application/json')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--service-port', help='This is the port of the rpc service', default=4000)
    args = parser.parse_args()

    run_simple('localhost', args.service_port, application)