from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher

# Documentation see: https://media.readthedocs.org/pdf/json-rpc/latest/json-rpc.pdf

@dispatcher.add_method
def foobar(**kwargs):
    print("triger foobar")
    return kwargs["foo"] + kwargs["bar"]

@dispatcher.add_method
def add(*args):
    args = list(args)
    print(f"look{args[0]}")
    return args[0] + args[1]


@Request.application
def application(request):
# Dispatcher is dictionary {<method_name>: callable}

    dispatcher["echo"] = lambda s: s
    # dispatcher["add"] = lambda a, b: a + b
    response = JSONRPCResponseManager.handle(
        request.data, dispatcher)
    return Response(response.json, mimetype='application/json')
    
if __name__ == '__main__':
    print(list(dispatcher.items()))
    print(Request)
    run_simple('localhost', 4000, application)
