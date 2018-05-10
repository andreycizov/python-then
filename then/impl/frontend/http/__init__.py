from bottle import route, run, template, response

from then.impl.frontend.tcp.client import FrontendTCPClient
from then.impl.serde.entity import worker_json_to, rule_json_to


def get_client():
    c = FrontendTCPClient('127.0.0.1', 9870)
    return c


URL = 'http://127.0.0.1:8080'


@route('/')
def index():
    c = get_client()
    return {
        'items': [
            {
                'url': URL + '/workers/'
            },
            {
                'url': URL + '/rules/'
            },
        ]
    }


@route('/workers/')
def workers_list():
    c = get_client()
    return {
        'parent': URL + '/',
        'items': [
            worker_json_to(w, c) for w, c in c.worker_list()
        ]
    }


@route('/rules/')
def workers_list():
    c = get_client()
    return {
        'parent': URL + '/',
        'items': [
            rule_json_to(r) for r in c.rule_list()
        ]
    }


def main():
    run(host='localhost', port=8080)


if __name__ == '__main__':
    main()
