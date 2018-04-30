import shutil
from tempfile import mkdtemp
from typing import List, Dict, Union
import os
import yaml


class Id:
    def __init__(self, id: str):
        self.id = id

    def __repr__(self):
        return f'Id({self.id})'

    def __eq__(self, other):
        return self.id == other.id

    def __lt__(self, b):
        return self.id < b.id

    def __hash__(self):
        return hash(self.id)


class Body:
    def __init__(self, val: Dict):
        self.val = val

    def merge(self, another: 'Body') -> 'Body':
        return Body({**self.val, **another.val})

    def __repr__(self):
        return f'Body{self.val}'


class Filter:
    def __init__(self, body: Body):
        self.body = body

    def __repr__(self):
        return f'Filter({self.body})'

    def match(self, body: Body):
        if self.body.val['type'] == 'Const':
            for k, v in self.body.val['body'].items():
                if k in body.val:
                    if body.val[k] == self.body.val['body'][k]:
                        pass
                    else:
                        return False
                else:
                    return False
            return True
        else:
            raise NotImplementedError(self.body)


class Job:
    def __init__(self, id: Id, rule: Body, task: Body):
        self.id = id
        self.rule = rule
        self.task = task

    @property
    def body(self) -> Body:
        return self.rule.merge(self.task)

    def __repr__(self):
        return f'Job({self.id}, {self.rule}, {self.task})'


class Rule:
    def __init__(self, id: Id, filter: Filter, body: Body):
        self.id = id
        self.filter = filter
        self.body = body

    def __repr__(self):
        return f'Rule({self.id}, {self.filter}, {self.body})'


def deserialize_yaml(path) -> Union[dict, str]:
    with open(path) as f_in:
        if path.endswith('.yaml'):
            return yaml.load(f_in)
        else:
            return f_in.read()


class Structure:
    def __init__(self, workdir):
        self.workdir = workdir

        os.makedirs(workdir, 0o700, exist_ok=True)

    def _rule_names(self) -> List[Id]:
        r1: List[str] = os.listdir(self.workdir)

        r2 = [Id(x) for x in r1 if not x.startswith('.')]

        return r2

    def _body_deser(self, body, workdir) -> Body:
        r = {
            'type': body['type']
        }

        if 'file' in body:
            r['body'] = deserialize_yaml(os.path.join(workdir, body['file']))

        return Body(r)

    def _body_serde(self, body: Body, workdir, context) -> dict:
        if body.val['type'] == 'Const':
            return body.val
        elif body.val['type'] == 'Python':
            fn = context + '.py'
            with open(os.path.join(workdir, fn), 'w+b') as f_out:
                f_out.write(body.val['body'].encode())
            return {
                'type': 'Python',
                'file': fn
            }

        else:
            raise NotImplementedError(str(body))

    def _rule_deser(self, id: Id):
        # todo if we fail at any of these steps - just ignore this rule amd maybe log an error
        file_dir = os.path.join(self.workdir, id.id)

        body = deserialize_yaml(os.path.join(file_dir, 'conf.yaml'))

        filter_obj = Filter(self._body_deser(body['filter'], file_dir))

        # todo check if the filter is runnable
        rule_obj = self._body_deser(body['body'], file_dir)

        rule = Rule(id, filter_obj, rule_obj)

        return rule

    def list(self) -> List[Rule]:
        r = []
        for x in self._rule_names():
            r.append(self._rule_deser(x))

        import pprint
        pprint.pprint(r)
        return r

    def match(self, body: Body) -> List[Body]:
        return [r.body for r in self.list() if r.filter.match(body)]

    def add(self, r: Rule):
        # we always rewrite the rule even if it exists.

        rule_dir = os.path.join(self.workdir, r.id.id)

        rule_temp_dir = mkdtemp()
        try:

            f_b = self._body_serde(r.filter.body, rule_temp_dir, 'filter')
            b_b = self._body_serde(r.body, rule_temp_dir, 'body')

            conf = {
                'filter': f_b,
                'body': b_b
            }

            with open(os.path.join(rule_temp_dir, 'conf.yaml'), 'w+') as f_out:
                yaml.dump(conf, f_out)
        except:
            os.unlink(rule_temp_dir)
            raise
        else:
            os.rename(rule_temp_dir, rule_dir)

    def remove(self, id: Id):
        rule_temp_dir = mkdtemp()

        try:
            os.rename(os.path.join(self.workdir, id.id), os.path.join(rule_temp_dir, 'tmp'))
        except FileNotFoundError:
            shutil.rmtree(rule_temp_dir)


def __repr__(self):
    return 'Structure(' + '\n'.join(repr(x) for x in self.rules) + ')'
