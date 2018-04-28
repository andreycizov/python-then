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
    def __init__(self, body: Dict):
        self.body = body

    def merge(self, another: 'Body') -> 'Body':
        return Body({**self.body, **another.body})

    @classmethod
    def deserialize(cls, body, workdir):
        r = {
            'type': body['type']
        }

        if 'file' in body:
            r['body'] = deserialize_yaml(os.path.join(workdir, body['file']))

        return Body(r)

    def __repr__(self):
        return f'Body{self.body}'


class Filter:
    def __init__(self, body: Body):
        self.body = body

    def __repr__(self):
        return f'Filter({self.body})'

    def match(self, body: Body):
        if self.body.body['type'] == 'Const':
            for k, v in self.body.body['body'].items():
                if k in body.body:
                    if body.body[k] == self.body.body['body'][k]:
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
    def __init__(self, rules: Dict[Id, Rule] = None):
        self.rules = rules if rules else {}  # type: Dict[Id, Rule]

    @classmethod
    def deserialize(cls, workdir):
        files = os.listdir(workdir)

        rules = {}

        for file in files:
            file_dir = os.path.join(workdir, file)

            body = deserialize_yaml(os.path.join(file_dir, 'conf.yaml'))

            filter_obj = Filter(Body.deserialize(body['filter'], file_dir))
            rule_obj = Body.deserialize(body['body'], file_dir)

            rule = Rule(Id(file), filter_obj, rule_obj)

            rules[rule.id] = rule
        return Structure(rules)

    def list(self) -> List[Rule]:
        return list(self.rules.values())

    def match(self, body: Body) -> List[Body]:
        return [r.body for r in self.rules.values() if r.filter.match(body)]

    def add(self, r: Rule):
        self.rules[r.id] = r

    def remove(self, id: Id):
        if id in self.rules:
            del self.rules[id]

    def __repr__(self):
        return 'Structure(' + '\n'.join(repr(x) for x in self.rules) + ')'
