a job specifies a filter and returns a set of new tasks to be executed.

we then may store a matrix of which jobs have run other jobs


nomatch ->

'name', lambda x: True, lambda x: [{ 'id': x['id'] }]


'rules' match 'tasks'
'rules' generate 'tasks'


# OK - how do we now execute the rules (?)