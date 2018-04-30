a job specifies a filter and returns a set of new tasks to be executed.

we then may store a matrix of which jobs have run other jobs


nomatch ->

'name', lambda x: True, lambda x: [{ 'id': x['id'] }]


'rules' match 'tasks'
'rules' generate 'tasks'


# OK - how do we now execute the rules (?)

Rule definition
---------------

 + How do we define rule executions - ?

# what the plugin does is it starts a container and then waits for it's finalisation
# loading of the variables is what is hard.

# what if it's a python script returning the required job definition?
# what if the python script is the DSL provided by the task server ?

def main(**kwargs):
    run("locust.bpmms.com", 'execution_name', 'docker.bpmms.com/bpmms_uploads_v2')
    wait("")

type: "Docker"
body:
 action: run
 host: locust.bpmms.com
 name: execution_name
 image: docker.bpmms.com/bpmms_uploads_v2
 args:
  - -m bpmms_uploads.scripts.reports.mid_mf
  - type: load
    name:
      - x

