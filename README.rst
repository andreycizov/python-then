a job specifies a filter and returns a set of new tasks to be executed.

we then may store a matrix of which jobs have run other jobs


nomatch ->

'name', lambda x: True, lambda x: [{ 'id': x['id'] }]


'rules' match 'tasks'
'rules' generate 'tasks'


# OK - how do we now execute the rules (?)

Rule definition
---------------

This format implements the abstract defined in |High-level structure defitions|.

 + How do we define rule executions - ?

# what the plugin does is it starts a container and then waits for it's finalisation
# loading of the variables is what is hard.

+ what if it's a python script returning the required job definition?
+ what if the python script is the DSL provided by the task server ?

def main(**kwargs):
    # in this script, globals are defined by the python execution plugin
    # this way, we provide a programmable interface to running the tasks
    # subsequently, we may move to purely-rule-definition language, if it
    # matures enough
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

High-level structure definitions
--------------------------------

Stopping
________

Need to implement the service that would allow one to set up a breakpoint.
Breakpoints are matched before rule matching and short-circuit the rule-matching if matches have been found
therefore allowing one to stop the tasks mid execution (they are otherwise simple rule definitions).

The other alternative to the solution above is to expect the users of the system
to implement the given functionality themselves. We are aiming for practical implementation.

Callables
_________

For now, this is just about static analysis subsequent are a set of tasks to be next subsequent.
We constrain analysis to only the statically-decided subpaths.
On the other hand, some parts of the program could only be inferred during the run time. For example:
a callback would be purely a symbolic value until the function receiving a callback is called. In this
situation we are able to tell which of the callbacks have been passed statically from the top caller.

If we agree that this system is always "online" - then the analyser decisions could be postponed till relevant paths are executed.
Can we easily represent this in the interface?

a Type is just a simple task name in this context.
atom: List[Type]|Type|Branch -> subsequent tasks are a list of tasks (1-many)
Type -> subsequent is a single task
Branch[a=atom, b=atom...]

what if we add a "call" -> it puts the return address on the stack, t
that way we could enable procedures defined outside the caller context.