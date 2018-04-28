from then.plugin.process import TaskPoolChannel, TaskPoolWorker


class TestTaskPoolChannel(TaskPoolChannel):
    def task_finished(self, task_id, task_payload):
        print('tf', task_id, task_payload)

    def child_started(self, child_id):
        print('cs', child_id)


class TestWorker(TaskPoolWorker):
    def message(self, task):
        print('msg', task)
