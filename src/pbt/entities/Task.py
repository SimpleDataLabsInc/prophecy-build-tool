
class Task(task_id, component, modelName, dag=None):
    def __init__(self, task_id, component, modelName, dag=None):
        self.task_id = task_id
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        
        # If no DAG passed explicitly, attach to current active DAG
        if dag is None:
            dag = DagContext.get_current_dag()
        
        # Register self to DAG
        if dag is not None:
            dag.add_task(self)
    
    def set_upstream(self, task):
        self.upstream_task_ids.add(task.task_id)
        task.downstream_task_ids.add(self.task_id)
    
    def set_downstream(self, task):
        self.downstream_task_ids.add(task.task_id)
        task.upstream_task_ids.add(self.task_id)
    
    def __rshift__(self, task):
        self.set_downstream(task)
        return task
    
    def __lshift__(self, task):
        self.set_upstream(task)
        return task

    def run(self):
        pass