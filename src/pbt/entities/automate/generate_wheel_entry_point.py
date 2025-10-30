
code_injection = r"""
class array:
    def __init__(*args,**kwargs):
        pass

class Date:
    def __init__(*args,**kwargs):
        pass

class Schedule:
    def __init__(*args,**kwargs):
        pass

class SensorSchedule:
    def __init__(*args,**kwargs):
        pass

class DagContext:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.current_dag = None
        return cls._instance

    @classmethod
    def get_instance(cls):
        return cls._instance or cls()

    def set_dag(self, dag):
        self.current_dag = dag

    def get_current_dag(self):
        return self.current_dag

    # Context manager methods
    def __enter__(self):
        self._previous_dag = self.current_dag
        self.current_dag = self.dag
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.current_dag = self._previous_dag

class DAG:
    def __init__(self, Schedule, SensorSchedule, *args, **kwargs):
        self.schedule = Schedule
        self.sensor_schedule = SensorSchedule
        self.config = kwargs.get("Config")
        self.tasks = []
    
    def __enter__(self):
        # Set this DAG as the current active DAG
        ctx = DagContext.get_instance()
        ctx.set_dag(self)
        return self

    def __exit__(self, *args, **kwargs): 
        sorted_tasks = self.topological_sort()

        #Do something with configs...

        for task in sorted_tasks:
            task.run()
    
    def add_task(self, task):
        self.tasks.append(task)

    def topological_sort(self):
        #Sorts tasks in topographical order, such that a task comes after any of its
        #upstream dependencies.

        #Heavily inspired by:
        #http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        #:return: list of tasks in topological order
        from collections import OrderedDict

        # convert into an OrderedDict to speedup lookup while keeping order the same
        graph_unsorted = OrderedDict((task.task_id, task) for task in self.tasks)

        graph_sorted = []

        # special case
        if len(self.tasks) == 0:
            return tuple(graph_sorted)

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted
            # graph. If a set of edges doesn't contain any nodes that
            # haven't been resolved, that is, that are still in the
            # unsorted graph, remove the pair from the unsorted graph,
            # and append it to the sorted graph. Note here that by using
            # using the items() method for iterating, a copy of the
            # unsorted graph is used, allowing us to modify the unsorted
            # graph as we move through it. We also keep a flag for
            # checking that that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to bail out as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.task_id in graph_unsorted:
                        break
                # no edges in upstream tasks
                else:
                    acyclic = True
                    del graph_unsorted[node.task_id]
                    graph_sorted.append(node)

            if not acyclic:
                 raise Exception("A cyclic dependency occurred")

        return tuple(graph_sorted)

class Task:
    def __init__(self, task_id, component, *args, **kwargs):
        self.task_id = task_id
        self.component = component
        self.modelName = kwargs.get("modelName")
        self.upstream_list = set()
        self.downstream_list = set()
        dag = kwargs.get("dag")
        # If no DAG passed explicitly, attach to current active DAG
        if dag is None:
            dag = DagContext.get_instance().get_current_dag()
        
        # Register self to DAG
        if dag is not None:
            dag.add_task(self)
    
    def set_upstream(self, task):
        self.upstream_list.add(task)
        task.downstream_list.add(self)
    
    def set_downstream(self, task):
        self.downstream_list.add(task)
        task.upstream_list.add(self)
    
    def __rshift__(self, task):
        if isinstance(task, list):
            for t in task:
                self.set_downstream(t) 
        else:
            self.set_downstream(task)
        return task
    
    def __lshift__(self, task):
        if isinstance(task, list):
            for t in task:
                self.set_upstream(t) 
        else:
            self.set_upstream(task)
        return task

    def run(self):
        if self.component == "Model":
            import subprocess
            #code to get new IDanywhere bearer token here
            def get_IDanywhere_bearer_token():
                pass

            token = get_IDanywhere_bearer_token()

            #TODO
            def read_deployments_file():
                #return catalog, schema, host, http_path
                pass
            
            def create_profile_yml(catalog, schema, host, http_path, token, target="dev",test=True):
                import yaml
                if test:
                    profile_data = {
                        'p4b_demo_2': {
                            'outputs': {
                                'dev': {
                                    'catalog': "hayden",
                                    'host': "HOST_PATH",
                                    'http_path': "HTTP_PATH",
                                    'schema': "insta_cart",
                                    'threads': 1,
                                    'token': "TOKEN",
                                    'type': 'databricks'
                                }
                            },
                            'target': "dev"
                        }
                    }
                else:
                    profile_data = {
                        'prophecy-default': {
                            'outputs': {
                                'dev': {
                                    'catalog': catalog,
                                    'host': host,
                                    'http_path': http_path,
                                    'schema': schema,
                                    'threads': 1,
                                    'token': token,
                                    'type': 'databricks'
                                }
                            },
                            'target': target
                        }
                    }
                
                # Write to profile.yml file
                with open('profile.yml', 'w') as f:
                    yaml.dump(profile_data, f, default_flow_style=False, sort_keys=False)
                
                pass
            
            def get_configs():
                import json
                from importlib import resources
                
                #hidden folder poses challenges
                config_dir = ".prophecy.ide.resolved_pipelines"
                # currently don't have pipeline_id initialized
                with open(json_path, 'r') as f:
                    configs = json.load(f)
                
                config_params = configs["metainfo"]["configuration"]["schema"]["fields"]

                def type_mapper_func(param):
                    if param["kind"]["type"] == "string":
                        val = f"\"{param["kind"]["value"]}\"".replace("'",'\\\"')
                    elif param["kind"]["type"] == "date":
                        val = f'cast({param["kind"]["value"].replace("'", '\"')} as date)'
                    elif param["kind"]["type"] == "sql_expression":
                        val = f'"{param["kind"]["value"]}"'
                    elif param["kind"]["type"] == "array":
                        array_entries_as_strings = [v if isinstance(eval(v),str) else "'"+v+"'" for v in param["kind"]["value"]]
                        _ = f'"ARRAY({",".join(array_entries_as_strings)})"'
                        val = _.replace("'", '\\\"')
                    elif param["kind"]["type"] in ["integer", "double", "float"]:
                        val = f"{param["kind"]["value"]}"
                    else:
                        val = f'"{param["kind"]["value"]}"'
                    return val
                
                return [f"{param["name"]}: {type_mapper_func(param)}" for param in config_params]
            
            token = get_IDanywhere_bearer_token()

            config_vars = get_configs()

            if config_vars:
                vars_string = f'{{{", ".join(config_vars)}}}'
                subprocess.run(["dbt", "run", "--select", self.modelName, "--vars", vars_string])
            else:
                subprocess.run(["dbt", "run", "--select", self.modelName])


"""

entry_function = """
def main():
    pass

if __name__ == "__main__":
    main()
"""

def main(pipeline_ids):
    import re
    from pathlib import Path
    
    deployment_folder = Path("deployment")
    deployment_folder.mkdir(parents=True, exist_ok=True)

    #Turn the repo into a python module
    Path("__init__.py").touch()

    pipeline_ids_list = pipeline_ids.split(",")

    for pipeline_id in pipeline_ids_list:
        with open(f"pipelines/{pipeline_id}.py","r") as f:
            content = f.read()
        
        content_sanitized = re.sub(r"\.in_\d+|\.out_\d+","",content)
        
        complete_script = f"""pipeline_id = "{pipeline_id}"\n\n""" + code_injection + content_sanitized + entry_function
        
        with open(f"deployment/{pipeline_id}_deployable.py","w") as f:
            f.write(complete_script)

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create wheel file entrypoint scripts for Prophecy automate pipelines. Pass a comma-seperated list of pipeline names to the --pipelines flag.")
    parser.add_argument("--pipelines", type=str, required=True, help="comma-seperated list of pipelines to deploy")
    args = parser.parse_args()

    main(args.pipelines)
