
class Task:
    def __init__(self, task_id, component, *args, **kwargs):
        self.task_id = task_id
        self.component = component
        self.modelName = kwargs.get("modelName")
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
        if self.component == "Model":
            import subprocess
            #code to get new IDanywhere bearer token here
            def get_idanywhere_bearer_token():
                pass

            token = get_IDanywhere_bearer_token()

            #TODO
            def read_deployments_file():
                return catalog, schema, host, http_path
            
            catalog, schema, host, http_path = read_deployments_file()
            
            def create_profile_yml(catalog, schema, host, http_path, token, target="dev"):
                import yaml
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
                # currently don't have pipeline_id initialized
                with open(f'.prophecy/ide/resolved_pipelines/{pipeline_id}.json', 'r') as f:
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
            
            config_vars = get_configs()

            if config_vars:
                vars_string = f'{{{", ".join(config_vars)}}}'
                subprocess.run(["dbt", "run", "--select", self.modelName, "--vars", vars_string])
            else:
                subprocess.run(["dbt", "run", "--select", self.modelName])
        
        elif self.component ==  "Script":

        