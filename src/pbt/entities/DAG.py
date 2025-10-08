
class DAG(Config, Schedule, SensorSchedule):
    def __init__(self, Schedule, SensorSchedule):
        self.schedule = Schedule
        self.sensor_schedule = SensorSchedule
        self.config = Config
        self.tasks = []
    
    def __enter__(self):
        # Set this DAG as the current active DAG
        DagContext.set_current_dag(self)
        return self

    def __exit__(self): 
        sorted_tasks = self.topological_sort()

        #Do something with configs...

        for task in sorted_tasks:
            task.run()

    def topological_sort(self):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """

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

            # if not acyclic:
            #     raise AirflowException("A cyclic dependency occurred in dag: {}"
            #                            .format(self.dag_id))

        return tuple(graph_sorted)