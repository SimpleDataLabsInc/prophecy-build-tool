from abc import ABC, abstractmethod

from ...utils.project_models import DAG


class AirflowRestClient(ABC):
    @abstractmethod
    def delete_dag_file(self, dag_id: str):
        pass

    @abstractmethod
    def pause_dag(self, dag_id: str):
        pass

    @abstractmethod
    def unpause_dag(self, dag_id: str):
        pass

    @abstractmethod
    def create_secret(self, key: str, value: str) -> bool:
        pass

    @abstractmethod
    def upload_dag(self, dag_id: str, file_path: str):
        pass

    @abstractmethod
    def get_dag(self, dag_id: str) -> DAG:
        pass

    @abstractmethod
    def delete_dag(self, dag_id: str) -> str:
        pass
