from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List, Dict


class OperationType(Enum):
    CREATED = 1
    UPDATED = 2
    DELETED = 3
    REFRESH = 4


class JobInfoAndOperation:
    def __init__(self, job_info, operation_type):
        self.job_info = job_info
        self.operation_type = operation_type


class EntityIdToFabricId:
    def __init__(self, entity_id: str, fabric_id: str):
        self.entity_id = entity_id
        self.fabric_id = fabric_id


def invert_entity_to_fabric_mapping(entity_id_dict: Dict[str, List[EntityIdToFabricId]]) -> \
        Dict[str, List[EntityIdToFabricId]]:
    result = {}

    for outer_key, inner_list in entity_id_dict.items():
        for inner_key, inner_value in inner_list:
            if inner_key not in result:
                result[inner_key] = []
            result[inner_key].append(EntityIdToFabricId(outer_key, inner_value))

    return result


class JobData(ABC):

    @property
    @abstractmethod
    def name(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def is_valid_job(self) -> bool:
        pass

    # we can't use pbt file because it doesn't have fabric per pipeline which airflow jobs supports
    @property
    @abstractmethod
    def pipeline_and_fabric_ids(self) -> List[EntityIdToFabricId]:
        pass

    @abstractmethod
    def validate_prophecy_managed_checksum(self, salt: str) -> bool:
        pass

    @property
    @abstractmethod
    def job_files(self):
        pass

    @property
    @abstractmethod
    def dag_name(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def is_disabled(self) -> bool:
        pass

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        pass

    @property
    @abstractmethod
    def fabric_id(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def pipelines(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def has_dbt_component(self) -> bool:
        pass
