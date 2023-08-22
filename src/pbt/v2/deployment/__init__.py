from enum import Enum


class OperationType(Enum):
    CREATED = 1
    UPDATED = 2
    DELETED = 3
    REFRESH = 4


class JobInfoAndOperation:
    def __init__(self, job_info, operation_type):
        self.job_info = job_info
        self.operation_type = operation_type
