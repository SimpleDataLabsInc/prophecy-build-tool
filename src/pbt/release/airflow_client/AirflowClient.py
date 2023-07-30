from abc import ABC, abstractmethod
from typing import Optional
from google.cloud.storage.blob import Blob
import re


class AirflowRestClient(ABC):

