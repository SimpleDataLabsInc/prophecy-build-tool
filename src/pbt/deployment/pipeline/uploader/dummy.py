from abc import ABC

from . import PipelineUploader
from ....utils.utility import Either


class DummyPipelineUploader(PipelineUploader, ABC):

    def upload_pipeline(self):
        return Either(right=True)
