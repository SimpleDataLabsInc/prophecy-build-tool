from abc import abstractmethod, ABC


class PipelineUploader(ABC):
    @abstractmethod
    def upload_pipeline(self, path: str):
        pass

    def exists(self) -> bool:
        pass
