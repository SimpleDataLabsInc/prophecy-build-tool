class PBTException(ValueError):
    def __init__(self, message):
        self.message = message


class DagNotAvailableException(PBTException):
    def __init__(self, message):
        self.message = message


class DagFileDeletionFailedException(PBTException):
    def __init__(self, message, e):
        self.message = message
        self.e = e


class DagUploadFailedException(PBTException):
    def __init__(self, message, e):
        self.message = message
        self.e = e


class DagListParsingFailedException(PBTException):
    def __init__(self, message, e):
        self.message = message
        self.e = e


class ProjectPathNotFoundException(PBTException):
    def __init__(self, message):
        self.message = message


class ProjectFileNotFoundException(PBTException):
    def __init__(self, message):
        self.message = message


class UnknownAirflowProviderException(PBTException):
    def __init__(self, message):
        self.message = message


class FabricNotConfiguredException(PBTException):
    def __init__(self, message):
        self.message = message


class ArtifactDownloadFailedException(PBTException):
    def __init__(self, response):
        self.message = f"Artifact download failed with status code {response.status_code} and reason {response.text}"


class InvalidFabricException(PBTException):
    def __init__(self, message):
        self.message = message


class ProjectBuildFailedException(PBTException):
    def __init__(self, message):
        self.message = message


class PipelineBuildFailedException(PBTException):
    def __init__(self, message):
        self.message = message


class ConfigFileNotFoundException(PBTException):
    def __init__(self, message):
        self.message = message


class PipelinePathNotFoundException(PBTException):
    def __init__(self, message):
        self.message = message


class DuplicateJobNameException(PBTException):
    def __init__(self, message):
        self.message = message
