class DagNotAvailableException(ValueError):
    def __init__(self, message):
        self.message = message


class DagFileDeletionFailedException(ValueError):
    def __init__(self, message, e):
        self.message = message
        self.e = e


class DagUploadFailedException(ValueError):
    def __init__(self, message, e):
        self.message = message
        self.e = e


class ProjectPathNotFoundException(ValueError):
    def __init__(self, message):
        self.message = message


class ProjectFileNotFoundException(ValueError):
    def __init__(self, message):
        self.message = message


class UnknownAirflowProviderException(ValueError):
    def __init__(self, message):
        self.message = message


class FabricNotConfiguredException(ValueError):
    def __init__(self, message):
        self.message = message


class ArtifactDownloadFailedException(ValueError):
    def __init__(self, response):
        self.message = f"Artifact download failed with status code {response.status_code} and reason {response.text}"
