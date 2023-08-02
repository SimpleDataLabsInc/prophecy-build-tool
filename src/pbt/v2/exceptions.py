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
