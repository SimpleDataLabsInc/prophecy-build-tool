
class Config:

    def __init__(self, fabricName: str=None):
        self.update(fabricName)

    def update(self, fabricName: str):
        self.fabricName = fabricName
