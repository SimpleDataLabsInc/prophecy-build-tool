from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, fabricName: str=None):
        self.spark = None
        self.update(fabricName)

    def update(self, fabricName: str):
        self.fabricName = fabricName
