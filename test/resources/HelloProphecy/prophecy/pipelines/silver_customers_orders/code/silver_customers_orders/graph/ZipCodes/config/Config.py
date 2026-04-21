from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, **kwargs):
        pass

    def update(self, updated_config):
        pass

Config = SubgraphConfig()
