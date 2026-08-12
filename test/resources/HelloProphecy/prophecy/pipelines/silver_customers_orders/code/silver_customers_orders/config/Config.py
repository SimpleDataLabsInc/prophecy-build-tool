from silver_customers_orders.graph.ZipCodes.config.Config import SubgraphConfig as ZipCodes_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, ZipCodes: dict=None, **kwargs):
        self.spark = None
        self.update(ZipCodes)

    def update(self, ZipCodes: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.ZipCodes = self.get_config_object(
            prophecy_spark, 
            ZipCodes_Config(prophecy_spark = prophecy_spark), 
            ZipCodes, 
            ZipCodes_Config
        )
        pass
