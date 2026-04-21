from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from raw_bronze.graph.ReformatCustomers import *
from raw_bronze.config.ConfigStore import *


class ReformatCustomersTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/raw_bronze/graph/ReformatCustomers/in0/schema.json',
            'test/resources/data/raw_bronze/graph/ReformatCustomers/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/raw_bronze/graph/ReformatCustomers/out/schema.json',
            'test/resources/data/raw_bronze/graph/ReformatCustomers/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = ReformatCustomers(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("customer_id", "account_open_date"),
            dfOutComputed.select("customer_id", "account_open_date"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
