from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from gold_sales.graph.Cleanup import *
from gold_sales.config.ConfigStore import *


class CleanupTest(BaseTestCase):

    def test_order_date_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/gold_sales/graph/Cleanup/in0/schema.json',
            'test/resources/data/gold_sales/graph/Cleanup/in0/data/test_order_date_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/gold_sales/graph/Cleanup/out/schema.json',
            'test/resources/data/gold_sales/graph/Cleanup/out/data/test_order_date_test.json',
            'out'
        )
        dfOutComputed = Cleanup(self.spark, dfIn0)
        assertDFEquals(dfOut.select("order_date"), dfOutComputed.select("order_date"), self.maxUnequalRowsToShow)

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
