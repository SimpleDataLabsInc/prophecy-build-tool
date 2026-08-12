from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from silver_customers_orders.graph.ByCustomerId import *
from silver_customers_orders.config.ConfigStore import *


class ByCustomerIdTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/in0/schema.json',
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/in1/schema.json',
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/in1/data/test_unit_test_0.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/out/schema.json',
            'test/resources/data/silver_customers_orders/graph/ByCustomerId/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = ByCustomerId(self.spark, dfIn0, dfIn1)
        assertDFEquals(
            dfOut.select("account_open_date", "order_id", "customer_id", "amount", "first_name", "last_name"),
            dfOutComputed.select("account_open_date", "order_id", "customer_id", "amount", "first_name", "last_name"),
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
