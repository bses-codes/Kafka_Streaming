"""
Author: Bishesh Kafle
Date: 2024-07-22
"""


import unittest
from mysql_connection import *


class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        # Load data from the database or existing data files
        self.prod_data = table_df('stream_data')
        self.con_data = table_df('kafka_con.stream_data')
        self.columns = ["order_id", "account_number", "branch", "transaction_code"]

    def test_size(self):
        # Test size of stream_data
        self.assertEqual(self.prod_data.shape[0], self.con_data.shape[0])

    def test_columns(self):
        # Test whether sent columns are present or not
        for column in self.columns:
            self.assertIn(column, self.con_data.columns)


if __name__ == '__main__':
    unittest.main()
