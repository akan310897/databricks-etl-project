"""
Unit tests for Siler layer cleaning logic.
"""

import pytest
from pyspark.sql import functions as F
from src.transformation.transformers import transform_customers,transform_orders,transform_products  # doesn't exist yet
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql import Row
import datetime
from pyspark.sql.types import DateType


class TestCases:
    """
    Each test maps to one rule in CUSTOMER PRODUCTS and ORDERS EXPECTATIONS.
    """

    # ── Critical rules: pipeline must not produce these ──────────────────────

    def test_null_name_is_replaced_not_dropped(self, spark):
        df = spark.createDataFrame(
           [("C001", "Alice1_@ Smith", "alice@example.com", "42158009025",
          "001 Jones Ridges Suite 338 Johnsonfort, FL 95462","Consumer", "USA", "Miami", "Florida", "33157", "Southwest")],
        ["customer_id","customer_name","email","phone","address",
         "segment","country","city","state","postal_code","region"],
        )
        clean,rejected = transform_customers(df)
        #assert clean.count() == 1, "Row must not be dropped"
        assert clean.first()["customer_name"] == "Alice Smith"

    def test_blank_name_is_replaced_not_dropped(self, spark):
        df = spark.createDataFrame(
            [("C002", "   ", "b@x.com", "3105551234","002 Jones Ridges Suite 338 Johnsonfort, FL 95462", "Consumer","USA","Miami","Florida","12345","NorthEast")],
            ["customer_id","customer_name","email","phone","address",
         "segment","country","city","state","postal_code","region"],
        )
        clean,rejected = transform_customers(df)
        assert clean.first()["customer_name"] == "Unknown"

    def test_raw_name_preserved_before_cleaning(self, spark):
        df = spark.createDataFrame(
              [("C003", "Alice3!", "b@x.com", "3105551234","002 Jones Ridges Suite 338 Johnsonfort, FL 95462", "Consumer","USA","Miami","Florida","12345","NorthEast")],
            ["customer_id","customer_name","email","phone","address",
         "segment","country","city","state","postal_code","region"],
        )
        clean,rejected = transform_customers(df)
        assert clean.first()["customer_name_raw"] == "Alice3!"

    def test_invalid_segment_is_quarantined_not_silently_dropped(self, spark):
        df = spark.createDataFrame(
            [
                ("C004", "Alice", "a@x.com", "12345", "3105551234", "Consumer"),
                ("C005", "Bob",   "b@x.com", "12345", "3105559999", "VIP"),
            ],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)  # returns two DFs
        assert clean.count() == 1
        assert rejected.count() == 1
        assert rejected.first()["quarantine_reason"] == "invalid_segment"

    # ── Edge cases ────────────────────────────────────────────────────────────

    def test_postal_code_4_digits_padded_to_5(self, spark):
        df = spark.createDataFrame(
            [("C006", "Carol", "c@x.com", "501", "3105551234", "Consumer")],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)
        assert clean.first()["postal_code"] == "00501"

    def test_postal_code_exactly_5_digits_unchanged(self, spark):
        df = spark.createDataFrame(
            [("C007", "Dan", "d@x.com", "90210", "3105551234", "Consumer")],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)
        assert clean.first()["postal_code"] == "90210"

    def test_phone_error_value_set_to_null(self, spark):
        df = spark.createDataFrame(
            [("C008", "Eve", "e@x.com", "12345", "#ERROR!", "Consumer")],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)
        assert clean.first()["phone"] is None

    def test_phone_extension_split_into_own_column(self, spark):
        df = spark.createDataFrame(
            [("C009", "Frank", "f@x.com", "12345",
              "310-555-1234x99", "Consumer")],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)
        row = clean.first()
        assert row["phone"] == "3105551234"
        assert row["phone_extension"] == "99"

    def test_phone_11_digits_with_country_code_stripped(self, spark):
        df = spark.createDataFrame(
            [("C010","Grace","g@x.com","12345","13105551234","Consumer")],
            ["customer_id","customer_name","email",
             "postal_code","phone","segment"],
        )
        clean, rejected = transform_customers(df)
        assert clean.first()["phone"] == "3105551234"

 

    # ── Volume / regression guards ────────────────────────────────────────────

    def test_output_row_count_never_exceeds_input(self, spark, valid_customer):
        clean, rejected = transform_customers(valid_customer)
        assert clean.count() <= valid_customer.count()

    def test_no_duplicate_customer_ids_in_output(self, spark, valid_customer):
        clean, rejected = transform_customers(valid_customer)
        total = clean.count()
        distinct = clean.select("customer_id").distinct().count()
        assert total == distinct


    #Orders
    def test_order_date_parsed_to_date_type(self, spark):
        df = spark.createDataFrame([("O001","1/1/2023","5/1/2023","Standard Clas","FUR-CH-10002961","JK-153702",2,100.0,0.1,18.0,)],["order_id","order_date", "ship_date","ship_mode","product_id", "customer_id","quantity", "price","discount", "profit"],
        )
        clean, rejected = transform_orders(df)
        assert clean.first()["order_date"] == datetime.date(2023, 1, 1)

    def test_zero_or_negative_quantity_is_rejected(self, spark):
        df = spark.createDataFrame(
            [("O001", "1/1/2023", "5/1/2023", "Standard Class", "FUR-CH-10002961", "JK-15370", 0, 100.0, 0.1, 18.0,)],["order_id","order_date", "ship_date","ship_mode","product_id", "customer_id","quantity", "price","discount", "profit"],
           
        )
        clean, rejected = transform_orders(df)
        assert clean.count() == 0
        assert rejected.count() == 1

    def test_null_quantity_is_rejected(self, spark):
        df = spark.createDataFrame(
            [
                ("O001", "1/1/2023", "5/1/2023", "Standard Class", "FUR-CH-10002961", "JK-15370", None, 100.0, 0.1, 18.0,),
                ("O002", "1/1/2023", "5/1/2023", "Standard Class", "FUR-CH-10002961", "JK-15370", 2, 100.0, 0.1, 18.0,),
            ],["order_id","order_date", "ship_date","ship_mode","product_id", "customer_id","quantity", "price","discount", "profit"],

        )
        clean, rejected = transform_orders(df)
        assert clean.count() == 1
        assert clean.first()["order_id"] == "O002"
        assert rejected.count() == 1
        assert rejected.first()["order_id"] == "O001"
    
    def test_discount_out_of_range_is_rejected(self, spark):
        df = spark.createDataFrame(
            [("O001", "1/1/2023", "5/1/2023", "Standard Class", "FUR-CH-10002961", "JK-15370", 2, 100.0, 2.0, 18.0,)],
            ["order_id","order_date", "ship_date","ship_mode","product_id", "customer_id","quantity", "price","discount", "profit"]
        )
        clean, rejected = transform_orders(df)
        assert clean.count() == 0
        assert rejected.count() == 1
    
    #Products
    def test_non_breaking_space_replaced_with_regular_space(self, spark):
        name_with_nbsp = "Staple\u00a0Holder"
        df = spark.createDataFrame([("P001","office supplies","art",name_with_nbsp,"California",9.99)]
            ,["product_id","category","sub_category","product_name","state","price_per_product"],
        )
        clean, rejected = transform_products(df)
        assert clean.first()["product_name"] == "Staple Holder"


    def test_positive_price_is_kept(self, spark):
        df = spark.createDataFrame(
            [("P001", "Office Chair", "Furniture", "Chairs", "CA", 150.0,)],
            ["product_id","category","sub_category","product_name","state","price_per_product"],
        )
        clean, rejec = transform_products(df)
        assert clean.count() == 1
