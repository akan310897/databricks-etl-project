"""
Unit tests for Gold layer aggregation logic (aggregations.py).
"""
 
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
from pyspark.sql import functions as F
from src.utils.schemas import ORDERS_SCHEMA,CUSTOMER_SCHEMA,PRODUCTS_SCHEMA,ENRICHED_ORDERS_SCHEMA
from datetime import date
 
# ── import the function under test ──────────────────────────────────────────
from src.transformation.transformers import transform_profit_summary
 


 
 
class TestCases:
    """
    Contract tests: verify the shape and semantics of build_profit_summary output.
    """
 
    def test_output_has_expected_columns(self, spark):
        df = spark.createDataFrame(
            [("O00001", date(2023, 1, 15), "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair", "Furniture", "Chairs"),],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
        expected_cols = {"year", "category", "sub_category", "customer_id",
                          "total_profit", "order_count"}
        assert set(agg_df.columns) == expected_cols, (
            f"Column mismatch. Got: {set(agg_df.columns)}"
        )
 
    def test_profit_summed_for_same_group(self, spark):
        df = spark.createDataFrame(
            [
                ("O00001", date(2023, 1, 15), "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair", "Furniture", "Chairs"),
                ("O00005", date(2023, 2, 20), "C001", date(2023, 8, 10), "Standard Class", None,
     1, 60.00, 0.00, 18.00, 60.00, 0.300, "Priya Sharma", "United States",
     "P0088", "Ergo Office Chair","Furniture", "Chairs")
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert agg_df.count() == 1, "Same group must collapse to one row"
        assert agg_df.first()["total_profit"] == 63.6
        assert agg_df.first()["order_count"] == 2
        assert quarantine_df.count() == 0
 
    
 
    def test_negative_profit_included_in_sum(self, spark):
        df = spark.createDataFrame(
            [
                ("O00001", date(2023, 1, 15), "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair", "Furniture", "Chairs"),
               ("O10002", date(2023, 2, 10), "C001", date(2023, 2, 13), "Standard Class", 3,
     1, 50.00, 0.10, -5.25, 45.00, -0.117, "Neha Kapoor", "United States",
     "P0500", "Study chair", "Furniture", "Chairs"),
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert agg_df.count() == 1
        assert agg_df.first()["total_profit"] == 40.35
 
   
 
    def test_null_profit_quarantined(self, spark):
        """
        transform_profit_summary flags null profit as `bad_profit_flag` and
        routes the row to quarantine rather than folding it into the sum as 0.
        This replaces the original "null treated as zero" expectation, which
        doesn't match the current transformer logic.
        """
        df = spark.createDataFrame(
            [
                ("O00001", date(2023, 1, 15), "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair", "Furniture", "Chairs"),
               ("O10002", date(2023, 2, 10), "C010", date(2023, 2, 13), "Standard Class", 3,
     1, 50.00, 0.10, None, 45.00, -0.117, "Neha Kapoor", "United States",
     "P0500", "Desk Lamp", "Furniture", "Furnishings"),
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert agg_df.first()["total_profit"] == 45.6, (
            "Only the valid row should contribute to the sum"
        )
        assert agg_df.first()["order_count"] == 1, (
            "The null-profit row must not be counted as a valid order line"
        )
        assert quarantine_df.count() == 1
        assert quarantine_df.first()["quarantine_reason"] == "missing_profit"
 
    def test_null_category_quarantined(self, spark):
        """New coverage: missing category also can't be grouped, so it's quarantined."""
        df = spark.createDataFrame(
            [
                ("O00001", date(2023, 1, 15), "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair",None, "Chairs"),
               ("O10002", date(2023, 2, 10), "C010", date(2023, 2, 13), "Standard Class", 3,
     1, 50.00, 0.10, 5.25, 45.00, -0.117, "Neha Kapoor", "United States",
     "P0500", "Desk Lamp", "Furniture", "Furnishings")
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert quarantine_df.count() == 1
        assert quarantine_df.first()["quarantine_reason"] == "missing_category"
        assert agg_df.count() == 1
        assert agg_df.first()["total_profit"] == 5.25
 
    def test_null_order_date_quarantined(self, spark):
        """New coverage: missing order_date means no year to group by."""
        df = spark.createDataFrame(
            [
                ("O00001", None, "C001", date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair","Furniture", "Chairs"),
               ("O10002", date(2023, 2, 10), "C010", date(2023, 2, 13), "Standard Class", 3,
     1, 50.00, 0.10, 5.25, 45.00, -0.117, "Neha Kapoor", "United States",
     "P0500", "Desk Lamp", "Furniture", "Furnishings")
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert quarantine_df.count() == 1
        assert quarantine_df.first()["quarantine_reason"] == "missing_order_date"
        assert agg_df.count() == 1
        assert agg_df.first()["total_profit"] == 5.25
 
    def test_null_customer_id_quarantined(self, spark):
        """New coverage: missing customer_id means no customer to group by."""
        df = spark.createDataFrame(
            [
                ("O00001", date(2023, 1, 15), None, date(2023, 1, 18), "Standard Class", 3,
     2, 120.00, 0.10, 45.60, 216.00, 0.211, "Priya Sharma", "United States",
     "P0001", "Ergo Office Chair","Furniture", "Chairs"),
               ("O10002", date(2023, 2, 10), "C010", date(2023, 2, 13), "Standard Class", 3,
     1, 50.00, 0.10, 5.25, 45.00, -0.117, "Neha Kapoor", "United States",
     "P0500", "Desk Lamp", "Furniture", "Furnishings")
            ],
            ENRICHED_ORDERS_SCHEMA,
        )
        agg_df, quarantine_df = transform_profit_summary(df)
 
        assert quarantine_df.count() == 1
        assert quarantine_df.first()["quarantine_reason"] == "missing_customer_id"
        assert agg_df.count() == 1
        assert agg_df.first()["total_profit"] == 5.25
 
   
 
    