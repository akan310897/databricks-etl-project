from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType,LongType

# Customer Data Schema
CUSTOMER_SCHEMA = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),  # String preserves leading zeros
    StructField("Region", StringType(), True)
])

# Orders Data Schema
ORDERS_SCHEMA = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),   # Casted to DateType during cleaning
    StructField("Ship Date", StringType(), True),    # Casted to DateType during cleaning
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

# Products Data Schema
PRODUCTS_SCHEMA = StructType([
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), True)
])

#Enriched Orders Schema

ENRICHED_ORDERS_SCHEMA = StructType([
    StructField("order_id",       StringType(),  False),
    StructField("order_date",     DateType(),    True),
    StructField("customer_id",    StringType(),  True),
    StructField("ship_date",      DateType(),    True),
    StructField("ship_mode",      StringType(),  True),
    StructField("days_to_ship",   IntegerType(), True),
    StructField("quantity",       IntegerType(), True),
    StructField("price",          DoubleType(),  True),
    StructField("discount",       DoubleType(),  True),
    StructField("profit",         DoubleType(),  True),
    StructField("net_revenue",    DoubleType(),  True),
    StructField("profit_margin",  DoubleType(),  True),
    StructField("customer_name",  StringType(),  True),
    StructField("Country",        StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("sub_category",   StringType(),  True),
])

# Agg table schema

GOLD_SALES_SCHEMA = StructType([
    StructField("year",             IntegerType(), nullable=False),
    StructField("category",         StringType(), nullable=True),
    StructField("sub_category",     StringType(), nullable=True),
    StructField("customer_id",      StringType(), nullable=False),
    StructField("total_profit",     DoubleType(), nullable=True),
    StructField("order_count",      LongType(),   nullable=False),
])