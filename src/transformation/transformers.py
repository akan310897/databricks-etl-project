from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


def transform_customers(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    aggregate products,orders and customer data.
    """
    
    
    # ── Drop fully null rows ──────────────────────────────────────────────────
    df = df.dropna(how="all")

    df = df.withColumn(
        "customer_name",
        F.when(
            F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == ""),
            F.lit("Unknown")
        ).otherwise(F.col("customer_name"))
    )
    df = df.withColumn("customer_name_raw", F.col("customer_name"))

    # strip everything except letters, space, hyphen, apostrophe, dot
    df = df.withColumn(
        "customer_name",
        F.regexp_replace(F.col("customer_name"), r"[^a-zA-Z .\-']", "")
    )
    df = df.withColumn(
        "customer_name",
        F.regexp_replace(F.col("customer_name"), r"(?<![a-zA-Z])-|-(?![a-zA-Z])", "")
    )
    df = df.withColumn(
        "customer_name",
        F.regexp_replace(F.col("customer_name"), r"(?<![a-zA-Z])'|'(?![a-zA-Z])", "")
    )
    df = df.withColumn(
        "customer_name",
        F.regexp_replace(F.col("customer_name"), r"\.(?!\s|$)", "")
    )
    # collapse multiple spaces and trim edges
    df = df.withColumn(
        "customer_name",
        F.trim(F.regexp_replace(F.col("customer_name"), r" {2,}", " "))
    )

    # ── Email normalisation ───────────────────────────────────────────────────
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # ── Postal code zero-padding ──────────────────────────────────────────────
    df = df.withColumn(
        "postal_code",
        F.when(
            F.length(F.col("postal_code").cast("string")) < 5,
            F.lpad(F.col("postal_code").cast("string"), 5, "0"),
        ).otherwise(F.col("postal_code").cast("string")),
    )

    # ── Phone cleaning ────────────────────────────────────────────────────────
    df = df.withColumn("phone_raw", F.col("phone"))
    df = df.withColumn(
        "phone",
        F.when(F.col("phone").rlike(r"^#ERROR!$|^-\d+$"), F.lit(None))
         .otherwise(F.col("phone")),
    )
    df = df.withColumn(
        "phone_extension",
        F.when(F.col("phone").rlike(r"(?i)x\d+$"),
               F.regexp_extract(F.col("phone"), r"(?i)x(\d+)$", 1))
         .otherwise(F.lit(None)),
    )
    df = (df
        .withColumn("phone", F.regexp_replace("phone", r"(?i)x\d+$", ""))
        .withColumn("phone", F.regexp_replace("phone", r"\D", ""))
        .withColumn("phone",
            F.when(F.col("phone").rlike(r"^00"),
                   F.regexp_replace("phone", r"^00", ""))
             .otherwise(F.col("phone")))
        .withColumn("phone",
            F.when((F.length("phone") == 11) & F.col("phone").rlike(r"^1"),
                   F.substring("phone", 2, 10))
             .otherwise(F.col("phone")))
        .withColumn("phone",
            F.when(F.length("phone") == 10, F.col("phone"))
             .otherwise(F.lit(None)))
    )

    # ── Segment validation → split into good / quarantine ────────────────────
    allowed = ["Consumer", "Corporate", "Home Office"]
    df = df.withColumn("segment", F.initcap(F.col("segment")))

    quarantine_df = (
        df.filter(~F.col("segment").isin(allowed))
          .withColumn("quarantine_reason", F.lit("invalid_segment"))
          .withColumn("quarantine_ts",     F.current_timestamp())
          .withColumn("source_table",      F.lit("bronze_customers"))
    )

    clean_df = df.filter(F.col("segment").isin(allowed))

    # ── Guard: raise if clean output is suspiciously empty ───────────────────
    if clean_df.count() == 0:
        raise ValueError(
            "transform_customers produced 0 rows — possible upstream data loss. "
            "Aborting to prevent overwriting silver with empty data."
        )

    logger.info(
        "transform_customers: %d clean rows, %d quarantined",
        clean_df.count(), quarantine_df.count(),
    )

    return clean_df, quarantine_df




def transform_orders(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Clean and validate raw bronze orders data.
    """
 
    # ── Drop fully null rows ──────────────────────────────────────────────────
    df = df.dropna(how="all")
 
    # ── Date parsing ──────────────────────────────────────────────────────────
    df = df.withColumn("order_date", F.to_date(F.col("order_date"), "d/M/yyyy"))
    df = df.withColumn("ship_date",  F.to_date(F.col("ship_date"),  "d/M/yyyy"))
 
    # ── Ship date must not precede order date ─────────────────────────────────
    df = df.withColumn(
        "ship_before_order_flag",
        F.col("ship_date") < F.col("order_date"),
    )
 
    # ── Quantity must be ≥ 1 ─────────────────────────────────────────────────
    df = df.withColumn(
        "bad_quantity_flag",
        F.col("quantity") <= 0,
    )
 
    # ── Price must be positive ────────────────────────────────────────────────
    df = df.withColumn(
        "bad_price_flag",
        F.col("price") <= 0,
    )
 
    # ── Discount must be in [0, 1] ────────────────────────────────────────────
    df = df.withColumn(
        "bad_discount_flag",
        (F.col("discount") < 0) | (F.col("discount") > 1),
    )
 
    # ── Composite valid / invalid condition ───────────────────────────────────
    invalid_cond = (
        F.col("ship_before_order_flag")
        | F.col("bad_quantity_flag")
        | F.col("bad_price_flag")
        | F.col("bad_discount_flag")
    )
 
    flag_cols = [
        "ship_before_order_flag",
        "bad_quantity_flag",
        "bad_price_flag",
        "bad_discount_flag",
    ]
 
    # ── Split: quarantine ─────────────────────────────────────────────────────
    quarantine_df = (
        df.filter(invalid_cond)
          .withColumn(
              "quarantine_reason",
              F.when(F.col("ship_before_order_flag"), F.lit("ship_before_order"))
               .when(F.col("bad_quantity_flag"),      F.lit("non_positive_quantity"))
               .when(F.col("bad_price_flag"),         F.lit("non_positive_price"))
               .when(F.col("bad_discount_flag"),      F.lit("discount_out_of_range"))
               .otherwise(F.lit(None)),
          )
          .withColumn("quarantine_ts", F.current_timestamp())
          .withColumn("source_table",  F.lit("bronze_orders"))
          .drop(*flag_cols)
    )
 
    # ── Split: clean + derived columns ────────────────────────────────────────
    clean_df = (
        df.filter(~invalid_cond)
          .drop(*flag_cols)
          .withColumn(
              "days_to_ship",
              F.datediff(F.col("ship_date"), F.col("order_date")),
          )
          .withColumn(
              "net_revenue",
              F.round(F.col("price") * (1 - F.col("discount")), 2),
          )
          .withColumn(
              "profit_margin",
              F.when(
                  F.col("price") > 0,
                  F.round(F.col("profit") / F.col("price"), 4),
              ).otherwise(F.lit(None)),
          )
    )
 
    # ── Guard: raise if clean output is suspiciously empty ───────────────────
    if clean_df.count() == 0:
        raise ValueError(
            "transform_orders produced 0 rows — possible upstream data loss. "
            "Aborting to prevent overwriting silver with empty data."
        )
 
    logger.info(
        "transform_orders: %d clean rows, %d quarantined",
        clean_df.count(), quarantine_df.count(),
    )
 
    return clean_df, quarantine_df



def transform_products(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Clean and validate raw bronze products data.
 
    """
 
    # ── Drop fully null rows ──────────────────────────────────────────────────
    df = df.dropna(how="all")
 
    # ── Product name normalisation ────────────────────────────────────────────
    df = (
        df
        .withColumn("product_name", F.regexp_replace("product_name", "\u00a0", " "))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("product_name", F.regexp_replace("product_name", r" {2,}", " "))
        .withColumn("product_name", F.regexp_replace("product_name", r"[.,]+$", ""))
        .withColumn("product_name", F.initcap(F.col("product_name")))
    )
 
    # ── Category / sub_category normalisation ─────────────────────────────────
    df = df.withColumn("category",     F.initcap(F.col("category")))
    df = df.withColumn("sub_category", F.initcap(F.col("sub_category")))
 
    # ── Price validation flag ─────────────────────────────────────────────────
    df = df.withColumn(
        "bad_price_flag",
        F.col("price_per_product") <= 0,
    )
 
    # ── Composite invalid condition ───────────────────────────────────────────
    invalid_cond = F.col("bad_price_flag")
 
    flag_cols = ["bad_price_flag"]
 
    # ── Split: quarantine ─────────────────────────────────────────────────────
    quarantine_df = (
        df.filter(invalid_cond)
          .withColumn(
              "quarantine_reason",
              F.when(F.col("bad_price_flag"), F.lit("non_positive_price"))
               .otherwise(F.lit(None)),
          )
          .withColumn("quarantine_ts", F.current_timestamp())
          .withColumn("source_table",  F.lit("bronze_products"))
          .drop(*flag_cols)
    )
 
    # ── Split: clean ──────────────────────────────────────────────────────────
    clean_df = df.filter(~invalid_cond).drop(*flag_cols)
 
    # ── Dedup: keep lowest price_per_product per product_id ──────────────────
    window_spec = Window.partitionBy("product_id").orderBy(F.col("price_per_product"))
    clean_df = (
        clean_df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn", "state")          # state becomes ambiguous after dedup
    )
 
    # ── Guard: raise if clean output is suspiciously empty ───────────────────
    if clean_df.count() == 0:
        raise ValueError(
            "transform_products produced 0 rows — possible upstream data loss. "
            "Aborting to prevent overwriting silver with empty data."
        )
 
    logger.info(
        "transform_products: %d clean rows, %d quarantined",
        clean_df.count(), quarantine_df.count(),
    )
 
    return clean_df, quarantine_df



def transform_profit_summary(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Validate enriched orders and aggregate profit to
    (year, category, sub_category, customer_id).
    """
 
    # ── Drop fully null rows ──────────────────────────────────────────────────
    df = df.dropna(how="all")
 
    # ── Derive year from order_date ───────────────────────────────────────────
    df = df.withColumn("year", F.year(F.col("order_date")))
 
    # ── Category / sub_category normalisation ─────────────────────────────────
    df = df.withColumn("category",     F.initcap(F.col("category")))
    df = df.withColumn("sub_category", F.initcap(F.col("sub_category")))
 
    # ── Missing-key validation flags ──────────────────────────────────────────
    df = (
        df
        .withColumn("bad_year_flag",     F.col("year").isNull())
        .withColumn("bad_category_flag", F.col("category").isNull() | (F.trim(F.col("category")) == ""))
        .withColumn("bad_customer_flag", F.col("customer_id").isNull())
        .withColumn("bad_profit_flag",   F.col("profit").isNull())
    )
 
    # ── Composite invalid condition ───────────────────────────────────────────
    invalid_cond = (
        F.col("bad_year_flag")
        | F.col("bad_category_flag")
        | F.col("bad_customer_flag")
        | F.col("bad_profit_flag")
    )
 
    flag_cols = ["bad_year_flag", "bad_category_flag", "bad_customer_flag", "bad_profit_flag"]
 
    # ── Split: quarantine ─────────────────────────────────────────────────────
    quarantine_df = (
        df.filter(invalid_cond)
          .withColumn(
              "quarantine_reason",
              F.when(F.col("bad_year_flag"), F.lit("missing_order_date"))
               .when(F.col("bad_category_flag"), F.lit("missing_category"))
               .when(F.col("bad_customer_flag"), F.lit("missing_customer_id"))
               .when(F.col("bad_profit_flag"), F.lit("missing_profit"))
               .otherwise(F.lit(None)),
          )
          .withColumn("quarantine_ts", F.current_timestamp())
          .withColumn("source_table",  F.lit('ENRICHED_ORDERS'))
          .drop(*flag_cols)
    )
 
    # ── Split: clean ──────────────────────────────────────────────────────────
    clean_df = df.filter(~invalid_cond).drop(*flag_cols)
 
    # ── Aggregate profit ──────────────────────────────────────────────────────
    order_by = ["year", "category", "sub_category", "customer_id"]
    agg_df = clean_df.groupBy(*order_by).agg(
        F.sum("profit").alias("total_profit"),
        F.count(F.lit(1)).alias("order_count"),
    )
 
    # ── Guard: raise if agg output is suspiciously empty ─────────────────────
    if agg_df.count() == 0:
        raise ValueError(
            "transform_profit_summary produced 0 rows — possible upstream data loss. "
            "Aborting to prevent overwriting gold with empty data."
        )
 
    logger.info(
        "transform_profit_summary: %d aggregated rows, %d quarantined",
        agg_df.count(), quarantine_df.count(),
    )
 
    return agg_df, quarantine_df
 



    