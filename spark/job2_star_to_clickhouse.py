import logging
from typing import Any
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PG_URL = "jdbc:postgresql://postgres:5432/mockdb"
PG_PROPS = {
    "user": "spark", "password": "sparkpass", "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPS = {
    "user": "spark", "password": "sparkpass", "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def get_ch_type(dtype: str) -> str:
    mapping = {
        "string": "String", "boolean": "Bool", "tinyint": "Int8",
        "short": "Int16", "integer": "Int32", "long": "Int64",
        "float": "Float32", "double": "Float64", "date": "Date",
        "timestamp": "DateTime"
    }
    if dtype.startswith("decimal"):
        return dtype.capitalize()
    return mapping.get(dtype.lower(), "String")

def ensure_ch_table(df: Any, table: str) -> str:
    fq_table = table if "." in table else f"default.{table}"
    cols = ", ".join([f"`{f.name}` {get_ch_type(f.dataType.simpleString())}" for f in df.schema.fields])
    
    jvm = df.sparkSession.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName(CH_PROPS["driver"])
    conn = jvm.java.sql.DriverManager.getConnection(CH_URL, CH_PROPS["user"], CH_PROPS["password"])
    
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(f"CREATE TABLE IF NOT EXISTS {fq_table} ({cols}) ENGINE = MergeTree ORDER BY tuple()")
            stmt.execute(f"TRUNCATE TABLE {fq_table}")
        finally:
            stmt.close()
    finally:
        conn.close()
    
    return fq_table

def ch_write(df: Any, table: str) -> None:
    fq_table = ensure_ch_table(df, table)
    df.write.option("isolationLevel", "NONE").jdbc(url=CH_URL, table=fq_table, mode="append", properties=CH_PROPS)
    logger.info(f"Записано {df.count()} строк в ClickHouse: {fq_table}")

def read_pg_table(spark: SparkSession, table: str, alias: str):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS).alias(alias)

def main():
    spark = (SparkSession.builder
             .appName("Job2: Star -> ClickHouse")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    f = read_pg_table(spark, "fact_sales", "f")
    p = read_pg_table(spark, "dim_product", "p")
    c = read_pg_table(spark, "dim_customer", "c")
    s = read_pg_table(spark, "dim_store", "s")
    sup = read_pg_table(spark, "dim_supplier", "sup")
    d = read_pg_table(spark, "dim_date", "d")

    sales_w_products = f.join(p, F.col("f.product_id") == F.col("p.product_id"), "inner")
    sales_w_customers = f.join(c, F.col("f.customer_id") == F.col("c.customer_id"), "inner")
    sales_w_stores = f.join(s, F.col("f.store_id") == F.col("s.store_id"), "inner")
    sales_w_suppliers = f.join(sup, F.col("f.supplier_id") == F.col("sup.supplier_id"), "inner")
    sales_w_dates = f.join(d, F.col("f.date_id") == F.col("d.date_id"), "inner")

    ch_write(
        sales_w_products.groupBy("p.product_id", F.col("p.name").alias("product_name"), "p.category", "p.brand")
        .agg(F.sum("f.quantity").cast("bigint").alias("total_units_sold"),
             F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.avg("p.rating").cast("double").alias("avg_rating"),
             F.max("p.reviews").alias("reviews_count"))
        .orderBy(F.desc("total_units_sold")).limit(10),
        "report_top10_products"
    )

    ch_write(
        sales_w_products.groupBy("p.category")
        .agg(F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.count("f.sale_id").cast("bigint").alias("total_sales"),
             F.avg("f.total_price").cast("double").alias("avg_sale_price"))
        .orderBy(F.desc("total_revenue")),
        "report_revenue_by_category"
    )

    ch_write(
        sales_w_customers.groupBy("c.customer_id", F.concat_ws(" ", "c.first_name", "c.last_name").alias("full_name"), "c.email", "c.country")
        .agg(F.sum("f.total_price").cast("double").alias("total_spent"),
             F.count("f.sale_id").cast("bigint").alias("purchases_count"),
             F.avg("f.total_price").cast("double").alias("avg_check"))
        .orderBy(F.desc("total_spent")).limit(10),
        "report_top10_customers"
    )

    ch_write(
        sales_w_customers.groupBy("c.country")
        .agg(F.countDistinct("c.customer_id").cast("bigint").alias("customer_count"),
             F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.avg("f.total_price").cast("double").alias("avg_check"))
        .orderBy(F.desc("total_revenue")),
        "report_customers_by_country"
    )

    ch_write(
        sales_w_dates.groupBy("d.year", "d.month", "d.month_name")
        .agg(F.count("f.sale_id").cast("bigint").alias("total_orders"),
             F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.sum("f.quantity").cast("bigint").alias("units_sold"))
        .orderBy("year", "month"),
        "report_monthly_trends"
    )

    ch_write(
        sales_w_dates.groupBy("d.year", "d.quarter")
        .agg(F.avg("f.total_price").cast("double").alias("avg_order_size"),
             F.sum("f.total_price").cast("double").alias("total_revenue"))
        .orderBy("year", "quarter"),
        "report_quarterly_avg_order"
    )

    ch_write(
        sales_w_stores.groupBy("s.store_id", F.col("s.name").alias("store_name"), "s.city", "s.country")
        .agg(F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.count("f.sale_id").cast("bigint").alias("orders_count"),
             F.avg("f.total_price").cast("double").alias("avg_check"))
        .orderBy(F.desc("total_revenue")).limit(5),
        "report_top5_stores"
    )

    ch_write(
        sales_w_suppliers.groupBy("sup.supplier_id", F.col("sup.name").alias("supplier_name"), "sup.country")
        .agg(F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.avg("f.unit_price").cast("double").alias("avg_product_price"))
        .orderBy(F.desc("total_revenue")).limit(5),
        "report_top5_suppliers"
    )

    ch_write(
        sales_w_products.withColumn("rating_bucket", F.round(F.col("p.rating").cast("double"), 0))
        .groupBy("rating_bucket")
        .agg(F.countDistinct("p.product_id").cast("bigint").alias("product_count"),
             F.sum("f.total_price").cast("double").alias("total_revenue"),
             F.avg("f.total_price").cast("double").alias("avg_revenue_per_sale"))
        .orderBy(F.desc("rating_bucket")),
        "report_rating_vs_sales"
    )

    logger.info("Job 2 успешно завершен.")
    spark.stop()

if __name__ == "__main__":
    main()