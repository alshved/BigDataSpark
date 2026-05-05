import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PG_URL = "jdbc:postgresql://postgres:5432/mockdb"
PG_PROPS = {
    "user": "spark",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver",
}

def pg_truncate(spark: SparkSession, table: str):
    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_PROPS["user"], PG_PROPS["password"])
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(f"TRUNCATE TABLE {table} CASCADE")
        finally:
            stmt.close()
    finally:
        conn.close()

def pg_write(df, table: str, spark: SparkSession):
    pg_truncate(spark, table)
    df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)
    logger.info(f"Записано {df.count()} строк в PostgreSQL: {table}")

def build_dim_with_id(df, cols_mapping, distinct_cols, id_col_name):
    select_expr = [F.col(k).alias(v) for k, v in cols_mapping.items()]
    return (df.select(*select_expr)
            .dropDuplicates(distinct_cols)
            .withColumn(id_col_name, (F.monotonically_increasing_id() + 1).cast("integer")))

def main():
    spark = (SparkSession.builder
             .appName("Job1: Raw to Star Schema")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS).cache()
    logger.info(f"Всего строк для обработки: {raw.count()}")

    dims_with_id = {
        "dim_customer": {
            "id_col": "sale_customer_id",
            "mapping": {
                "sale_customer_id": "customer_id", "customer_first_name": "first_name",
                "customer_last_name": "last_name", "customer_age": "age",
                "customer_email": "email", "customer_country": "country",
                "customer_postal_code": "postal_code", "customer_pet_type": "pet_type",
                "customer_pet_name": "pet_name", "customer_pet_breed": "pet_breed"
            }
        },
        "dim_seller": {
            "id_col": "sale_seller_id",
            "mapping": {
                "sale_seller_id": "seller_id", "seller_first_name": "first_name",
                "seller_last_name": "last_name", "seller_email": "email",
                "seller_country": "country", "seller_postal_code": "postal_code"
            }
        }
    }

    for table, config in dims_with_id.items():
        select_expr = [F.col(k).alias(v) for k, v in config["mapping"].items()]
        dim_df = raw.select(*select_expr).dropDuplicates([config["mapping"][config["id_col"]]])
        pg_write(dim_df, table, spark)

    dim_product = (raw.select(
        F.col("sale_product_id").alias("product_id"), F.col("product_name").alias("name"),
        F.col("product_category").alias("category"), F.col("product_price").alias("price"),
        F.col("product_quantity").alias("quantity"), F.col("product_weight").alias("weight"),
        F.col("product_color").alias("color"), F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"), F.col("product_material").alias("material"),
        F.col("product_description").alias("description"), F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews"), F.col("pet_category"),
        F.to_date("product_release_date", "M/d/yyyy").alias("release_date"),
        F.to_date("product_expiry_date", "M/d/yyyy").alias("expiry_date")
    ).dropDuplicates(["product_id"]))
    pg_write(dim_product, "dim_product", spark)

    dim_store = build_dim_with_id(
        raw,
        {"store_name": "name", "store_location": "location", "store_city": "city",
         "store_state": "state", "store_country": "country", "store_phone": "phone", "store_email": "email"},
        ["name", "city"], "store_id"
    )
    pg_write(dim_store, "dim_store", spark)

    dim_supplier = build_dim_with_id(
        raw,
        {"supplier_name": "name", "supplier_contact": "contact", "supplier_email": "email",
         "supplier_phone": "phone", "supplier_address": "address", "supplier_city": "city", "supplier_country": "country"},
        ["name"], "supplier_id"
    )
    pg_write(dim_supplier, "dim_supplier", spark)

    dim_date = (raw.select(F.to_date("sale_date", "M/d/yyyy").alias("full_date"))
                .dropDuplicates(["full_date"])
                .withColumn("day", F.dayofmonth("full_date"))
                .withColumn("month", F.month("full_date"))
                .withColumn("year", F.year("full_date"))
                .withColumn("quarter", F.quarter("full_date"))
                .withColumn("month_name", F.date_format("full_date", "MMMM"))
                .withColumn("day_of_week", F.dayofweek("full_date"))
                .withColumn("date_id", (F.monotonically_increasing_id() + 1).cast("integer")))
    pg_write(dim_date, "dim_date", spark)

    store_lkp = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS).select("store_id", "name", "city")
    supplier_lkp = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS).select("supplier_id", "name")
    date_lkp = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS).select("date_id", "full_date")

    fact_sales = (raw
        .join(store_lkp.alias("st"), (raw["store_name"] == F.col("st.name")) & (raw["store_city"] == F.col("st.city")), "left")
        .join(supplier_lkp.alias("sup"), raw["supplier_name"] == F.col("sup.name"), "left")
        .join(date_lkp, F.to_date(raw["sale_date"], "M/d/yyyy") == date_lkp["full_date"], "left")
        .select(
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            F.col("st.store_id").alias("store_id"),
            F.col("sup.supplier_id").alias("supplier_id"),
            F.col("date_id"),
            F.col("sale_quantity").alias("quantity"),
            F.col("sale_total_price").alias("total_price"),
            F.col("product_price").alias("unit_price")
        )
    )
    pg_write(fact_sales, "fact_sales", spark)

    logger.info("Job 1 успешно завершен.")
    spark.stop()

if __name__ == "__main__":
    main()