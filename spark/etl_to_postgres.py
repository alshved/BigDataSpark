from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("ETL_Pet_Sales_Snowflake") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pg_url = "jdbc:postgresql://postgres:5432/mockdb"
    pg_props = {
        "user": "spark",
        "password": "sparkpass",
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "mock_data") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    df_dates = df.select(F.col("sale_date").alias("full_date")) \
        .where(F.col("full_date").isNotNull()) \
        .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")) \
        .withColumn("year", F.year("full_date")) \
        .withColumn("quarter", F.quarter("full_date")) \
        .withColumn("month", F.month("full_date")) \
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM")) \
        .withColumn("day", F.dayofmonth("full_date")) \
        .withColumn("day_of_week", F.dayofweek("full_date")) \
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE")) \
        .select("date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name") \
        .dropDuplicates(["date_key"])

    df_dates.write.jdbc(url=pg_url, table="dim_date", mode="overwrite", properties=pg_props)
    print("dim_date записано успешно")

    df_cust = df.select(
        F.col("sale_customer_id").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed")
    ).dropDuplicates(["customer_id"])

    df_cust.write.jdbc(url=pg_url, table="dim_customer", mode="overwrite", properties=pg_props)
    print("dim_customer записано успешно")

    df_seller = df.select(
        F.col("sale_seller_id").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code")
    ).dropDuplicates(["seller_id"])

    df_seller.write.jdbc(url=pg_url, table="dim_seller", mode="overwrite", properties=pg_props)
    print("dim_seller записано успешно")

    df_prod = df.select(
        F.col("sale_product_id").alias("product_id"),
        F.col("product_name"),
        F.col("product_category"),
        F.col("pet_category"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_weight").alias("weight"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews")
    ).dropDuplicates(["product_id"])

    df_prod.write.jdbc(url=pg_url, table="dim_product", mode="overwrite", properties=pg_props)
    print("dim_product записано успешно")

    df_store = df.select(
        F.col("store_name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email")
    ).dropDuplicates(["store_name"])

    df_store.write.jdbc(url=pg_url, table="dim_store", mode="overwrite", properties=pg_props)
    print("dim_store записано успешно")

    df_sup = df.select(
        F.col("supplier_name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country")
    ).dropDuplicates(["supplier_name"])

    df_sup.write.jdbc(url=pg_url, table="dim_supplier", mode="overwrite", properties=pg_props)
    print("dim_supplier записано успешно")
    
    facts = df.where(F.col("sale_date").isNotNull()) \
        .withColumn("sale_date_key", F.date_format(F.col("sale_date"), "yyyyMMdd").cast("int")) \
        .select(
            F.col("sale_date_key"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            F.col("store_name"),
            F.col("supplier_name"),
            F.col("sale_quantity"),
            F.col("sale_total_price"),
            F.col("product_price").alias("unit_price"),
            F.col("product_quantity").alias("product_quantity_at_sale")
        )

    facts.write.jdbc(url=pg_url, table="fact_sales", mode="overwrite", properties=pg_props)
    print("fact_sales записано успешно")

    spark.stop()

if __name__ == "__main__":
    main()