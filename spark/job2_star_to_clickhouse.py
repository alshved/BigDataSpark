from pyspark.sql import SparkSession, functions as F, types as T

PG_URL = "jdbc:postgresql://postgres:5432/mockdb"
PG_PROPS = {
    "user": "spark",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver",
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_USER = "spark"
CH_PASSWORD = "sparkpass"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

def get_clickhouse_connection(spark):
    jvm = spark._jvm
    thread = jvm.java.lang.Thread.currentThread()
    
    props = jvm.java.util.Properties()
    props.setProperty("user", CH_USER)
    props.setProperty("password", CH_PASSWORD)
    
    driver_class = thread.getContextClassLoader().loadClass(CH_DRIVER)
    driver_instance = driver_class.newInstance()
    
    conn = driver_instance.connect(CH_URL, props)
    if conn is None:
        raise Exception(f"Не удалось подключиться к ClickHouse по URL: {CH_URL}")
        
    return conn

def ensure_ch_table(spark, df, table_name):
    def to_ch_type(dt):
        if isinstance(dt, (T.DecimalType, T.DoubleType)): return "Float64"
        if isinstance(dt, T.IntegerType): return "Int32"
        if isinstance(dt, T.LongType): return "Int64"
        if isinstance(dt, T.TimestampType): return "DateTime"
        if isinstance(dt, T.DateType): return "Date"
        return "String"

    cols = ", ".join([f"{field.name} {to_ch_type(field.dataType)}" for field in df.schema])

    conn = get_clickhouse_connection(spark)
    try:
        stmt = conn.createStatement()
        stmt.execute(f"CREATE TABLE IF NOT EXISTS default.{table_name} ({cols}) ENGINE = MergeTree() ORDER BY tuple()")
        stmt.execute(f"TRUNCATE TABLE default.{table_name}")
        print(f"Таблица default.{table_name} успешно подготовлена.")
    except Exception as e:
        print(f"Ошибка JDBC при работе с ClickHouse: {e}")
        raise
    finally:
        if conn:
            conn.close()

def ch_write(spark, df, table_name):
    ensure_ch_table(spark, df, table_name)

    col_names = df.columns

    def write_partition(rows):
        try:
            import urllib.request

            rows_list = list(rows)
            if not rows_list:
                return

            def fmt(v):
                if v is None:
                    return "NULL"
                if isinstance(v, str):
                    return "'" + v.replace("'", "\\'") + "'"
                return str(v)

            values = ",".join(
                "(" + ",".join(fmt(getattr(r, c)) for c in col_names) + ")"
                for r in rows_list
            )
            query = f"INSERT INTO default.{table_name} ({','.join(col_names)}) VALUES {values}"

            data = query.encode("utf-8")
            req = urllib.request.Request(
                "http://clickhouse:8123/",
                data=data,
                headers={
                    "X-ClickHouse-User": "spark",
                    "X-ClickHouse-Key": "sparkpass",
                    "Content-Type": "text/plain",
                },
                method="POST",
            )
            with urllib.request.urlopen(req) as resp:
                resp.read()
        except Exception as e:
            raise RuntimeError(f"Ошибка записи в ClickHouse: {e}")

    df.foreachPartition(write_partition)


def main():
    spark = (
        SparkSession.builder
        .appName("Job2: Star to ClickHouse Full")
        .config("spark.sql.debug.maxToStringFields", "2000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    fact = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)
    dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)
    dim_store = spark.read.jdbc(url=PG_URL, table="dim_store", properties=PG_PROPS)
    dim_supplier = spark.read.jdbc(url=PG_URL, table="dim_supplier", properties=PG_PROPS)
    dim_date = spark.read.jdbc(url=PG_URL, table="dim_date", properties=PG_PROPS)

    fact = fact.withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
    fact = fact.withColumn("sale_quantity", F.col("sale_quantity").cast("long"))

    report_products = (
        fact.join(dim_product, "product_id", "inner")
        .groupBy("product_id", "product_name", "product_category")
        .agg(
            F.sum("sale_quantity").alias("total_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.first("rating").alias("avg_rating"),
            F.first("reviews").alias("reviews_count")
        )
    )
    ch_write(spark, report_products, "report_products")

    report_customers = (
        fact.join(dim_customer, "customer_id", "inner")
        .groupBy("customer_id", "first_name", "last_name", "country")
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.count("*").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_check"),
        )
    )
    ch_write(spark, report_customers, "report_customers")

    report_time_trends = (
        fact.join(dim_date, fact["sale_date_key"] == dim_date["date_key"], "inner")
        .groupBy("year", "month", "month_name")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_total_price").alias("avg_order_size"),
            F.count("*").alias("orders_count"),
        )
    )
    ch_write(spark, report_time_trends, "report_time_trends")

    report_stores = (
        fact.join(dim_store, "store_name", "inner")
        .groupBy("store_name", "city", "country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_total_price").alias("avg_check"),
            F.count("*").alias("orders_count"),
        )
    )
    ch_write(spark, report_stores, "report_stores")

    report_suppliers = (
        fact.join(dim_supplier, "supplier_name", "inner")
        .groupBy("supplier_name", "city", "country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("unit_price").alias("avg_price"),
            F.count("*").alias("orders_count"),
        )
    )
    ch_write(spark, report_suppliers, "report_suppliers")

    report_quality = (
        fact.join(dim_product, "product_id", "inner")
        .groupBy("product_id", "product_name", "rating", "reviews")
        .agg(
            F.sum("sale_quantity").alias("total_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
        )
    )
    ch_write(spark, report_quality, "report_quality")

    spark.stop()

if __name__ == "__main__":
    main()