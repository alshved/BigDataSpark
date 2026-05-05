#!/bin/bash
set -euo pipefail

PG_JAR="/opt/spark/jars/postgresql-42.7.10.jar"
CH_JAR="/opt/spark/jars/clickhouse-jdbc-all-0.9.8.jar"
SPARK_BIN="/opt/spark/bin/spark-submit"

submit_job() {
    local job_desc=$1
    local job_file=$2
    local jars=$3

    echo "Запуск: $job_desc"
    
    $SPARK_BIN \
        --master local[*] \
        --jars "$jars" \
        --conf spark.sql.legacy.timeParserPolicy=LEGACY \
        --conf spark.driver.memory=2g \
        --conf spark.log.level=WARN \
        "/opt/spark/work-dir/$job_file"
}

submit_job "Job1 (Raw -> Star Schema)" "job1_raw_to_star.py" "$PG_JAR"
submit_job "Job2 (Star Schema -> ClickHouse)" "job2_star_to_clickhouse.py" "$PG_JAR,$CH_JAR"

echo "Все задачи успешно завершены!"