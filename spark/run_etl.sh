#!/bin/bash
set -e

find_spark_submit() {
  candidates=("/spark/bin/spark-submit" "/opt/spark/bin/spark-submit" "/usr/bin/spark-submit")
  for p in "${candidates[@]}"; do
    if [ -x "$p" ]; then
      echo "$p"
      return 0
    fi
  done
  if command -v spark-submit >/dev/null 2>&1; then
    command -v spark-submit
    return 0
  fi
  return 1
}

SPARK_SUBMIT="$(find_spark_submit || true)"
if [ -z "$SPARK_SUBMIT" ]; then
  echo "ОШИБКА: spark-submit не найден. Проверьте установку Spark." >&2
  exit 2
fi

PACKAGES="org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6"

run_job1() {
    echo -e "\nЗапуск: Job1 данные -> звезда (PostgreSQL)"
    "$SPARK_SUBMIT" \
        --packages "$PACKAGES" \
        --master local[*] \
        --conf spark.sql.legacy.timeParserPolicy=LEGACY \
        --conf spark.driver.memory=2g \
        /opt/spark/work-dir/etl_to_postgres.py
}

run_job2() {
    echo -e "\nЗапуск: Job2 звезда -> ClickHouse"
    "$SPARK_SUBMIT" \
        --packages "$PACKAGES" \
        --master local[*] \
        --conf spark.driver.memory=2g \
        /opt/spark/work-dir/job2_star_to_clickhouse.py
}

run_job1
run_job2

echo -e "\nDone!"