from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="orders_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice"],
) as dag:
    check_input = BashOperator(
        task_id="check_input",
        bash_command="test -f /opt/data/orders.csv && wc -l /opt/data/orders.csv",
    )

    spark_etl = SparkSubmitOperator(
        task_id="spark_etl",
        conn_id="spark_default",
        application="/opt/spark-jobs/etl_orders.py",
        name="orders_etl",
        conf={
            # executor trên spark-worker phải kết nối ngược lại driver trong container này
            "spark.driver.host": "airflow-scheduler",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
    )

    check_input >> spark_etl
