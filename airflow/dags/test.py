from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# "Hello World"를 출력하는 함수 정의
def print_hello():
    print("Hello World")

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 객체 생성
dag = DAG(
    'hello_world_dag',              # DAG 이름
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),  # 실행 간격 지정
    start_date=datetime(2023, 1, 1),      # 시작 날짜 지정
    catchup=False,                        # 과거 기간 배치 실행 여부
)

# 작업 생성
hello_operator = PythonOperator(
    task_id='print_hello',        # 작업 ID
    python_callable=print_hello,  # 실행할 함수
    dag=dag                       # 이 작업이 속할 DAG 지정
)

# DAG의 실행 순서를 정의할 필요가 없다면, 이 예제처럼 단일 작업만 있으면 된다.
