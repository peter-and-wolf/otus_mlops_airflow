import logging
from io import StringIO, BytesIO
from datetime import timedelta

from boto3 import resource  # type: ignore
import pandas as pd  # type: ignore

from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore


PATH_TO_DATA = 'https://raw.githubusercontent.com/peter-and-wolf/otus_mlops_airflow/refs/heads/main/data/GlobalLandTemperaturesByCountry.csv'
TARGET_COUNTRIES = ['Sweden', 'Norway', 'Danmark']


default_args = {
    "owner": "peter-and-wolf",
    "depends_on_past": False,
    "start_date": "2025-05-18",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_data(path: str, countries: list[str] = TARGET_COUNTRIES) -> str:
  logging.info(f'Extracting data from {path}')
  df = pd.read_csv(path)
  return df[df['Country'].isin(countries)].reset_index().to_csv()


def calc_median(data: str, country: str, **kwargs) -> float:
  logging.info(f'Caclulating median temperature for {country}')
  
  # get data from extract_data task using xcom_pull
  ti = kwargs['ti']
  data = ti.xcom_pull(task_ids='calc_median')
  
  df = pd.read_csv(StringIO(data))
  return float(df[df['Country'] == country].groupby('Country')['AverageTemperature'].median().iloc[0])


def calc_min_temp(**kwargs) -> str:
  logging.info(f'Calculating minimum median temperature')
  
  # get data from extract_data task using xcom_pull
  ti = kwargs['ti']
  data = ti.xcom_pull(task_ids=[
    'calc_median_sweden',
    'calc_median_norway',
    'calc_median_danmark',
  ])
  
  logging.info(f'Minimum median temperature: {min(data)}')
  return min(data)


with DAG(
  'min_temp_calc',
  default_args=default_args,
  description="DAG to calculate the minimum median temperature",
  schedule=timedelta(days=1),
  catchup=False,
  tags=['calc'],
) as dag:
  
  extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={'path': PATH_TO_DATA},
  )

  calc_task1 = PythonOperator(
    task_id='calc_median_sweden',
    python_callable=calc_median,
    op_kwargs={'country': 'Sweden'},
  )

  calc_task2 = PythonOperator(
    task_id='calc_median_norway',
    python_callable=calc_median,
    op_kwargs={'country': 'Norway'},
  )

  calc_task3 = PythonOperator(
    task_id='calc_median_danmark',
    python_callable=calc_median,
    op_kwargs={'country': 'Danmark'},
  )

  calc_min_task = PythonOperator(
    task_id='calc_min_temp',
    python_callable=calc_min_temp,
  )

  extract_task >> [calc_task1, calc_task2, calc_task3] >> calc_min_task
