import csv
import time
import hashlib
import random
import logging
from io import StringIO, BytesIO

from boto3 import resource  # type: ignore
import pandas as pd  # type: ignore


AWS_ACCESS_KEY_ID = 'YCAJEZBqrd9eCpbuwP9Naze_F'
AWS_SECRET_ACCESS_KEY = 'YCOIypzb_j8gMOb8Sunvo-raaxK2v07ybmgZTBUr'
TARGET_YEAR = 2000
PATH_TO_DATA = 'https://raw.githubusercontent.com/peter-and-wolf/otus_mlops_airflow/refs/heads/main/data/GlobalLandTemperaturesByCountry.csv'
S3_ENDPOINT_URL = 'http://storage.yandexcloud.net'
S3_BUCKET_NAME = 'bucket-43'


def generate_key() -> str:
  return hashlib.md5(f'{random.random()}{time.time()}'.encode('utf-8')).hexdigest() + '.csv'


def extract_data(path: str, year: int = TARGET_YEAR) -> str:
  logging.info(f'Extracting data from {path} for year {year}')
  df = pd.read_csv(path)
  df['Year'] = pd.to_datetime(df['dt'], format="%Y-%m-%d").dt.year
  return df[df['Year'] >= year].reset_index().to_csv()


def transform_data(data: str) -> str:
  logging.info(f'Transforming data')
  df = pd.read_csv(StringIO(data))
  gb = df.groupby('Country')['AverageTemperature'] 
  return gb.median().reset_index().to_csv()


def load_data(data: str) -> None:
  logging.info(f'Loading data to S3')
  s3 = resource(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
  )
  bucket = s3.Bucket(S3_BUCKET_NAME)
  bucket.upload_fileobj(
    BytesIO(bytes(data, encoding='utf-8')),
    f'temperature/{generate_key()}'
  )


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  load_data(
    transform_data(
      extract_data(PATH_TO_DATA)
    )
  )
  logging.info(f'done...')
