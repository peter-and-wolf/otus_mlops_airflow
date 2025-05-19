# otus_mlops_airflow

## Запуск сервисов Airflow

[ссылка на оригинальную документацию](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) 

В директории [airflow](airflow):

1. Инициализируйте необходимые параметры (создаст директории, установит переменную AIRFLOW_UID, инициализирует БД) airflow:

    ```bash
    make init
    ```
  
1. Запустите все контейнеры airflow:

    ```bash
    make start
    ```

## Остановка сервисов Airflow

Остановить запущенные контейнеры можно так:

```bash
make stop
```

И прибрать за собой:

```bash
make clean
```