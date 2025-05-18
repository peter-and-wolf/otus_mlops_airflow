# otus_mlops_airflow

## Запуск сервисов Airflow

[ссылка на оригинальную документацию](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) 

В директории [airflow](airflow):

1. Создайте директории, которые будут монтироваться в volumes docker-compose.

    ```bash
    make dirs
    ```

1. Инициализируйте базу данных airflow:

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