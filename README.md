# otus_mlops_airflow

## Запуск сервисов Airflow

[ссылка на оригинальную документацию](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) 

1. Сперва необходино установить переменную окружнния с идентификатором прользователя, от которого стартанут контейнеры. Выполните:

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

1. Теперь инициализируем базу данных:

    ```bash
    docker compose up airflow-init
    ```
  
1. И запускаем airflow:

    ```bash
    docker compose up
    ```