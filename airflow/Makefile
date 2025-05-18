dirs:
	rm -rf dags logs plugins config
	mkdir -p dags logs plugins config

init:
	echo -e "AIRFLOW_UID=$(id -u)" > .env
	docker compose up airflow-init

start:
	docker compose up

stop:
	docker compose down -v

clean: stop
	docker compose down --volumes --rmi all
	rm -rf dags logs plugins config
