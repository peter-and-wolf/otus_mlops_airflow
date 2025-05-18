dirs:
	rm -rf dags logs plugins config
	mkdir -p dags logs plugins config

init:
	docker compose up airflow-init

start:
	docker compose up

stop:
	docker compose down -v

all:
	make dirs
	make init
	make start
	make stop

clean:
	make stop
	docker compose down --volumes --rmi all
	rm -rf dags logs plugins config
