install:
	pip install -r requeriments.txt

create_db:
	@sudo -u postgres psql -c "CREATE DATABASE airflowdb;"
	@sudo -u postgres psql -c "CREATE USER airflowuser WITH PASSWORD 'senha';"
	@sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflowuser;"

run:
	mkdir -p ~/airflow/dags
	cp dag.py ~/airflow/dags
	airflow standalone