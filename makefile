install:
	pip install -r requeriments.txt
run:
	mkdir -p ~/airflow/dags
	cp dag.py ~/airflow/dags
	airflow standalone