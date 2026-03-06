install:
	pip install -r requirements.txt

demo:
	python main.py demo

dbt-run:
	cd dbt_project && dbt run --target prod

dbt-test:
	cd dbt_project && dbt test --target prod

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve

test:
	pytest tests/ -v --cov=src
