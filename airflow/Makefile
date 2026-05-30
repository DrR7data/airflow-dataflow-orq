header:
	@echo "For work with Airflow"
expor:
	@echo "Exporting environment variables..."
	echo 'PS1="> "' >> ~/.bashrc
uv:
	pip install uv
	python3 -m pip install --upgrade pip
	uv sync
	@echo "Activating virtual environment..."
	source .venv/bin/activate
mk:
	#mkdir -p ./dags ./logs ./plugins
	echo -e "AIRFLOW_UID=$(id -u)" > .env
up-dbec:
	@echo "Initialize the database..."
	sudo docker compose up airflow-init 

upec:
	sudo docker compose up -d 

downec:
	sudo docker compose down

up-db:
	@echo "Initialize the database..."
	docker compose up airflow-init 
up:
	docker compose up -d 
down:
	docker compose down

start:
	docker compose start

conn-pg:
	@echo "Connecting to Postgres..."
	uvx pgcli -h localhost -p 5434 -U flow -d flow
conn-air:
	@echo "Connecting to Postgres..."
	uvx pgcli -h localhost -p 5432 -U airflow -d airflow



