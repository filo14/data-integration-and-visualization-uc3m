.PHONY: venv db backend

venv:
	@echo "--- Setting up python venv ---"
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

db:
	@echo "--- Setting up database ---"
	cd ./database && make up schema

etl: venv db
	@echo "--- Running ETL ---"
	.venv/bin/python crime_immigration_etl.py

delete: 
	@echo "--- Deleting & resetting database ---"
	cd ./database && make down

backup:
	@echo "--- Creating database backup ---"
	cd ./database && make backup

connect:
	@echo "--- Connecting to database ---"
	cd ./database && make connect

backend: venv
	@echo "--- Starting backend server ---"
	cd backend && ../.venv/bin/uvicorn main:app --reload

plots: venv
	@echo "--- Generating plots ---"
	cd map-making && ../.venv/bin/python plots.py