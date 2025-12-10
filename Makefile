.PHONY: venv db etl delete backup connect backend frontend install_frontend plots

venv:
	@echo "--- Setting up python venv ---"
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

db: venv
	@echo "--- Setting up database ---"
	cd ./database && make up schema

etl: db
	@echo "--- Running ETL ---"
	.venv/bin/python crime_immigration_etl.py

delete: 
	@echo "--- Deleting & resetting database ---"
	cd ./database && make down

backup: db
	@echo "--- Creating database backup ---"
	cd ./database && make backup

connect: db
	@echo "--- Connecting to database ---"
	cd ./database && make connect

backend: etl
	@echo "--- Starting backend server ---"
	cd backend && ../.venv/bin/uvicorn main:app --reload

install_frontend:
	@echo "--- Installing frontend dependencies ---"
	cd frontend && npm install

frontend: install_frontend
	@echo "--- Starting frontend server ---"
	cd frontend && npm run dev

plots: venv
	@echo "--- Generating plots ---"
	cd map-making && ../.venv/bin/python plots.py