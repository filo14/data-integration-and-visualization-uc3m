.PHONY: venv db

venv:
	@echo "--- Setting up python venv ---"
	python3 -m venv .venv
	source .venv/bin/activate
	pip install -r requirements.txt

db:
	@echo "--- Setting up database ---"
	cd ./database && make up schema