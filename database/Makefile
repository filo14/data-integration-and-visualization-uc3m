# =================================================================
# PostgreSQL Management Makefile
# Standardizes common development tasks for the Docker setup.
# =================================================================

# Configuration Variables
# These must match the environment variables in docker-compose.yml
SERVICE := db
DB_NAME := mydb
DB_USER := myuser
BACKUP_DIR := .backup
SCHEMA_FILE := initdb/schema.sql
BACKUP_FILE=$(BACKUP_DIR)/$(DB_NAME)_$(shell date +'%Y%m%d_%H%M%S').sql.gz


# Phony targets don't correspond to actual files
.PHONY: up down schema backup connect clean

# --- Core Docker Compose Commands ---

up:
	@echo "--- Starting PostgreSQL container (db) in detached mode ---"
	docker compose up -d

down:
	@echo "--- Stopping and removing containers and volumes ---"
	# -v also removes the db-data volume, which is useful for a clean reset
	docker compose down -v

# --- Schema Management ---

schema: up
	@echo "--- Applying schema from $(SCHEMA_FILE) ---"
	# 1. Wait until the database is ready to accept connections (important after 'up')
	docker compose exec -T $(SERVICE) sh -c "until pg_isready -U $(DB_USER) -d $(DB_NAME); do echo 'Waiting for DB...'; sleep 3; done"
	# 2. Execute the schema file using the psql client inside the container
	docker compose exec -T $(SERVICE) psql -U $(DB_USER) -d $(DB_NAME) -f /docker-entrypoint-initdb.d/schema.sql
	@echo "Schema application complete."


# --- Backup and Utility ---

backup: up
	@echo "--- Creating gzipped backup of '$(DB_NAME)' database ---"
	mkdir -p $(BACKUP_DIR)
	# Generate a timestamped filename
	# Run pg_dump inside the container and pipe the output to a local compressed file
	docker compose exec -T $(SERVICE) pg_dump -U $(DB_USER) -d $(DB_NAME) | gzip > $(BACKUP_FILE)
	@echo "Successfully saved backup to $(BACKUP_FILE)"

connect: up
	@echo "--- Connecting to $(DB_NAME) via psql ---"
	docker compose exec $(SERVICE) psql -U $(DB_USER) -d $(DB_NAME)

clean: down
	@echo "--- Removing backup directory ---"
	rm -rf $(BACKUP_DIR)
