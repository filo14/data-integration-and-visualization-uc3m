# Database

## Start and fill Database with Schema

```bash
make up schema
```

## Connecting to Database with Python

```python
import psycopg2

# Parameters defined in compose.yaml
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
```

## Makefile commands

- Start docker database container

    ```bash
    make up
    ```

- Stop docker database container

    ```bash
    make down
    ```

- Apply database schema

    ```bash
    make schema
    ```

- Create datbase backup

    ```bash
    make backup
    ```

- Enter database shell

    ```bash
    make connect
    ```

- Clean backup folder

    ```bash
    make clean
    ```