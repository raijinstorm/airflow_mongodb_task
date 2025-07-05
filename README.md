# airflow_mongodb_task

# Only first time
```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

# Initialize DB
```
docker compose up airflow-init
```

# Run docker 
```
docker compose up -d
```

# Check UI

http://localhost:8080

login: airflow, pass: airflow


# Details
connections are set using .env file, for more details look example.env

```bash
cp example.env .env
```