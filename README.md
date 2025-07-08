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


Ideas to improve project.

Add config file, in this config the user can define if he want the file to be removed after it is readed 
Add labels to edges of DAG
Instead of local file system use S3