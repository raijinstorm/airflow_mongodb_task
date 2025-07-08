# airflow_mongodb_task

## for Linux users
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## Initialize DB (for airflow ?)
```
docker compose up airflow-init
```

## Run docker 
```
docker compose up -d
```

## Check UI

http://localhost:8080

login: airflow, pass: airflow


<!-- ## Details
connections are set using .env file, for more details look example.env

```bash
cp example.env .env
``` -->

## You can connect to your MongoDB using Compass with this connection string
```
mongodb://root:example@localhost:27017/?authSource=admin&ssl=false
```

## Ideas to improve project.

Add config file (maybe just env variable), in this config the user can define if he want the file to be removed after it is readed 
Instead of saving temp csv files between tasks use temp folder, then clear it
Add labels to edges of DAG
Instead of local file system use S3