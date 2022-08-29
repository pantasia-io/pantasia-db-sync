# pantasia-db-sync

Copies data from cardano-db-sync database and transforms them for Pantasia app's usage

# Configuration (.env)

Rename ```.env.template``` file to ```.env```, then configure the values accordingly.

- ```PANTASIA_DB``` prefix is for configuring connection settings to Pantasia Postgres DB.
- ```PANTASIA_CDB``` prefix is for configuring connection settings to Cardano-Db-Sync Postgres DB.
- ```PANTASIA_IN_MEMORY_INDEX``` Set to True to use fully in-memory index, or False to use minimal in-memory index for ID lookups and duplicate detection
- ```PANTASIA_TIME_INTERVAL``` sets the maximum time period that pantasia-db-sync will try to query for, in minutes.
- ```LOG_LEVEL``` sets the logging level. Use "INFO" for regular run, or "DEBUG" when debugging.

If these environment variables are not set in ```.env``` file or through other means, the configuration will default to values set in app/settings.py

# Docker

Run these commands to build and run the app in a docker container

```
docker build --tag pantasia-db-sync .\
docker run --env-file .env --net="host" -d --name pantasia-db-sync pantasia-db-sync:latest
```

# Docker-Compose

You can also use docker-compose to build and start the docker container with the following command

```
docker-compose --env-file .env -f docker-compose.yml up --build -d
```

# Backup/Restore

## Config
You will need these values to be in env vars:
```
PANTASIA_DB_HOST=localhost
PANTASIA_DB_PORT=5432
PANTASIA_DB_USER=<username>
PANTASIA_DB_PASS=<password>
PANTASIA_DB_NAME=pantasia
PANTASIA_DB_BACKUP_PATH=./backups/
```

Create a snapshot of the database:
```
python backup_restore.py --action backup --verbose true
```

Restore from a previous snapshot of the database:
```
python backup_restore.py --action restore --verbose true
```
This will search for snapshots/backup files in the backup path, and you will need to select which file you'd like to restore.
