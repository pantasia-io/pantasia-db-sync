# pantasia-db-sync
Copies data from cardano-db-sync database and transforms them for Pantasia app's usage

# Docker
Run these commands to build and run the app in a docker container
```
docker build --tag pantasia-db-sync .\
docker run --net="host" -d --name pantasia-db-sync pantasia-db-sync:latest
