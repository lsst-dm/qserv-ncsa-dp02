# qserv-ncsa-dp02
Configuration files, scripts and instructions for ingesting DP02 catalogs into Qserv at NCSA

## Setting up the environment
Install required Python modules (sudo is required):
```
pip3 install requests
```
Create missing folders:
```
mkdir loader_logs
mkdir indexes_logs
```
## Cleaning up from the failed ingests (if needed to restart ingest from scratch)
Delete the database (if restaring from scratch):
```
curl 'http://localhost:25081/ingest/database/dp02_test_PREOPS863_00' -X DELETE -H 'Content-Type: application/json' -d'{"auth_key":""}'
```
## Ingesting
Start by creating the database in Qserv:
```
curl 'http://localhost:25081/ingest/database' -X POST -H 'Content-Type: application/json' -d@config/dp02_test_PREOPS863_00.json
```
The next step is to registering tables:
```
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/Object.json
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/Source.json
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/ForcedSource.json
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/DiaObject.json
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/DiaSource.json
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/ForcedSourceOnDiaObject.json
```
Initiate ASYNC requests for loading contributions (1 transaction, 16 sub-processes for submitting REST requests):
```
python3 load-async.py 1 16 config/filecache_dp02_test_PREOPS863_00.json >& loader_logs/load-async.log&
```
Commit transactions (replace 390 with the rigth one that was open by the loader):
```
curl 'http://localhost:25081/ingest/trans/390?abort=0' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}'
```
Publish the database:
```
curl 'http://localhost:25081/ingest/database/dp02_test_PREOPS863_00' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}'
```

Create table-level indexes:
```
for f in $(ls config_indexes); do
    curl 'http://localhost:25081/replication/sql/index' -X POST -H 'Content-Type: application/json' -d@config_indexes/${f} -oindexes_logs/${f}.result >& indexes_logs/${f}.log;
done
```

## Test results
```
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.Object"
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.Source"
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.ForcedSource"
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.DiaObject"
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.DiaSource"
mysql --protocol=tcp -P4040 -uqsmaster -e "SELECT COUNT(*) FROM dp02_test_PREOPS863_00.ForcedSourceOnDiaObject"
```

