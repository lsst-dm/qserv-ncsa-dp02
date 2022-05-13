# The protocol for loaiding the complete table `Object` into Qserv instances at NCSA

# Creating the database
```
curl 'http://localhost:25081/ingest/database' -X POST -H 'Content-Type: application/json' -d@config/dp02_dc2_catalogs.json
```

# Registering the tables
```
curl 'http://localhost:25081/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/Object.json
```
# Loading contributions
```
mkdir -p logs
python3 ../tools/load-async.py dp02_dc2_catalogs http://localhost:25081 <auth-key> 9 10 filecache_dp02_dc2_catalogs.json >& logs/load-async.log&
```

# Committing transactions (replace transaction numbers with the actual ones open by the loader tool)
```
for tid in $(seq 410 418); do
    curl 'http://localhost:25081/ingest/trans/'${tid}'?abort=0' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}';
done
```

# Publishing the database
```
curl 'http://localhost:25081/ingest/database/dp02_dc2_catalogs' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}'
```
# Creating table-level indexes
```
for f in $(ls config/ | grep idx_); do
    curl 'http://localhost:25081/replication/sql/index' -X POST -H 'Content-Type: application/json' -d@config/${f} -ologs/${f}.result >& logs/${f}.log;
done
```
