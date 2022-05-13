# The protocol for loaiding the complete table `Object` into Qserv instances in IDF
 
## Creating the database
```
curl 'http://localhost:8080/ingest/database' -X POST -H 'Content-Type: application/json' -d@config/dp02_dc2_catalogs.json
```

## Registering the tables
```
curl 'http://localhost:8080/ingest/table' -X POST -H 'Content-Type: application/json' -d@config/Object.json
```

## Loading contributions
```
kubectl cp ../tools/load-async.py qserv-repl-ctl-0:load-async.py
kubectl cp filecache_dp02_dc2_catalogs.IDF.json qserv-repl-ctl-0:filecache_dp02_dc2_catalogs.IDF.json
kubectl exec qserv-repl-ctl-0 -- bash -c 'mkdir -p logs'
kubectl exec qserv-repl-ctl-0 -- bash -c 'mkdir -p loader_logs'
kubectl exec qserv-repl-ctl-0 -- bash -c 'python3 ./load-async.py dp02_dc2_catalogs http://localhost:8080 "" 9 10 filecache_dp02_dc2_catalogs.IDF.json >& logs/load-async.log'
```

## Committing transactions (replace transaction nunbers with the actual ones open by the loader tool)
```
for tid in $(seq 17 25); do
    curl 'http://localhost:8080/ingest/trans/'${tid}'?abort=0' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}';
done
```

## Publish the database
```
curl 'http://localhost:8080/ingest/database/dp02_dc2_catalogs' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}'
```

## Create table-level indexes
```
for f in $(ls config/ | grep idx_); do
    echo $f;
    curl 'http://localhost:8080/replication/sql/index' -X POST -H 'Content-Type: application/json' -d@config/${f} -ologs/${f}.result >& logs/${f}.log;
done
```

## Scan row counters and deploy those at czar database
```
curl 'http://localhost:8080/ingest/table-stats' -X POST -H 'Content-Type: application/json' -d'{"auth_key":"","database":"dp02_dc2_catalogs","table":"Object","row_counters_state_update_policy":"ENABLED","row_counters_deploy_at_qserv":1,"force_rescan":1}'
```


