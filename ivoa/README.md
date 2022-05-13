# The protocol for loaiding a subset of table `ObsCore` into Qserv instances in IDF

Note the following:
* the table has only about 88k rows
* one chunk of the complete `Object` table had to be loaded as well. Otherwise Qserv wouldn't work.

# Initialize GKE environment
```
gcloud container clusters get-credentials qserv-int --region us-central1 --project qserv-int-8069
gcloud container clusters get-credentials qserv-int --region us-central1 --project qserv-int-8069 && kubectl port-forward qserv-repl-ctl-0 8080:8080 >& http_8080.log&
```
# Compute the index of the last worker for the 'for' loops of this script
```
LAST_WORKER_IDX=$(($(kubectl get pods | grep qserv-worker | wc -l)-1))
echo "LAST_WORKER_IDX: "${LAST_WORKER_IDX}
```

# Creating the database
```
curl 'http://localhost:8080/ingest/database' -X POST -H 'Content-Type: application/json' -d@ivoa.json
```

# Registering the tables
```
curl 'http://localhost:8080/ingest/table' -X POST -H 'Content-Type: application/json' -d@Object.json
curl 'http://localhost:8080/ingest/table' -X POST -H 'Content-Type: application/json' -d@ObsCore.json
```

# Start the transaction
```
TID=$(curl 'http://localhost:8080/ingest/trans' -X POST -H 'Content-Type: application/json' -d'{"database":"ivoa","auth_key":""}' | sed 's/,/\n/g' | grep id | awk -F: '{print $2}')
echo "Started transaction: "${TID}
```

# Ingest the regular table. Note that the actual ingest will be made from inside
# the Replication Controller's container.
```
curl 'http://localhost:8080/ingest/regular' -X GET -H 'Content-Type: application/json' -d@ObsCore.json
kubectl cp obscore-slice.csv qserv-repl-ctl-0:obscore-slice.csv
kubectl exec -it qserv-repl-ctl-0 -- bash -c 'for worker in $(seq --format="qserv-worker-%01.0f.qserv-worker" 0 '${LAST_WORKER_IDX}'); do qserv-replica-file INGEST FILE ${worker} 25002 '${TID}' ObsCore R obscore-slice.csv --fields-enclosed-by="\"" --fields-terminated-by="," --verbose; done'
```

# Ingest one chunk of the director table. Note that the actual ingest will be made from inside
# the Replication Controller's container.
```
curl 'http://localhost:8080/ingest/chunk' -X POST -H 'Content-Type: application/json' -d'{"database":"ivoa","chunk":57866,"auth_key":""}';
kubectl cp chunk_57866.txt qserv-repl-ctl-0:chunk_57866.txt
kubectl exec -it qserv-repl-ctl-0 -- bash -c 'qserv-replica-file INGEST FILE qserv-worker-0.qserv-worker 25002 '${TID}' Object P chunk_57866.txt --fields-terminated-by="," --verbose'
```

# Commit transaction
```
curl 'http://localhost:8080/ingest/trans/'${TID}'?abort=0' -X PUT -H 'Content-Type: application/json' -d'{"database":"ivoa","auth_key":""}'
```

# Publish the catalog
```
curl 'http://localhost:8080/ingest/database/ivoa' -X PUT -H 'Content-Type: application/json' -d'{"auth_key":""}'
```

# Modify column type in all worker and czar databases.
```
for i in $(seq 0 ${LAST_WORKER_IDX}); do
  kubectl exec qserv-worker-${i} -c mariadb -- mysql -uqsmaster -e 'ALTER TABLE ivoa.ObsCore MODIFY s_region_bounds geometry NOT NULL';
done
kubectl exec qserv-czar-0 -c mariadb -- mysql -uqsmaster -e 'ALTER TABLE ivoa.ObsCore MODIFY s_region_bounds geometry NOT NULL'
```

# Create the spatial index
```
curl 'http://localhost:8080/replication/sql/index' -X POST -H 'Content-Type: application/json' -d@idx_ObsCore_s_region_bounds.json -oidx_ObsCore_s_region_bounds.json.result
```

# Try the catalog
```
kubectl exec -it qserv-czar-0 -c proxy -- mysql -h127.0.0.1 -P4040 -uqsmaster -e 'SHOW TABLES FROM ivoa'
kubectl exec -it qserv-czar-0 -c proxy -- mysql -h127.0.0.1 -P4040 -uqsmaster -e 'SELECT COUNT(*) FROM ivoa.ObsCore'
kubectl exec -it qserv-czar-0 -c proxy -- mysql -h127.0.0.1 -P4040 -uqsmaster -e 'SELECT s_region FROM ivoa.ObsCore WHERE CONTAINS(s_region_bounds, POINT(59.0, -37.1))=1 AND scisql_s2PtInCPoly(59, -37.1, s_region_scisql)=1'
```

