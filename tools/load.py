import json as json
from multiprocessing import Process, Queue
import os
import requests
import sys

masterBaseUrl = "http://localhost:25081"
authKey = "kukara4a"

def error(log, method, url, code, message):
    log.write("Error in method: {} url: {} http_code: {} server_error: {}\n".format(method, url, code, message))

def transactionBegin(log, database):
    log.write("Starting transaction\n")
    url = "{}/ingest/trans".format(masterBaseUrl)
    data = {"auth_key":authKey,"database":database}
    response = requests.post(url, json=data)
    if response.status_code != 200:
        error(log, "POST", url, response.status_code, "")
        sys.exit(1)
    responseJson = response.json()
    if not responseJson['success']:
        error(log, "POST", url, response.status_code, responseJson["error"])
        sys.exit(1)
    return responseJson["databases"][database]["transactions"][0]["id"]

def transactionEnd(log, transactionId, abort=0):
    log.write("Ending transaction: id={} abort={}\n".format(transactionId, abort))
    url = "{}/ingest/trans/{}?abort={}".format(masterBaseUrl, transactionId, abort)
    data = {"auth_key":authKey}
    response = requests.put(url, json=data)
    if response.status_code != 200:
        error(log, "PUT", url, response.status_code, "")
        sys.exit(1)
    responseJson = response.json()
    if not responseJson['success']:
        error(log, "PUT", url, response.status_code, responseJson["error"])
        sys.exit(1)

def proc(jobs, database):
    transactionId = None
    with open("loader_logs/loader-{}.log".format(os.getpid()), "w") as log:
        transactionId = transactionBegin(log, database)
        while True:
            job = jobs.get()
            if job is None: return
            job["data"]["transaction_id"] = transactionId
            log.write("job: {}\n".format(str(job)))
            response = requests.post(job["url"], json=job["data"])
            if response.status_code != 200:
                error(log, "POST", url, response.status_code, "")
                sys.exit(1)
            responseJson = response.json()
            if not responseJson['success']:
                error(log, "POST", url, response.status_code, )
                sys.exit(1)
                log.write("error: {}\n".format(responseJson["error"]))
                abort = 1
                transactionEnd(log, transactionId, abort)
                return
    if transactionId is not None:
        transactionEnd(log, transactionId)

def file2chunk(f):
    fname = f.split("/")[-1][0:-4][6:]
    if len(fname) > len("_overlap") and "_overlap" == fname[-8:]:
        return int(fname[0:-8])
    else:
        return int(fname)

def file2overlap(f):
    fname = f.split("/")[-1][0:-4][6:]
    if len(fname) > len("_overlap") and "_overlap" == fname[-8:]:
        return 1
    else:
        return 0

if __name__ == '__main__':

    table = "Object"
    if len(sys.argv) != 4:
        print("usage: <database> <files-list> <num-proc>")
        sys.exit(1)
    database = sys.argv[1]
    filesList = sys.argv[2]
    numProc = int(sys.argv[3])

    print("Reading locations of input files")

    files = []
    with open(filesList, "r") as f:
        files = [line[:-1] for line in f]

    chunks = set()
    for f in files:
        chunks.add(file2chunk(f))

    print("Allocating chunks");

    url = "{}/ingest/chunks".format(masterBaseUrl)
    data = {"database":database,
            "chunks":[c for c in chunks],
            "auth_key":authKey}
    response = requests.post(url, json=data)
    responseJson = response.json()
    if not responseJson['success']:
        print("error: {}".format(responseJson["error"]))
        sys.exit(1)

    chunk2location = {}
    for location in responseJson["location"]:
        chunk2location[location["chunk"]] = location

    print("Preparing a collection of jobs")

    jobsList = []
    for f in files:
        chunk = file2chunk(f)
        location = chunk2location[chunk]
        overlap = file2overlap(f)
        job = {"url":"http://{}:{}/ingest/file".format(location["http_host"], location["http_port"]),
               "data":{"auth_key":authKey,
                       "transaction_id":None,
                       "table":table,
                       "column_separator":",",
                       "chunk":chunk,
                       "overlap":overlap,
                       "url":"file://{}".format(f)}}
        jobsList.append(job)

    print("Loading data")

    print("  - Starting processes")
    jobs = Queue()
    processes = [Process(target=proc, args=(jobs, database)) for i in range(0, numProc)]
    for p in processes:
        p.start()

    print("  - Feeding jobs into the queue")
    for job in jobsList:
        jobs.put(job)

    print("  - Feeding terminators into the queue")
    for p in processes:
        jobs.put(None)

    print("  - Waiting for the processes to finish")
    for p in processes:
        p.join()
    jobs.close()

    print("Done")
