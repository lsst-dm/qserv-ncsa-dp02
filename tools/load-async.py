import json as json
from multiprocessing import Process, Queue
import os
import random
import requests
import sys

masterBaseUrl = "http://127.0.0.1:25081"
authKey = "kukara4a"
database = "dp02_test_PREOPS863_00"

def info(message, log=sys.stdout):
    log.write("{}\n".format(message))
    log.flush()

def error(method, url, code, message, log):
    info("Error in method: {} url: {} http_code: {} server_error: {}".format(method, url, code, message), log)

def transactionBegin(database, log=sys.stdout):
    info("Starting transaction", log)
    url = "{}/ingest/trans".format(masterBaseUrl)
    data = {"auth_key":authKey,"database":database}
    response = requests.post(url, json=data, timeout=None)
    if response.status_code != 200:
        error("POST", url, response.status_code, "", log)
        sys.exit(1)
    responseJson = response.json()
    if not responseJson['success']:
        error("POST", url, response.status_code, responseJson["error"], log)
        sys.exit(1)
    return responseJson["databases"][database]["transactions"][0]["id"]

def transactionEnd(transactionId, abort, log=sys.stdout):
    # TODO: do not commit transactions yet
    return
    info("Ending transaction: id={} abort={}".format(transactionId, abort), log)
    url = "{}/ingest/trans/{}?abort={}".format(masterBaseUrl, transactionId, abort)
    data = {"auth_key":authKey}
    response = requests.put(url, json=data, timeout=None)
    if response.status_code != 200:
        error("PUT", url, response.status_code, "", log)
        sys.exit(1)
    responseJson = response.json()
    if not responseJson['success']:
        error("PUT", url, response.status_code, responseJson["error"], log)
        sys.exit(1)

def proc(jobs, database, failedJobs):
    with open("loader_logs/loader-{}.log".format(os.getpid()), "w") as log:
        while True:

            info("pid: {}, pulling a job from the input queue..".format(os.getpid()), log)
            job = jobs.get()
            if job is None: break
            url = job["url"]
            success = False
            retry = 0
            while retry < 2:

                info("pid: {}, job: {}, retry: {}".format(os.getpid(), str(job), retry), log)
                try:
                    response = requests.post(url, json=job["data"], timeout=None)
                    if response.status_code != 200:
                        error("POST", url, response.status_code, "pid: {}".format(os.getpid()), log)
                        break

                    responseJson = response.json()
                    success = responseJson['success'] != 0
                    if not success:
                        error("POST", url, response.status_code, str(responseJson["error"]) + ", pid: {}".format(os.getpid()), log)
                        if 'extended_err' in responseJson:
                            if 'retry_allowed' in responseJson['extended_err']:
                                if responseJson['extended_err']['retry_allowed'] != 0:
                                    retry = retry + 1
                                    info("pid: {}, job: {}, retry: {}, retrying...".format(os.getpid(), str(job), retry), log)
                                    continue

                except requests.exceptions.RequestException as ex:
                    error("POST", url, 0, str(ex) + ", pid: {}".format(os.getpid()), log)

                break

            if not success:
                info("pid: {}, )ob: {}, retry: {}, reporting as failed".format(os.getpid(), str(job), retry), log)
                failedJobs.put(job)
                info("pid: {}, job: {}, retry: {}, reported as failed".format(os.getpid(), str(job), retry), log)

        info("pid: {}, is finishing...".format(os.getpid()), log)

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

    usage = "usage: <num-trans> <num-proc> <cache-file>"

    if len(sys.argv) != 4:
        info(usage)
        sys.exit(1)

    numTrans = int(sys.argv[1])
    if numTrans < 1:
        info("error: the number of transactions must be 1 or higher")
        sys.exit(1)

    numProc = int(sys.argv[2])
    if numProc < 1:
        info("error: the number of processes must be 1 or higher")
        sys.exit(1)

    cacheFileName = sys.argv[3]

    info("Reading locations of input files from JSON file '{}'".format(cacheFileName))
    with open(cacheFileName, "r") as f:
        files = json.loads(f.read())

    chunks = set()
    for f in files:
        chunks.add(file2chunk(f["url"]))

    info("Allocating chunks");

    url = "{}/ingest/chunks".format(masterBaseUrl)
    data = {"database":database,
            "chunks":[c for c in chunks],
            "auth_key":authKey}
    response = requests.post(url, json=data, timeout=None)
    responseJson = response.json()
    if not responseJson['success']:
        info("error: {}".format(responseJson["error"]))
        sys.exit(1)

    chunk2location = {}
    for location in responseJson["location"]:
        chunk2location[location["chunk"]] = location

    info("Starting transactions");

    transactions = []
    for i in range(0, numTrans):
        transactions.append(transactionBegin(database))

    info("Preparing a collection of jobs")

    # Shuffle the list of files because entries are ordered by tables
    random.shuffle(files)

    transIdx = 0
    jobsList = []
    for f in files:
        table = f["table"]
        file_url = f["url"]
        chunk = file2chunk(file_url)
        location = chunk2location[chunk]
        overlap = file2overlap(file_url)
        job = {"url":"http://{}:{}/ingest/file-async".format(location["http_host"], location["http_port"]),
               "data":{"auth_key":authKey,
                       "transaction_id":transactions[transIdx % numTrans],
                       "table":table,
                       "column_separator":",",
                       "chunk":chunk,
                       "overlap":overlap,
                       "url":file_url}}
        jobsList.append(job)
        transIdx = transIdx + 1

    info("Loading data")

    info("  - Starting processes")
    jobs = Queue()
    failedJobs = Queue()
    processes = [Process(target=proc, args=(jobs, database, failedJobs)) for i in range(0, numProc)]
    for p in processes:
        p.start()

    info("  - Feeding jobs into the queue")
    for job in jobsList:
        jobs.put(job)

    info("  - Feeding terminators into the queue")
    for p in processes:
        jobs.put(None)

    info("  - Waiting for the processes to finish")
    for p in processes:
        p.join()
    jobs.close()

    info("  - Checking for failed jobs")
    transactionsToAbort = set()
    while True:
        try:
            job = failedJobs.get_nowait()
            info("      job: {}".format(str(job)))
            transactionsToAbort.add(job['data']['transactionId'])
        except:
            break

    info("  - Finishing transactions")
    for transactionId in transactions:
        if transactionId in transactionsToAbort:
            info("    ABORT  {}".format(transactionId))
            transactionEnd(transactionId, 1)
        else:
            info("    COMMIT {}".format(transactionId))
            transactionEnd(transactionId, 0)

    info("Done")
