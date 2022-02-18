import json
import requests
import sys

database = "dp02_test_PREOPS863"

fileIdx = 0
def ls(source_database, table, folder):
    global fileIdx
    hosts = [
        "https://ccnetlsst01.in2p3.fr:65101",
        "https://ccnetlsst02.in2p3.fr:65101",
        "https://ccnetlsst03.in2p3.fr:65101",
        "https://ccnetlsst04.in2p3.fr:65101"]
    files = []
    base_url = "{}/{}/".format(hosts[0], source_database)
    if folder[-1:] != "/": folder = "{}/".format(folder)
    url = "{}{}".format(base_url, folder)
    headers = {"accept":"application/json"}
    response = requests.get(url, headers=headers, timeout=None)
    response.raise_for_status()
    responseJson = response.json()
    for e in responseJson:
        name = "{}{}".format(folder, e["name"])
        if not (e["is_dir"] or e["is_symlink"]):
            if name[-4:] == ".txt":
                files.append({"table":table,"url":"{}/{}/{}".format(hosts[fileIdx % 4], source_database, name)})
                fileIdx = fileIdx + 1
        if e["is_dir"]:
            files = files + ls(source_database, table, name)
    return files

tables = (("Object", "objectTable_tract",),
          ("Source", "sourceTable_visit",),
          ("ForcedSource", "forcedSourceTable",),
          ("DiaObject", "diaObjectTable_tract",),
          ("DiaSource", "diaSourceTable_tract",),
          ("ForcedSourceOnDiaObject", "forcedSourceOnDiaObjectTable_tract",),
         )
files = []
for table in tables:
    files = files + ls(database, table[0], table[1])
print(json.dumps(files))
