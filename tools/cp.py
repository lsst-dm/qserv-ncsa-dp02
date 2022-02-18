import time
import requests
import sys

base_url = "https://ccnetlsst01.in2p3.fr:65101/"
headers = {}

def make_folders(filename):
    import errno
    import os
    path_elements = filename.split("/")
    folder = None
    for subfolder in path_elements[:-1]:
        if folder is None: folder = subfolder
        else: folder = "{}/{}".format(folder, subfolder)
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

def cp(filename):
    size_bytes = 0
    url = "{}{}".format(base_url, filename)
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    make_folders(filename)
    with open(filename, "wb") as f:
         chunk_size = 1024 * 1024
         for chunk in response.iter_content(chunk_size=chunk_size):
             f.write(chunk)
             size_bytes = size_bytes + len(chunk)
    return size_bytes

if __name__ == "__main__":
    usage = "Usage: [-v] [<file> ... <file>]"
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    verbose = False
    files = []
    for arg in sys.argv[1:]:
        if arg == "-v": verbose = True
        else: files.append(arg) 

    if len(files) == 0:
        print("error: no files found in the command line")
        print(usage)
        sys.exit(1)

    for filename in files:
        started = time.time()
        size_bytes = cp(filename)
        finished = time.time()
        elapsed = finished - started
        perf_mbytes_s = 0
        if size_bytes > 0: perf_mbytes_s = size_bytes / (1024 * 1024) / elapsed
        if verbose:
            print("{}: {:d} bytes, {:f} seconds, {:f} MB/s".format(filename, size_bytes, elapsed, perf_mbytes_s))

