import multiprocessing
from multiprocessing import Process, Queue
import os
import requests
import time
import sys

base_url = "https://ccnetlsst01.in2p3.fr:65101/"

def make_folders(filename):
    import errno
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

def cp(in_file, out_file):
    size_bytes = 0
    url = "{}{}".format(base_url, in_file)
    headers = {}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    make_folders(out_file)
    with open(out_file, "wb") as f:
         chunk_size = 1024 * 1024
         for chunk in response.iter_content(chunk_size=chunk_size):
             f.write(chunk)
             size_bytes = size_bytes + len(chunk)
    return size_bytes

def ls(folder, recursive):
    result = {"folders": [], "links": [], "files": []}
    if folder[-1:] != "/": folder = "{}/".format(folder)
    url = "{}{}".format(base_url, folder)
    headers = {"accept":"application/json"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    responseJson = response.json()
    for e in responseJson:
        category = "files"
        if   e["is_dir"]: category = "folders"
        elif e["is_symlink"]: category = "links"
        result[category].append("{}{}".format(folder, e["name"]))
    if recursive:
        for subfolder in result["folders"]:
            subfolder_result = ls(subfolder, recursive)
            for category in result.keys():
                result[category] = result[category] + subfolder_result[category]
    return result

def proc(jobs, out_dir, log_dir, verbose):
    with open("{}{:d}.log".format(log_dir, os.getpid()), "w") as log:
        while True:
            in_file = jobs.get()
            if in_file is None: return
            out_file = "{}{}".format(out_dir, in_file)
            if verbose: log.write("GET {:s} -> {:s}\n".format(in_file, out_file))
            started = time.time()
            size_bytes = cp(in_file, out_file)
            finished = time.time()
            elapsed = finished - started
            perf_mbytes_s = 0
            if size_bytes > 0: perf_mbytes_s = size_bytes / (1024 * 1024) / elapsed
            if verbose:
                log.write("PERF file:{:s} size[bytes]:{:d} elapsed[s]:{:f} speed[MB/s]:{:f}\n".format(in_file, size_bytes, elapsed, perf_mbytes_s))

def proc_all(files, out_dir, log_dir, num_proc, verbose):
    jobs = Queue()
    processes = [Process(target=proc, args=(jobs, out_dir, log_dir, verbose)) for i in range(0, num_proc)]
    for p in processes:
        p.start()

    for filename in files:
        jobs.put(filename)

    # Feed terminators to stop processes
    for p in processes:
        jobs.put(None)

    for p in processes:
        p.join()
    jobs.close()

if __name__ == "__main__":
    usage = "Usage: <in-dir> [--out-dir=<folder>] [--log-dir=<folder>] [--num-proc=<num>] [-v] [-r] [-D]"
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    in_dir = sys.argv[1]
    out_dir = "./"
    log_dir = "./"
    num_proc = multiprocessing.cpu_count()
    recursive = False
    verbose = False
    debug = False
    for arg in sys.argv[2:]:
        if   arg == "-r": recursive = True
        elif arg == "-v": verbose = True
        elif arg == "-D": debug = True
        elif arg[:len("--out-dir=")] == "--out-dir=":
            if out_dir != "":
                out_dir = arg[len("--out-dir="):]
        elif arg[:len("--log-dir=")] == "--log-dir=":
            if log_dir != "":
                log_dir = arg[len("--log-dir="):]
        elif arg[:len("--num-proc=")] == "--num-proc=":
            num_proc = int(arg[len("--num-proc="):])
            if num_proc <= 0:
                print("error: --num-proc must be 1 or greater");
                print(usage)
                sys.exit(1)
        else:
            print("error: unknown option {}".format(arg))
            print(usage)
            sys.exit(1)

    if (out_dir != "./") and (out_dir[-1:] != "/"): out_dir = "{:s}/".format(out_dir)
    if (log_dir != "./") and (log_dir[-1:] != "/"): log_dir = "{:s}/".format(log_dir)

    if debug:
        print("\n[DEBUG] PARAMETERS:")
        print("  Base URL:  {:s}".format(base_url))
        print("  In.dir:    {:s}".format(in_dir))
        print("  Out.dir:   {:s}".format(out_dir))
        print("  Log.dir:   {:s}".format(log_dir))
        print("  Num.proc:  {:d}".format(num_proc))
        print("  Recursive: {:s}".format(str(recursive)))
        print("  Verbose:   {:s}".format(str(verbose)))
        print("  Debug:     {:s}".format(str(debug)))

    if verbose: print("\nPulling directory info on the input folder...")

    result = ls(in_dir, recursive)

    if verbose: print("\nProcessing results...")
    if debug:
        for category in result.keys():
            print("\n[DEBUG] {:s}:".format(category.upper()))
            for path in result[category]:
                print("  {:s}".format(path))
    else:
        if out_dir != "./": make_folders(out_dir)
        if log_dir != "./": make_folders(log_dir)
        proc_all(result["files"], out_dir, log_dir, num_proc, verbose)

