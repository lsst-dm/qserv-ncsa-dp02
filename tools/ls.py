import requests
import sys

base_url = "https://ccnetlsst01.in2p3.fr:65101/"
headers = {"accept":"application/json"}

def ls(folder, print_header, recursive):
    if folder[-1:] != "/": folder = "{}/".format(folder)
    if print_header:
        header    = "         Size  Modified          File"
        separator = "   ----------  ----------------  ----------------------------------------------------"
        print(header)
        print(separator)
    url = "{}{}".format(base_url, folder)
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    responseJson = response.json()
    for e in responseJson:
        fileType = " "
        if e["is_dir"]: fileType = "D"
        if e["is_symlink"]: fileType = "L"
        size = e["size"]
        modTime = e["mod_time"]
        ymd = modTime[:10]
        hm = modTime[11:16]
        name = "{}{}".format(folder, e["name"])
        print("{}  {:10d}  {:9} {:5}  {}".format(fileType, size, ymd, hm, name))
        if e["is_dir"] and recursive:
            ls(name, False, recursive)
    if print_header:
        print(separator)

if __name__ == "__main__":
    usage = "Usage: [-H] [-r] [<folder> ... <folder>]"
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    print_header = False
    recursive = False
    folders = []
    for arg in sys.argv[1:]:
        if arg == "-H": print_header = True
        elif arg == "-r": recursive = True
        else: folders.append(arg) 

    if len(folders) == 0:
        print("error: no folders found in the command line")
        print(usage)
        sys.exit(1)

    print("")
    for folder in folders:
        ls(folder, print_header, recursive)
        print_header = False       
    print("")
