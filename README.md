# Memgator Bulk TimeMap Downloader

 Have you ever had a need to download 100 or 1 million TimeMaps using [oduwsdl/memgator](https://github.com/oduwsdl/memgator)?

With the caveat that it must be done in a timely manner?

If so then you are in luck because this project has you covered.

# Requirements
**Requires python 3**

Be sure to install the dependencies first

- ```[sudo] pip install -r requirements.txt```

# Usage

#### Basic usage
```
$ python download.py -u {MGURL} {FORMAT2} -d {DUMDIR} -l {LIST}
# MGURL   => http://localhost:1208/timemap/{FORMAT}
# FORMAT  => link|json|cdxj
# FORMAT2 => (-n, --link)|(-j, --json)|(-c, --cdxj)
# DUMDIR  => Path to directory where timemaps will be dumped
# LIST    => Path to URL list
```

#### Full Usage
```
$ python download.py --help
usage: download [-h] [-u URL] [-w WORKERS] [-r REQUESTS] [-d DUMP] -l LIST
                [-k KEY] (-j | -n | -c)

Bulk download TimeMaps using a local memgator instance

optional arguments:
  -h, --help            show this help message and exit

  -u URL, --url URL     URL for running memgator instance.
                        Defaults to http://localhost:1208/timemap/json

  -w WORKERS, --workers WORKERS
                        Max number of worker processes spawned.
                        Defaults to 5

  -r REQUESTS, --requests REQUESTS
                        How many requests should be queued per chunk.
                        Defaults to 10

  -d DUMP, --dump DUMP  Directory to dump the TimeMaps in.
                        Defaults to <cwd>/timemaps

  -l LIST, --list LIST  Path to file (.txt, .csv, .json) containing list of URLs.
                        File type detected by considering extension.
                        If .csv must supply -k <key> so we know where to get the url

  -k KEY, --key KEY     The csv key for the urls

  -j, --json            Download TimeMaps in json format. Default format
  -n, --link            Download TimeMaps in link format
  -c, --cdxj            Download TimeMaps in cdxj format

```

#### URL List Format
- **.txt**: 1 URL per line
- **.csv**: Requires -k or --key {KEY} argument. _KEY_ is the csv column containing the URL
- **.json**: List of URLs  

# License
MIT
