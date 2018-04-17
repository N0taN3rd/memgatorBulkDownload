# Memgator Bulk TimeMap Downloader

 Have you ever had a need to bulk download lets say 1 million TimeMaps using [oduwsdl/memgator](https://github.com/oduwsdl/memgator)?

With the caveat that it must be done in a timely manner?

If so then you are in luck because this project has you covered.

# Usage
**Requires python 3**

Be sure to install the dependencies first

```[sudo] pip install -r requirements.txt```

Basic usage
```
$ python download.py -u {MGURL} {FORMAT2} -d {DUMDIR} -l {LIST}
# MGURL   => http://localhost:1208/timemap/{FORMAT}
# FORMAT  => link|json|cdxj
# FORMAT2 => (-n, --link)|(-j, --json)|(-c, --cdxj)
# DUMDIR  => Path to directory where timemaps will be dumped
# LIST    => Path to URL list
```
To see all options supported execute
```
$ python download.py --help
```

# License
MIT
