__author__ = 'John Berlin (n0tan3rd@gmail.com)'
__version__ = '1.0.0'
__copyright__ = 'Copyright (c) 2018-Present John Berlin'
__license__ = 'MIT'

from concurrent.futures import ProcessPoolExecutor
import argparse
import csv
import re
import time
from glob import glob
import os
import requests
from requests_futures.sessions import FuturesSession
import ujson as json


FILE_NAME_RE = re.compile('[:/.-]+')
NO_SCHEME_RE = re.compile('^http://')

UA_LIST = [
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.01',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'
    '54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Linux; Ubuntu 14.04) AppleWebKit/537.36 Chromium/35.0.1870.2 Safa'
    'ri/537.36',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.'
    '0.2228.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko'
    ') Chrome/42.0.2311.135 '
    'Safari/537.36 Edge/12.246',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, '
    'like Gecko) Version/9.0.2 Safari/601.3.9',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/47.0.2526.111 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
]
"""list[str]: user agent strings used when fetching the lists"""


DEFAULT_DL_URL = 'http://localhost:1208/timemap/%s/%s'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='download',
                                     description='Bulk download TimeMaps '
                                                 'using a local memgator instance')
    parser.add_argument('-u', '--url', help='URL for running memgator instance. '
                                            'Defaults to http://localhost:1208/timemap',
                        default=DEFAULT_DL_URL, type=str)
    parser.add_argument('-w', '--workers', help='Max number of worker processes spawned. '
                                                'Defaults to 10',
                        default=10, type=int)
    format_group = parser.add_mutually_exclusive_group(required=True)
    format_group.add_argument('-j', '--json',
                              help='Download TimeMaps in json format',
                              default=True, action='store_true')
    format_group.add_argument('-l', '--link',
                              help='Download TimeMaps in link format',
                              action='store_false')
    timeMapDir = '10k_timemaps'
    uaLen = len(UA_LIST)
    c = 0
    processed = 0
    temp = []
    with open('alexaUrls/10k_fromLatestCrawl.csv', 'r') as urlIN:
        with FuturesSession(session=requests.Session(), executor=ProcessPoolExecutor(max_workers=10)) as session:
            for row in csv.DictReader(urlIN):
                if not os.path.exists(
                        '%s/%s.json' % (timeMapDir, '%s-%s' % (FILE_NAME_RE.sub('', row['url']), row['rank']))):
                    temp.append(row)
                else:
                    print('exists', row)
                    continue

                if len(temp) >= 20:
                    pending = []
                    for r in temp:
                        pending.append(
                            (session.get(qurl % NO_SCHEME_RE.sub('', r['url']).rstrip('/')),
                             '%s/%s.json' % (timeMapDir, '%s-%s' %
                                             (FILE_NAME_RE.sub('', r['url']), r['rank'])),
                             r['url']))
                    for future, fpath, url in pending:
                        try:
                            response = future.result()
                            if response.status_code == 200:
                                with open(fpath, 'w') as out:
                                    out.write(response.text)
                            else:
                                print('baddd', url)
                        except Exception as e:
                            print(e, url)
                    processed += 20
                    temp.clear()
                    print('process %d' % processed)
                    print('----------------------------------------')
                    if processed % 60 == 0:
                        time.sleep(25)
                    else:
                        time.sleep(5)
