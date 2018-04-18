# -*- coding: utf-8 -*-
__author__ = 'John Berlin (n0tan3rd@gmail.com)'
__version__ = '1.0.0'
__copyright__ = 'Copyright (c) 2018 John Berlin'
__license__ = 'MIT'

import argparse
import csv
import os
import re
import sys
import time
import ujson as json
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse, urljoin

import requests
from goldfinch import validFileName as vfn
from requests_futures.sessions import FuturesSession

DEFAULT_DL_URL = 'http://localhost:1208'
"""Default memgator url"""

NO_SCHEME = re.compile('^https?://')
"""Helper regex to test if a URL is schemeless"""


def eprint(*args, **kwargs):
    """Helper function for printing to standard error"""
    print(*args, file=sys.stderr, **kwargs)


class MemgatorAliveException(Exception):
    """Helper Exception to inform users of an error during alive check"""
    pass


def check_memgator(memurl):
    """
    Checks that the memegator instance we will be talking to is up and running.
    Executes a HTTP head requests to memurl.
    Also if the memurl is schemeless this method will add http:// to it
    :param str memurl: The base memgator URL to be used
    :return str: The memgator URL after
    :raise MemgatorUnreachableError: If the head requests did not receive an
    HTTP 200 response or some other error occurred
    """
    mu_re = re.compile('/timemap(?:/[a-z]{4})?$')
    if not mu_re.search(memurl):
        memurl = memurl + '/timemap'

    if not NO_SCHEME.match(memurl):
        memurl = 'http://' + memurl

    parsed = urlparse(memurl)
    check_url = parsed.scheme + '://' + parsed.netloc

    try:
        req = requests.head(check_url)
        req.raise_for_status()
    except requests.exceptions.RequestException as e:
        msg = 'An exception occurred while making an HTTP head requests to ' \
              'memgator instance at %s. %s' % (memurl, str(e))
        raise MemgatorAliveException(msg)

    return memurl


def read_plaintext(textfile_path):
    """
    Generator that reads a plain text file containing URLs, one per line, yielding each URL
    :param str textfile_path: Path to plain text file containing URLs
    """
    with open(textfile_path, 'r') as textin:
        for line in textin:
            yield line.rstrip()


def read_csv(csv_path, key):
    """
    Generator that reads a csv file containing URLs, yielding each URL found using key
    :param str csv_path: Path to plain csv file containing URLs
    :param str key: CSV key for the URLs
    """
    with open(csv_path, 'r') as csvin:
        for row in csv.DictReader(csvin):
            yield row[key]


def read_json(json_path):
    """
    Generator that reads a json file containing a list of URLs, yielding each URL found
    :param str json_path: Path to plain json file containing URLs in a list
    """
    with open(json_path, 'r') as jsonin:
        for url in json.load(jsonin):
            yield url


def make_list_reader(args):
    """
    Creates the correct URL list reader based on the list files extension
    :param args: Args object returned from argparse
    :return: Generator yielding the URLs for TimeMap retrieval
    """
    url_file = args.urls
    _, ext = os.path.splitext(url_file)
    if ext == '.txt':
        url_generator = read_plaintext(url_file)
    elif ext == '.csv':
        if not args.key:
            raise ValueError('The URL list is a csv file but the -k or --key '
                             'argument was not supplied')
        url_generator = read_csv(url_file, args.key)
    elif ext == '.json':
        url_generator = read_json(url_file)
    else:
        raise ValueError(
            'Invalid file extension %s, we do not know how to parse this file type' %
            ext)
    return url_generator


def file_name_path_creator(dump_dir, the_ext):
    """
    Creates a function that will generate the co
    :param str dump_dir: Path to the directory where the TimeMaps will be dumped
    :param str the_ext: The file extension for the to be saved TimeMaps
    :return: Function that creates TimeMap file name for the supplied url
    """

    def creator(url):
        """
        Creates the full path to TimeMap bases on save_dir, url and args configuration
        :param str url: URL to be filenamified
        :return: Path to TimeMap
        """
        sanity = vfn(url.replace('://', '_').replace('/', '_'),
                     initCap=False).decode("utf-8")
        return os.path.join(dump_dir, sanity + the_ext)

    return creator


def make_requests(murl, urls, file_path, session):
    """
    Makes the requests to the running memgator instance
    :param murl: The URL to the running memgator instance
    :param urls: The generator yielding URLs for TimeMap retrieval
    :param file_path: Function returning the correct TimeMap file path
    :param session: The FuturesSession object to be used
    """
    pending = []
    for request_url in urls:
        if not NO_SCHEME.match(request_url):
            request_url = 'http://' + request_url
        pending.append(
            (session.get(
                '%s/%s' %
                (murl, request_url)), request_url))
    for future, request_url in pending:
        try:
            response = future.result()
            if response.status_code == 200:
                with open(file_path(request_url), 'w') as out:
                    out.write(response.text)
            else:
                eprint(
                    'The url %s did not get HTTP 200. It got %d' %
                    (request_url, response.status_code))
        except Exception as e:
            eprint('The url %s got an exception' % request_url)
            eprint(e)


def batch_dl(murl, nrequests, workers, file_path, url_generator):
    """
    Batch download TimeMaps from Memgator
    :param str murl: URL to the running Memgator instance
    :param int nrequests: How many requests should be made at a time
    :param int workers: How many workers should be spawned
    :param file_path: Function returning the file path for the TimeMap from the URL
    :param url_generator: Generator that generates URLs
    """
    with FuturesSession(session=requests.Session(),
                        executor=ProcessPoolExecutor(max_workers=workers)) as session:
        processed = 0
        temp = []
        for url in url_generator:
            temp.append(url)
            if len(temp) == nrequests:
                make_requests(murl, temp, file_path, session)
                processed += nrequests
                temp.clear()
                print('Processed %d URLs' % processed)
                print('----------------------------------------')
                if processed % 60 == 0:
                    time.sleep(25)
                else:
                    time.sleep(5)
        remaining = len(temp)
        if remaining > 0:
            make_requests(murl, temp, file_path, session)
            processed += remaining
        print('Finished processing %d URLs' % processed)
        print('----------------------------------------')


def main():
    """The main method"""
    parser = argparse.ArgumentParser(
        prog='download',
        description='Bulk download TimeMaps '
                    'using a local memgator instance')
    parser.add_argument(
        '-m',
        '--memurl',
        help='URL for running memgator instance. '
             'Defaults to http://localhost:1208',
        default=DEFAULT_DL_URL,
        type=str)
    parser.add_argument(
        '-w',
        '--workers',
        help='Max number of worker processes spawned. '
             'Defaults to 5',
        default=5,
        type=int)
    parser.add_argument(
        '-r',
        '--requests',
        help='How many requests should be queued per chunk. '
             'Defaults to 10',
        default=10,
        type=int)
    parser.add_argument(
        '-d',
        '--dump',
        help='Directory to dump the TimeMaps in. '
             'Defaults to <cwd>/timemaps',
        default='timemaps',
        type=str)
    parser.add_argument(
        '-u', '--urls', help='Path to file (.txt, .csv, .json) containing list of '
                             'URLs. File type detected by considering extension. '
                             'If .csv must supply -k <key> so we know where to get '
                             'the url', required=True)
    parser.add_argument('-k', '--key', help='The csv key for the urls')
    format_group = parser.add_mutually_exclusive_group()
    format_group.add_argument(
        '-j',
        '--json',
        help='Download TimeMaps in json format. Default format',
        action='store_true')
    format_group.add_argument('-l', '--link',
                              help='Download TimeMaps in link format',
                              action='store_true')
    format_group.add_argument('-c', '--cdxj',
                              help='Download TimeMaps in cdxj format',
                              action='store_true')

    args = parser.parse_args()
    memurl = check_memgator(args.memurl)

    if args.link:
        murl = '%s/%s' % (memurl, 'link') \
            if not memurl.endswith('link') else memurl
        ext = '.link'
    elif args.cdxj:
        murl = '%s/%s' % (memurl, 'cdxj') \
            if not memurl.endswith('cdxj') else memurl
        ext = '.cdxj'
    else:
        murl = '%s/%s' % (memurl, 'json') \
            if not memurl.endswith('json') else memurl
        ext = '.json'

    if not os.path.exists(args.dump):
        os.makedirs(args.dump, exist_ok=True)

    url_generator = make_list_reader(args)
    fp_function = file_name_path_creator(args.dump, ext)
    batch_dl(murl, args.requests, args.workers, fp_function, url_generator)


if __name__ == '__main__':
    main()
