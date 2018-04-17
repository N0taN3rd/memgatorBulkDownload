# -*- coding: utf-8 -*-
__author__ = 'John Berlin (n0tan3rd@gmail.com)'
__version__ = '1.0.0'
__copyright__ = 'Copyright (c) 2018 John Berlin'
__license__ = 'MIT'

from concurrent.futures import ProcessPoolExecutor
import argparse
import csv
import re
import time
import os
import sys
import requests
from requests_futures.sessions import FuturesSession
import ujson as json
from goldfinch import validFileName as vfn

FILE_NAME_RE = re.compile('[:/.-]+')
NO_SCHEME_RE = re.compile('^http://')

DEFAULT_DL_URL = 'http://localhost:1208/timemap'


def eprint(*args, **kwargs):
    """Helper function for printing to standard error"""
    print(*args, file=sys.stderr, **kwargs)


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


def sane_filename(url):
    """
    Turns a url into a sane filename. Replaces :// and / with _
    then feeds url to goldfinch.validFileName
    :param str url: The url to turn into a valid filename
    :return str: A valid filename from the supplied url
    """
    return vfn(url.replace('://', '_').replace('/', '_'),
               initCap=False).decode("utf-8")


def file_name_path_creator(args):
    """
    Creates a function that will generate the co
    :param args: Args object returned from the arg parser
    :return: Function that creates TimeMap file name for the supplied url
    """
    if args.json:
        the_ext = '.json'
    elif args.link:
        the_ext = '.link'
    else:
        the_ext = '.cdxj'

    def creator(save_dir, url):
        """
        Creates the full path to TimeMap bases on save_dir, url and args configuration
        :param str save_dir: Path to the directory the TimeMaps are saved in
        :param str url: URL to be filenamified
        :return: Path to TimeMap
        """
        sanity = sane_filename(url)
        return os.path.join(save_dir, '%s.%s' % (sanity, the_ext))

    return creator


def batch_dl(args, file_path, url_generator):
    """
    Batch download TimeMaps from Memgator
    :param args: Args object returned from the arg parser
    :param file_path: Function returning the file path for the TimeMap from the URL
    :param url_generator: Generator that generates URLs
    """
    with FuturesSession(session=requests.Session(),
                        executor=ProcessPoolExecutor(max_workers=args.workers)) as session:
        processed = 0
        temp = []
        for url in url_generator:
            temp.append(url)
            if len(temp) == args.requests:
                pending = []
                for request_url in temp:
                    pending.append(
                        (session.get(
                            '%s/%s' %
                            (args.url, request_url)), request_url))
                for future, request_url in pending:
                    try:
                        response = future.result()
                        if response.status_code == 200:
                            with open(file_path(args.dump, request_url), 'w') as out:
                                out.write(response.text)
                        else:
                            eprint(
                                'The url %s did not get HTTP 200. It got %d' %
                                (request_url, response.status_code))
                    except Exception as e:
                        eprint('The url %s got an exception' % request_url)
                        eprint(e)
                processed += args.requests
                temp.clear()
                print('Processed %d URLs' % processed)
                print('----------------------------------------')
                if processed % 60 == 0:
                    time.sleep(25)
                else:
                    time.sleep(5)


def main():
    parser = argparse.ArgumentParser(prog='download',
                                     description='Bulk download TimeMaps '
                                                 'using a local memgator instance')
    parser.add_argument('-u', '--url', help='URL for running memgator instance. '
                                            'Defaults to http://localhost:1208/timemap/json',
                        default=DEFAULT_DL_URL, type=str)
    parser.add_argument('-w', '--workers', help='Max number of worker processes spawned. '
                                                'Defaults to 5',
                        default=5, type=int)
    parser.add_argument('-r', '--requests', help='How many requests should be queued per chunk. '
                                                 'Defaults to 10',
                        default=10, type=int)
    parser.add_argument('-d', '--dump', help='Directory to dump the TimeMaps in. '
                                             'Defaults to <cwd>/timemaps',
                        default='timemaps', type=str)
    parser.add_argument('-l', '--list', help='Path to file (.txt, .csv, .json) containing list of URLs. '
                                             'File type detected by considering extension. '
                                             'If .csv must supply -k <key> so we know where to get the url',
                        required=True)
    parser.add_argument('-k', '--key', help='The csv key for the urls')
    format_group = parser.add_mutually_exclusive_group(required=True)
    format_group.add_argument('-j', '--json',
                              help='Download TimeMaps in json format. Default format',
                              default=True, action='store_true')
    format_group.add_argument('-n', '--link',
                              help='Download TimeMaps in link format',
                              action='store_false')
    format_group.add_argument('-c', '--cdxj',
                              help='Download TimeMaps in cdxj format',
                              action='store_false')
    args = parser.parse_args()
    url_file = args.list
    _, ext = os.path.splitext(url_file)
    if ext == '.txt':
        url_generator = read_plaintext(url_file)
    elif ext == '.csv':
        url_generator = read_csv(url_file, args.key)
    elif ext == '.json':
        url_generator = read_json(url_file)
    else:
        raise ValueError(
            'Invalid file extension %s, we do not know how to parse this file type' %
            ext)
    if not os.path.exists(args.dump):
        os.makedirs(args.dump, exist_ok=True)
    fp_function = file_name_path_creator(args)
    batch_dl(args, fp_function, url_generator)


if __name__ == '__main__':
    main()
