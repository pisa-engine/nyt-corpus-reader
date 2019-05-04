#! /usr/bin/env python
"""
Ingest a list of .tar files into the MongoDB store.
"""

import argparse
import multiprocessing
import tarfile
from typing import Iterable, Dict, Any

from nytcorpusreader import NYTArticle

_XML_SUFFIX = '.xml'
_BULK_INSERT_SIZE = 1000


def get_documents(tar_path: str) -> Iterable[NYTArticle]:
    with tarfile.open(tar_path, 'r:*') as tar:
        for inner_file in tar:
            filename = inner_file.name
            # Skip everything but the articles
            if not filename.endswith(_XML_SUFFIX):
                continue

            content = tar.extractfile(inner_file).read().decode('utf8')
            article = NYTArticle.from_str(content)
            yield article


def process_path(tar_path: str) -> None:
    for article in get_documents(tar_path):
        if article:
            print(article.docid, end='\t')
            print(article.title, end=' ')
            for par in article.paragraphs:
                print(par, end=' ')
            print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('list', type=argparse.FileType('r', encoding='utf8'),
                        help='A file containing a list of tar files to ingest')
    args = parser.parse_args()

    files = [line.strip() for line in args.list]
    args.list.close()
    if not files:
        raise ValueError('No files in input file list')

    for filename in files:
        process_path(filename)

if __name__ == '__main__':
    main()
