#! /bin/env python3
import csv
import gc
import json
import logging
import humanize
import time
import sys
import os
from tqdm import tqdm

import csv
import os
import time

from pony.orm import Database, Json, Optional, PrimaryKey, Required, Set, StrArray, commit, count, db_session, select
from collections import namedtuple

from postgres_import import define_entities


if __name__ == '__main__':
    csv.field_size_limit(2147483647)

    author_genders = {}
    with open('author-genders.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            author_genders[row[0]] = row[-1]

    authors_works = {}
    with open('author-works.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[-1] in author_genders:
                authors_works[row[0]] = author_genders[row[-1]]


    edition_works = {}
    with open('edition-works.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)

        with open('edition-genders.csv', 'w', encoding='utf-8', newline='') as newcsv:
            writer = csv.writer(newcsv)

            for row in reader:
                if row[-1] in authors_works:
                    new_row = list(row)
                    new_row[-1] = authors_works[row[-1]]
                    writer.writerow(new_row)