# ! /bin/env python3
import csv
import gc
import json
import logging
import math
import sqlite3

import humanize
import time
import sys
import os
from tqdm import tqdm
import glob
import csv
import os
import time

from pony.orm import Database, Json, Optional, PrimaryKey, Required, Set, StrArray, commit, count, db_session, select
from collections import namedtuple
import gender_guesser.detector as gender


db = Database()


class OLAuthor(db.Entity):
    key = PrimaryKey(str, auto=False)
    name = Optional(str, nullable=True)
    personal_name = Optional(str, nullable=True)
    title = Optional(str, nullable=True)
    # bio = Optional(Json, nullable=True)
    location = Optional(str, nullable=True)
    birth_date = Optional(str, nullable=True)
    death_date = Optional(str, nullable=True)
    date = Optional(str, nullable=True)
    gender = Optional(str, nullable=True)
    wikipedia = Optional(str, nullable=True)

    alternate_names = Optional(StrArray, nullable=True)


    # links = Optional(StrArray, nullable=True)


    @classmethod
    def from_dict(cls, kwargs_dict):
        attrs = dir(cls)
        keys = list(kwargs_dict.keys())
        for k in keys:
            if k not in attrs:
                del kwargs_dict[k]
        cls(**kwargs_dict)


def gender_guess_db(db_path):
    CHUNK_SIZE = 10000
    chunk_idx = 0
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    total_rows = int(cursor.execute("SELECT count(*) FROM OLAuthor WHERE gender is null and name is not null").fetchone()[0])
    expected_chunks = math.ceil(total_rows / float(CHUNK_SIZE))
    pbar = tqdm(total=total_rows, desc=os.path.basename(db_path))
    res = cursor.execute("SELECT * FROM OLAuthor WHERE gender is null and name is not null")
    while True:
        chunk = res.fetchmany(CHUNK_SIZE)
        if not chunk:
            break

        for row in chunk:
            genders = []
            genders.append(d.get_gender(row[1].split(' ')[0]))
            if row[2] is not None:
                genders.append(d.get_gender(row[2].split(' ')[0]))
            if row[9] is not None:
                try:
                    alternate_names = json.loads(row[9])
                    for alternate_name in alternate_names:
                        d.get_gender(alternate_name.split(' ')[0])
                except:
                    pass
            connection.cursor().execute(f"UPDATE OLAuthor SET gender = ? WHERE key = ?", (json.dumps(genders), row[0]))
            connection.commit()
            pbar.update(1)
    pbar.close()


if __name__ == '__main__':
    d = gender.Detector(case_sensitive=False)
    db_paths = sorted(glob.glob(os.path.join('openlibrary', f'ol-*-*.sqlite')))

    # db.bind(provider='sqlite', filename=db_path, create_db=True)
    # db.generate_mapping(create_tables=True)
    for db in db_paths:
        gender_guess_db(db)
