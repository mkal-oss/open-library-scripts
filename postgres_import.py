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


stuff = [
    ('/type/author', 8219164),
    ('/type/edition', 29542254),
    ('/type/redirect', 699331),
    ('/type/work', 21531431),
]

VALID_TYPES = [
    'author',
    'edition',
    'redirect',
    'work'
]


def define_entities(db):
    class OLWork(db.Entity):
        key = PrimaryKey(str, auto=False)
        title = Optional(str, nullable=True)
        subtitle = Optional(str, nullable=True)
        description = Optional(Json, nullable=True)
        original_languages = Optional(StrArray, nullable=True)
        other_titles = Optional(StrArray, nullable=True)
        first_publish_date = Optional(str, nullable=True)
        notes = Optional(str, nullable=True)
        cover_edition = Optional(Json, nullable=True)

        authors = Optional(Json, nullable=True)
        translated_titles = Optional(StrArray, nullable=True)
        subjects = Optional(StrArray, nullable=True)
        subject_places = Optional(StrArray, nullable=True)
        subject_times = Optional(StrArray, nullable=True)
        subject_people = Optional(StrArray, nullable=True)


        @classmethod
        def from_dict(cls, kwargs_dict):
            attrs = dir(cls)
            keys = list(kwargs_dict.keys())
            for k in keys:
                if k not in attrs:
                    del kwargs_dict[k]
            cls(**kwargs_dict)


    class OLEdition(db.Entity):
        key = PrimaryKey(str, auto=False)
        title = Optional(str, nullable=True)
        title_prefix = Optional(str, nullable=True)
        subtitle = Optional(str, nullable=True)
        other_titles = Optional(StrArray, nullable=True)

        # authors[] of type /type/author_role
        publish_date = Optional(str, nullable=True)
        copyright_date = Optional(str, nullable=True)
        edition_name = Optional(str, nullable=True)
        languages = Optional(Json, nullable=True)
        description = Optional(Json, nullable=True)
        notes = Optional(Json, nullable=True)
        genres = Optional(StrArray, nullable=True)

        work_titles = Optional(StrArray, nullable=True)

        series = Optional(StrArray, nullable=True)
        subjects = Optional(StrArray, nullable=True)
        contributions = Optional(StrArray, nullable=True)
        publish_places = Optional(StrArray, nullable=True)
        publish_country = Optional(StrArray, nullable=True)
        publishers = Optional(StrArray, nullable=True)
        distributors = Optional(StrArray, nullable=True)
        location = Optional(StrArray, nullable=True)
        works = Optional(Json, nullable=True)


        @classmethod
        def from_dict(cls, kwargs_dict):
            attrs = dir(cls)
            keys = list(kwargs_dict.keys())
            for k in keys:
                if k not in attrs:
                    del kwargs_dict[k]
            cls(**kwargs_dict)


    class OLRedirect(db.Entity):
        key = PrimaryKey(str, auto=False)
        location = Required(str)


        @classmethod
        def from_dict(cls, kwargs_dict):
            attrs = dir(cls)
            keys = list(kwargs_dict.keys())
            for k in keys:
                if k not in attrs:
                    del kwargs_dict[k]
            cls(**kwargs_dict)


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


def old_main():
    start_line = int(sys.argv[1])
    end_line = start_line + int(sys.argv[2]) - 1
    pbar_idx = int(sys.argv[3])
    print(f'{start_line} -> {end_line}')

    start = time.perf_counter()
    csv.field_size_limit(2147483647)

    db.bind(provider='sqlite', filename=os.path.join('openlibrary', f'ol-{start_line}-{end_line}.sqlite'), create_db=True)
    db.generate_mapping(create_tables=True)

    file_to_parse = os.path.join('openlibrary', 'ol_dump_2020-11-30.txt')
    with db_session:
        pbar = tqdm(total=61320756, position=pbar_idx + 1)
        logging.info(f"Starting processing: {file_to_parse}")
        save_limit = 10000
        with open(file_to_parse, 'r', encoding='utf-8') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                if pbar.n < start_line:
                    pbar.update(1)
                    continue
                if pbar.n > end_line:
                    commit()
                    break

                type = row[0].split('/')[-1]
                if type not in VALID_TYPES:
                    pbar.update(1)
                    continue

                data = json.loads(row[4])

                try:
                    if type == 'author':
                        OLAuthor.from_dict(data)
                    elif type == 'redirect':
                        OLRedirect.from_dict(data)
                    elif type == 'edition':
                        OLEdition.from_dict(data)
                    elif type == 'work':
                        OLWork.from_dict(data)
                    else:
                        # shouldnt have gotten here
                        pbar.update(1)
                        continue
                except:
                    print(f'[{type}: {json.dumps(data)}')

                # commit()
                pbar.update(1)
                if (pbar.n % save_limit) == 0:
                    commit()
                    gc.collect()
        commit()
        pbar.close()

        end = time.perf_counter()
        logging.info(f"Total time: {end - start:0.4f} seconds")
        logging.info(f"Total time: {humanize.naturaltime(end - start)}")


if __name__ == '__main__':
    start_line = int(sys.argv[1])
    end_line = start_line + 1000000 - 1
    sqlite_db = os.path.join('openlibrary', f'ol-{start_line}-{end_line}.sqlite')

    dst_db = Database()
    define_entities(dst_db)
    dst_db.bind(provider='postgres', user='', password='', host='localhost', database='openlibrary')
    dst_db.generate_mapping(create_tables=True)

    start = time.perf_counter()
    # start_line = 0
    # end_line = 1799999

    # start_line = 60800000
    src_db = Database()
    count = 0
    save_limit = 10000
    define_entities(src_db)
    src_db.bind(provider='sqlite', filename=sqlite_db, create_db=True)
    src_db.generate_mapping(create_tables=True)
    with db_session:
        # print(count(u for u in dst_db.OLAuthor))
        dst_tables = [dst_db.OLAuthor, dst_db.OLRedirect, dst_db.OLWork, dst_db.OLEdition]
        src_tables = [src_db.OLAuthor, src_db.OLRedirect, src_db.OLWork, src_db.OLEdition]
        for src_thing, dst_thing in zip(src_tables, dst_tables):
            table_total = select(u for u in src_thing).count()
            # select(author for author in OLAuthor).cout()
            pbar = tqdm(total=table_total, desc=src_thing._table_)
            for obj in select(u for u in src_thing):
                dst_thing.from_dict(obj.to_dict())
                pbar.update(1)
                if (pbar.n % save_limit) == 0:
                    commit()
                    gc.collect()

            pbar.close()
        commit()
        end = time.perf_counter()
        logging.info(f"Total time: {end - start:0.4f} seconds")
        logging.info(f"Total time: {humanize.naturaltime(end - start)}")
