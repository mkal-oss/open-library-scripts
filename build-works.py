#! /bin/env python3
import csv
import json
import logging
import humanize
import time
import sys

if __name__ == '__main__':
    start = time.perf_counter()
    csv.field_size_limit(sys.maxsize)
    file_to_parse = 'ol_dump_works_2020-11-30.txt'
    # logging.basicConfig(level=logging.INFO,
    #                         format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
    #                         filename=f'{file_to_parse}.log',
    #                         filemode='a')

    logging.info(f"Starting processing: {file_to_parse}")
    all_data = {}
    limit = 100
    i = 0
    with open(file_to_parse, 'r') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in reader:
            # print(len(row))
            # print('|'.join(row))
            data = json.loads(row[4])
            id = data['key'].split('/')[-1]
            authors = [a['author']['key'].split('/')[-1] for a in data.get('authors', [])]
            # print(f'[{id}]: %s' % '|'.join(authors))
            i += 1
            if i >= limit:
                exit(1)

            # if len(row) != 5:
            #     logging.error("wrong row length for: %s" % '\t'.join(row))
            #     continue

            # try:
            #     id = None
            #     data = json.loads(row[4])
            #     id = data['key'].split('/')[-1]
            #     all_data[id] = {
            #         'name': data['name'].encode('ascii', 'ignore').decode('ascii')
            #     }
            #     if 'personal_name' in data:
            #         all_data[id]['personal_name'] = data['personal_name'].encode('ascii', 'ignore').decode('ascii')
            # except Exception as ex:
            #     if id is not None:
            #         logging.error(f'[{id}]: {ex}')
            #     else:
            #         logging.error(ex)

    logging.info(len(all_data.keys()))
    # with open(f'{file_to_parse}.json', 'w') as fp:
    #     json.dump(all_data, fp)
    
    end = time.perf_counter()
    logging.info(f"Total time: {end - start:0.4f} seconds")
    logging.info(f"Total time: {humanize.naturaltime(end - start)}")