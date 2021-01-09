import os
import subprocess
import concurrent.futures
import time
from multiprocessing.dummy import freeze_support
from pprint import pprint
import humanize

from multiprocessing import Process, Manager, Lock, Queue, Pool

from tqdm import tqdm


PROCESS_LINE_COUNT = 1000000


def mp_import_db_old(tuple):
    start, idx = tuple
    cmd = [
        os.path.join('venv', 'scripts', 'python'),
        'import_to_sqlite.py',
        str(start),
        str(PROCESS_LINE_COUNT),
        str(idx)
    ]
    print(' '.join(cmd))
    subprocess.run(
        cmd
    )

def mp_import_db(tuple):
    start, idx = tuple
    if start > 60000000:
        return
    cmd = [
        os.path.join('venv', 'scripts', 'python'),
        'postgres_import.py',
        str(start),
    ]
    print(' '.join(cmd))
    subprocess.run(
        cmd
    )


if __name__ == '__main__':
    freeze_support()
    TOTAL_LINE_COUNT = 61320756
    starting_lines = list(range(21800000, TOTAL_LINE_COUNT, PROCESS_LINE_COUNT))
    tuples = (list(zip(starting_lines, range(0, len(starting_lines)))))
    workers = 2
    print(f"Starting processing of {len(starting_lines)} tasks with {workers} workers")
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        r = list(tqdm(executor.map(mp_import_db, tuples), total=len(starting_lines), position=0))
    end = time.perf_counter()
    print(f"Total time: {end - start:0.4f} seconds")
    print(f"Total time: {humanize.naturaltime(end - start)}")

    # for starting_line in starting_lines:
    #     cmd = [
    #         os.path.join('venv', 'scripts', 'python'),
    #         'import_to_sqlite.py',
    #         str(starting_line),
    #         str(PROCESS_LINE_COUNT)
    #     ]
    #     print(' '.join(cmd))
    #     subprocess.run(
    #         cmd
    #     )
