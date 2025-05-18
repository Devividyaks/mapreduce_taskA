#!/usr/bin/env python3
"""
mapreduce_taskA.py

Task A: Determine the passenger(s) having had the highest number of flights
using a MapReduce-style prototype in pure Python.
"""

import csv
import sys
import multiprocessing as mp
import numpy as np

# 1) MAP + LOCAL COMBINE
def map_chunk(passenger_ids):
    """
    Count occurrences of each passenger_id in this chunk.
    Returns a dict: passenger_id -> local count.
    """
    counts = {}
    for pid in passenger_ids:
        counts[pid] = counts.get(pid, 0) + 1
    return counts

# 2) SHUFFLE
def shuffle(all_counts):
    """
    Merge a list of local-counts dicts into
    passenger_id -> list of partial counts.
    """
    shuffled = {}
    for local in all_counts:
        for pid, cnt in local.items():
            shuffled.setdefault(pid, []).append(cnt)
    return shuffled

# 3) REDUCE
def reduce_counts(item):
    """
    Sum up the list of counts for one passenger_id.
    Input: (passenger_id, [c1, c2, ...])
    Output: (passenger_id, total_count)
    """
    pid, counts = item
    return pid, sum(counts)

# 4) CHUNKING HELPER
def chunkify(data_list, n_chunks):
    """
    Split a list into n_chunks as evenly as possible.
    Uses numpy.array_split under the hood. :contentReference[oaicite:0]{index=0}:contentReference[oaicite:1]{index=1}
    """
    return np.array_split(data_list, n_chunks)

def main(input_csv):
    # ————————
    # A) FILE READING :contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}
    passenger_ids = []
    try:
        with open(input_csv, newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                # Skip empty/comment lines
                if not row or row[0].startswith('#'):
                    continue
                passenger_ids.append(row[0])
    except Exception as e:
        print(f"Error reading '{input_csv}': {e}")
        sys.exit(1)

    if not passenger_ids:
        print("No data found in input.")
        return

    # ————————
    # B) DETERMINE CHUNKS :contentReference[oaicite:4]{index=4}:contentReference[oaicite:5]{index=5}
    cpu_count = mp.cpu_count()
    num_chunks = min(cpu_count, len(passenger_ids))
    chunks = chunkify(passenger_ids, num_chunks)

    # ————————
    # C) MAP + COMBINE (parallel)
    with mp.Pool(processes=cpu_count) as pool:
        intermediate = pool.map(map_chunk, chunks)

    # ————————
    # D) SHUFFLE (single-process)
    shuffled = shuffle(intermediate)

    # ————————
    # E) REDUCE (parallel)
    with mp.Pool(processes=cpu_count) as pool:
        reduced = pool.map(reduce_counts, shuffled.items())

    # ————————
    # F) FIND HIGHEST-FLYER(S)
    totals = dict(reduced)
    max_flights = max(totals.values())
    top_passengers = [pid for pid, cnt in totals.items() if cnt == max_flights]

    # ————————
    # G) OUTPUT
    print("Passenger(s) with the highest number of flights:")
    print(f"  → {max_flights} flight(s)\n")
    for pid in top_passengers:
        print(f"    • {pid}")

if __name__ == '__main__':
    # ensures multiprocessing can import this module correctly :contentReference[oaicite:6]{index=6}:contentReference[oaicite:7]{index=7}
    if len(sys.argv) != 2:
        print("Usage: python mapreduce_taskA.py AComp_Passenger_data_no_error.csv")
        sys.exit(1)
    main(sys.argv[1])
