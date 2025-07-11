#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from csv import DictWriter

def main(input_file: Path, output_file: Path):
    inf = input_file.open()
    outf = output_file.open("w")
    first_line = json.loads(next(inf))

    writer = DictWriter(outf, fieldnames=first_line.keys(), lineterminator="\n")
    writer.writeheader()
    writer.writerow(first_line)

    for line in inf:
        writer.writerow(json.loads(line))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path, help="Input JSONL file to convert")
    parser.add_argument("csv", type=Path, help="Output CSV file to write converted data to")
    args = parser.parse_args()
    main(args.jsonl, args.csv)