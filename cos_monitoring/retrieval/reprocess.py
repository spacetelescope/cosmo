#! /usr/bin/env python
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data", nargs="+",  help="Reprocess programs or datasets")
    args = parser.parse_args()

    print(args.data)

