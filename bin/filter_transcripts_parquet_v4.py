#!/usr/bin/env python3

#v2 removes "UnassignedCodeword" transcripts from data

import argparse
import sys
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd


def main():
    args = parse_args()
    dataset = ds.dataset(args.transcript, format="parquet")
    fname = ds.field("feature_name")
    # build an Arrow expression combining all your filters
    expr = (
        (ds.field("qv") >= args.min_qv) &
        (ds.field("x_location") >= args.min_x) &
        (ds.field("x_location") <= args.max_x) &
        (ds.field("y_location") >= args.min_y) &
        (ds.field("y_location") <= args.max_y) &
        ~pc.match_substring_regex(fname, "^NegControlProbe_") &
        ~pc.match_substring_regex(fname, "^antisense_") &
        ~pc.match_substring_regex(fname, "^NegControlCodeword_") &
        ~pc.match_substring_regex(fname, "^UnassignedCodeword") &
        ~pc.match_substring_regex(fname, "^BLANK_")
    )

    scanner = dataset.scanner(
        filter=expr,
        batch_size=1_000_000
    )

    out_csv = f"X{args.min_x}-{args.max_x}_Y{args.min_y}-{args.max_y}_filtered_transcripts.csv"
    header = True
    with open(out_csv, 'w', newline='') as f:
        for batch in scanner.to_batches():
            df = batch.to_pandas()
            df['cell_id'] = df['cell_id'].replace({-1: '0', 'UNASSIGNED': '0'})
            df.to_csv(f, index=False, header=header)
            header = False


def parse_args():
    """Parses command-line options for main()."""
    summary = 'Filter transcripts from transcripts.csv based on Q-Score threshold \
               and upper bounds on x and y coordinates. Remove negative controls.'

    parser = argparse.ArgumentParser(description=summary)
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-transcript',
                               required = True,
                               help="The path to the transcripts.parquet file produced " +
                                    "by Xenium.")
    parser.add_argument('-min_qv',
                        default='20.0',
                        type=float,
                        help="The minimum Q-Score to pass filtering. (default: 20.0)")
    parser.add_argument('-min_x',
                        default='0.0',
                        type=float,
                        help="Only keep transcripts whose x-coordinate is greater than specified limit. " +
                             "If no limit is specified, the default minimum value will be 0.0")
    parser.add_argument('-max_x',
                        default='24000.0',
                        type=float,
                        help="Only keep transcripts whose x-coordinate is less than specified limit. " +
                             "If no limit is specified, the default value will retain all " +
                             "transcripts since Xenium slide is <24000 microns in x and y. " +
                             "(default: 24000.0)")
    parser.add_argument('-min_y',
                        default='0.0',
                        type=float,
                        help="Only keep transcripts whose y-coordinate is greater than specified limit. " +
                             "If no limit is specified, the default minimum value will be 0.0")
    parser.add_argument('-max_y',
                        default='24000.0',
                        type=float,
                        help="Only keep transcripts whose y-coordinate is less than specified limit. " +
                             "If no limit is specified, the default value will retain all " +
                             "transcripts since Xenium slide is <24000 microns in x and y. " +
                             "(default: 24000.0)")

    try:
        opts = parser.parse_args()
    except:
        sys.exit(0)

    return opts



if __name__ == "__main__":
    main()
