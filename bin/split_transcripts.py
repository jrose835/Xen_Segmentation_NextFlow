#!/usr/bin/env python3
import argparse
import pandas as pd

def compute_quantile_ranges(df: pd.DataFrame, col: str, n_bins: int):
    """
    Compute the bin edges for `df[col]` such that each of the n_bins
    has ~equal count of points. Returns a list of (min, max) tuples.
    """
    # qcut with retbins=True gives you the edges of each quantile bin
    _, bins = pd.qcut(df[col], q=n_bins, retbins=True, duplicates='drop')
    # build [(min1, max1), (min2, max2), ...]
    ranges = [(bins[i], bins[i+1]) for i in range(len(bins)-1)]
    return ranges

def make_tiles(df: pd.DataFrame, x_bins: int, y_bins: int):
    """
    Produce a DataFrame with one row per tile:
      tile_id, x_min, x_max, y_min, y_max
    """
    x_ranges = compute_quantile_ranges(df, 'x_location', x_bins)
    y_ranges = compute_quantile_ranges(df, 'y_location', y_bins)
    
    tiles = []
    for ix, (x_min, x_max) in enumerate(x_ranges, start=1):
        for iy, (y_min, y_max) in enumerate(y_ranges, start=1):
            tiles.append({
                'tile_id': f'{ix}_{iy}',
                'x_min': x_min,
                'x_max': x_max,
                'y_min': y_min,
                'y_max': y_max
            })
    return pd.DataFrame(tiles)

def main():
    parser = argparse.ArgumentParser(
        description="Split transcript coordinates into quantile‐based tiles"
    )
    parser.add_argument("input", help="path to your transcripts CSV")
    parser.add_argument("output_csv", help="where to write tile definitions")
    parser.add_argument(
        "--x_bins", type=int, default=10,
        help="number of slices along the x axis (default: 10)"
    )
    parser.add_argument(
        "--y_bins", type=int, default=10,
        help="number of slices along the y axis (default: 10)"
    )
    args = parser.parse_args()

    # 1) load
    df = pd.read_parquet(args.input, engine='fastparquet')

    # 2) compute tiles
    tiles_df = make_tiles(df, args.x_bins, args.y_bins)

    # 3) save
    tiles_df.to_csv(args.output_csv, index=False)
    print(f"Wrote {len(tiles_df)} tiles to {args.output_csv}")

if __name__ == "__main__":
    main()
