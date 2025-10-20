#!/usr/bin/env python3
"""
Detect the maximum token ID from transcripts.parquet in a Xenium bundle.
Outputs the recommended num_tx_tokens value.
"""

import sys
import argparse
import pandas as pd
from pathlib import Path

def detect_max_token_id(base_dir):
    """
    Scan Xenium bundle for maximum feature_name_id value in transcripts.parquet.
    
    Args:
        base_dir: Path to Xenium bundle directory
        
    Returns:
        int: Maximum token ID found
    """
    base_path = Path(base_dir)
    
    # Look for transcripts.parquet in the bundle
    transcripts_file = base_path / "transcripts.parquet"
    
    if not transcripts_file.exists():
        # Try in outs subdirectory (for processed bundles)
        transcripts_file = base_path / "outs" / "transcripts.parquet"
    
    if not transcripts_file.exists():
        print(f"Error: Could not find transcripts.parquet in {base_dir}", file=sys.stderr)
        return 312  # Default for standard Xenium
    
    max_token_id = 0
    
    try:
        print(f"Reading {transcripts_file}", file=sys.stderr)
        df = pd.read_parquet(transcripts_file)
        
        if 'feature_name_id' in df.columns:
            max_token_id = df['feature_name_id'].max()
            unique_tokens = df['feature_name_id'].nunique()
            print(f"  Found {unique_tokens} unique transcript types", file=sys.stderr)
            print(f"  Maximum feature_name_id: {max_token_id}", file=sys.stderr)
            
            # Also report some statistics
            if 'feature_name' in df.columns:
                total_transcripts = len(df)
                unique_genes = df['feature_name'].nunique()
                print(f"  Total transcripts: {total_transcripts:,}", file=sys.stderr)
                print(f"  Unique gene names: {unique_genes}", file=sys.stderr)
        else:
            print(f"Error: feature_name_id column not found in {transcripts_file}", file=sys.stderr)
            print(f"Available columns: {', '.join(df.columns)}", file=sys.stderr)
            return 312
            
    except Exception as e:
        print(f"Error reading {transcripts_file}: {e}", file=sys.stderr)
        return 312
    
    return int(max_token_id)

def main():
    parser = argparse.ArgumentParser(description='Detect num_tx_tokens for Segger from Xenium bundle')
    parser.add_argument('base_dir', help='Path to Xenium bundle directory')
    parser.add_argument('--buffer', type=int, default=10, help='Buffer to add (default: 10)')
    parser.add_argument('--min-tokens', type=int, default=313, 
                       help='Minimum number of tokens (default: 313 for standard Xenium)')
    parser.add_argument('--quiet', action='store_true', help='Only output the number')
    args = parser.parse_args()
    
    max_token_id = detect_max_token_id(args.base_dir)
    
    # Calculate num_tx_tokens with buffer and minimum
    num_tx_tokens = max(int(max_token_id) + args.buffer, args.min_tokens)
    
    if not args.quiet:
        print(f"\n=== Token Analysis ===", file=sys.stderr)
        print(f"Maximum token ID: {max_token_id}", file=sys.stderr)
        print(f"Buffer: {args.buffer}", file=sys.stderr)
        print(f"Minimum tokens: {args.min_tokens}", file=sys.stderr)
        print(f"Final num_tx_tokens: {num_tx_tokens}", file=sys.stderr)
        
        # Identify the panel type
        if max_token_id <= 313:
            print("\n✓ Standard Xenium panel detected", file=sys.stderr)
        elif max_token_id > 400:
            print("\n⚠️  Prime 5K panel detected (expanded gene set)", file=sys.stderr)
            print(f"   Standard panel: ~313 tokens", file=sys.stderr)
            print(f"   Your panel: {num_tx_tokens} tokens", file=sys.stderr)
        else:
            print("\n⚠️  Custom or extended panel detected", file=sys.stderr)
    
    # Output just the number to stdout for easy capture
    print(num_tx_tokens)
    return 0

if __name__ == "__main__":
    sys.exit(main())