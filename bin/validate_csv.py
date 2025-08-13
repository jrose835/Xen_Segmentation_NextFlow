#!/usr/bin/env python3

"""
Script to validate that all cells in a CSV have corresponding polygons in a JSON file.
Removes transcript rows for cells that don't have polygons.
"""

import json
import csv
import sys
import argparse
from collections import defaultdict

def extract_cell_ids_from_json(json_path):
    """
    Extract all cell IDs from the JSON geometry collection.
    
    Returns:
        set: Set of cell IDs (as strings) found in the JSON
    """
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        json_cells = set()
        if 'geometries' in data:
            for geom in data['geometries']:
                if 'cell' in geom and geom['cell'] is not None:
                    json_cells.add(str(geom['cell']))
        
        print(f"Found {len(json_cells)} unique cells in JSON", file=sys.stderr)
        return json_cells
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading JSON file: {e}", file=sys.stderr)
        sys.exit(1)

def find_cell_column(header_row):
    """
    Find the index of the 'cell' column in the CSV header.
    
    Returns:
        int: Column index (0-based) or None if not found
    """
    try:
        return header_row.index('cell')
    except ValueError:
        return None

def extract_cell_id(cell_value):
    """
    Extract the numeric ID from a cell value in format PREFIX-ID.
    
    Args:
        cell_value: The cell value (e.g., "BAYSOR-123")
    
    Returns:
        str: The extracted ID or None if invalid
    """
    if not cell_value or cell_value in ['', 'NA', 'null', 'None']:
        return None
    
    cell_str = str(cell_value)
    if '-' in cell_str:
        # Get everything after the last dash
        parts = cell_str.rsplit('-', 1)
        if len(parts) == 2:
            return parts[1]
    
    # If no dash, return the whole value
    return cell_str

def validate_and_filter_csv(csv_path, json_cells, output_path, cell_col_name='cell'):
    """
    Validate CSV against JSON cells and filter out orphaned transcript rows.
    
    Args:
        csv_path: Path to input CSV file
        json_cells: Set of valid cell IDs from JSON
        output_path: Path to output validated CSV file
        cell_col_name: Name of the cell column (default: 'cell')
    
    Returns:
        tuple: (kept_count, removed_count, orphaned_cells_sample)
    """
    kept_count = 0
    removed_count = 0
    orphaned_cells = []
    orphaned_cells_counts = defaultdict(int)
    
    try:
        with open(csv_path, 'r', newline='') as infile, \
             open(output_path, 'w', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Process header
            header = next(reader)
            writer.writerow(header)
            
            # Find cell column index
            cell_col_idx = find_cell_column(header)
            if cell_col_idx is None:
                print(f"Error: Could not find '{cell_col_name}' column in CSV header", file=sys.stderr)
                print(f"Available columns: {', '.join(header)}", file=sys.stderr)
                sys.exit(1)
        
            # Process data rows
            for row_num, row in enumerate(reader, start=2):
                cell_value = row[cell_col_idx]
                cell_id = extract_cell_id(cell_value)
                
                if cell_id is None:
                    # No cell assignment - keep the row (unassigned transcript)
                    writer.writerow(row)
                    kept_count += 1
                elif cell_id in json_cells:
                    # Cell has a polygon - keep the row
                    writer.writerow(row)
                    kept_count += 1
                else:
                    # Cell has no polygon - remove the row
                    removed_count += 1
                    orphaned_cells_counts[cell_value] += 1
                    if len(orphaned_cells) < 10:
                        orphaned_cells.append(cell_value)
            
        return kept_count, removed_count, orphaned_cells, orphaned_cells_counts
        
    except Exception as e:
        print(f"Error processing CSV file: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Validate CSV transcripts against JSON polygons and remove orphaned cells'
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Path to the merged CSV file'
    )
    parser.add_argument(
        '--json',
        required=True,
        help='Path to the merged JSON file with polygons'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path for the validated CSV output'
    )
    parser.add_argument(
        '--cell-column',
        default='cell',
        help='Name of the cell column in CSV (default: cell)'
    )
    
    args = parser.parse_args()
    
    print("Starting validation of cell-polygon correspondence...", file=sys.stderr)
    
    # Extract cell IDs from JSON
    json_cells = extract_cell_ids_from_json(args.json)
    
    # Validate and filter CSV
    print("Checking for orphaned cells in CSV...", file=sys.stderr)
    kept_count, removed_count, orphaned_cells, orphaned_cells_counts = validate_and_filter_csv(
        args.csv, 
        json_cells, 
        args.output,
        args.cell_column
    )
    
    # Report results
    if removed_count > 0:
        print(f"\nWARNING: Removed {removed_count} transcript rows with no corresponding polygon", file=sys.stderr)
        
        # Show sample of orphaned cells
        if len(orphaned_cells) > 0:
            if removed_count <= 10:
                print(f"Orphaned cells: {', '.join(orphaned_cells)}", file=sys.stderr)
            else:
                print(f"Orphaned cells (first 10): {', '.join(orphaned_cells)}...", file=sys.stderr)
        
        # Show statistics about orphaned cells
        unique_orphaned = len(orphaned_cells_counts)
        print(f"Total unique orphaned cell IDs: {unique_orphaned}", file=sys.stderr)
        
        # Show top orphaned cells by transcript count
        if unique_orphaned > 0:
            top_orphaned = sorted(orphaned_cells_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"Top orphaned cells by transcript count:", file=sys.stderr)
            for cell, count in top_orphaned:
                print(f"  {cell}: {count} transcripts", file=sys.stderr)
    
    print(f"\nKept {kept_count} transcript rows with valid polygons or unassigned", file=sys.stderr)
    
    # Final statistics
    print(f"\nFinal validation complete:", file=sys.stderr)
    print(f"  - Validated CSV rows: {kept_count}", file=sys.stderr)
    print(f"  - JSON polygons: {len(json_cells)}", file=sys.stderr)
    print(f"  - Removed orphaned rows: {removed_count}", file=sys.stderr)
    
    print(f"\nValidated CSV written to {args.output}", file=sys.stderr)

if __name__ == "__main__":
    main()