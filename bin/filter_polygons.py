#!/usr/bin/env python3

import argparse
import json
import pandas as pd
import sys

def extract_cell_ids_from_csv(csv_path):
    """
    Extract cell IDs from the CSV file.
    The cell column format is "PREFIX-id" - we extract just the id part.
    """
    try:
        df = pd.read_csv(csv_path)
        
        if 'cell' not in df.columns:
            print("Error: 'cell' column not found in CSV file", file=sys.stderr)
            sys.exit(1)
        
        # Extract IDs from the cell column (format: PREFIX-id)
        # We want everything after the last dash
        cell_ids = set()
        for cell_value in df['cell'].dropna().unique():
            cell_str = str(cell_value)
            if '-' in cell_str:
                # Get everything after the last dash
                cell_id = cell_str.split('-')[-1]
                try:
                    # Convert to integer to match JSON format
                    cell_ids.add(int(cell_id))
                except ValueError:
                    # If conversion fails, keep as string
                    cell_ids.add(cell_id)
            
        print(f"Found {len(cell_ids)} unique cell IDs in CSV", file=sys.stderr)
        return cell_ids
        
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        sys.exit(1)

def filter_polygons_by_cells(json_path, cell_ids, output_path):
    """
    Filter the JSON file to only include polygons with matching cell IDs.
    """
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        if 'geometries' not in data:
            print("Error: 'geometries' key not found in JSON file", file=sys.stderr)
            sys.exit(1)
        
        original_count = len(data['geometries'])
        
        # Filter geometries to only include those with matching cell IDs
        filtered_geometries = []
        for geometry in data['geometries']:
            if 'cell' in geometry:
                cell_value = geometry['cell']
                # Check if cell value matches any of our IDs
                # Handle both integer and string comparisons
                if cell_value in cell_ids or str(cell_value) in [str(c) for c in cell_ids]:
                    filtered_geometries.append(geometry)
        
        # Create the filtered JSON structure
        filtered_data = {
            'geometries': filtered_geometries,
            'type': 'GeometryCollection'
        }
        
        # Write the filtered JSON
        with open(output_path, 'w') as f:
            json.dump(filtered_data, f, indent=2)
        
        filtered_count = len(filtered_geometries)
        removed_count = original_count - filtered_count
        
        print(f"Original polygons: {original_count}", file=sys.stderr)
        print(f"Filtered polygons: {filtered_count}", file=sys.stderr)
        print(f"Removed polygons: {removed_count}", file=sys.stderr)
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing JSON file: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Filter polygon JSON to only include cells present in CSV'
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Path to the segmentation CSV file'
    )
    parser.add_argument(
        '--json',
        required=True,
        help='Path to the polygons JSON file'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path for the filtered JSON output'
    )
    
    args = parser.parse_args()
    
    # Extract cell IDs from CSV
    cell_ids = extract_cell_ids_from_csv(args.csv)
    
    if not cell_ids:
        print("Warning: No valid cell IDs found in CSV. Output will be empty.", file=sys.stderr)
    
    # Filter the JSON polygons
    filter_polygons_by_cells(args.json, cell_ids, args.output)
    
    print(f"Filtered polygons written to {args.output}", file=sys.stderr)

if __name__ == "__main__":
    main()