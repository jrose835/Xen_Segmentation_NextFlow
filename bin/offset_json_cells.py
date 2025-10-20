#!/usr/bin/env python3

"""
Script to add an offset to cell IDs in a Baysor JSON geometry file.
Used during reconstruction of segmentation from multiple tiles.
Enhanced with better empty file handling.
"""

import json
import sys
import argparse
import os

def offset_json_cells(input_file, output_file, offset):
    """
    Add an offset to all cell IDs in a JSON geometry collection.
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file (will contain only geometries content)
        offset: Integer offset to add to cell IDs
    """
    try:
        # Check if file exists and has content
        if not os.path.exists(input_file):
            print(f"Error: Input file {input_file} not found", file=sys.stderr)
            sys.exit(1)
        
        # Check file size
        file_size = os.path.getsize(input_file)
        if file_size == 0:
            print(f"Info: Input file {input_file} is empty", file=sys.stderr)
            # Write empty output
            with open(output_file, 'w') as out:
                out.write("")
            return
        
        # Read and parse JSON
        with open(input_file, 'r') as f:
            content = f.read().strip()
        
        # Handle empty or whitespace-only files
        if not content:
            print(f"Info: Input file {input_file} contains only whitespace", file=sys.stderr)
            with open(output_file, 'w') as out:
                out.write("")
            return
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse JSON from {input_file}: {e}", file=sys.stderr)
            print(f"File content preview: {content[:100]}...", file=sys.stderr)
            # For malformed JSON, write empty output instead of crashing
            with open(output_file, 'w') as out:
                out.write("")
            return
        
        if 'geometries' not in data:
            print(f"Warning: No 'geometries' key found in {input_file}", file=sys.stderr)
            # Write empty file
            with open(output_file, 'w') as out:
                out.write("")
            return
        
        geometries = data['geometries']
        
        # Handle empty geometries array
        if not geometries:
            print(f"Info: No geometries found in {input_file} (empty array)", file=sys.stderr)
            with open(output_file, 'w') as out:
                out.write("")
            return
        
        # Add offset to each geometry's cell ID
        for geometry in geometries:
            if 'cell' in geometry and geometry['cell'] is not None:
                # Add offset to the cell ID
                try:
                    geometry['cell'] = int(geometry['cell']) + offset
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert cell ID {geometry['cell']} to integer", file=sys.stderr)
                    continue
        
        # Output just the geometries array content (not wrapped in JSON structure)
        if geometries:
            with open(output_file, 'w') as out:
                for j, geom in enumerate(geometries):
                    if j > 0:
                        out.write(',\n')
                    json.dump(geom, out)
        else:
            # Write empty file if no geometries after processing
            with open(output_file, 'w') as out:
                out.write("")
                
    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing {input_file}: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Add offset to cell IDs in Baysor JSON geometry file'
    )
    parser.add_argument(
        'input_file',
        help='Input JSON file path'
    )
    parser.add_argument(
        'output_file',
        help='Output JSON file path (will contain geometries only)'
    )
    parser.add_argument(
        'offset',
        type=int,
        help='Offset to add to cell IDs'
    )
    
    args = parser.parse_args()
    
    if args.offset < 0:
        print("Warning: Using negative offset", file=sys.stderr)
    
    offset_json_cells(args.input_file, args.output_file, args.offset)
    
    print(f"Processed {args.input_file} with offset {args.offset} -> {args.output_file}", file=sys.stderr)

if __name__ == "__main__":
    main()