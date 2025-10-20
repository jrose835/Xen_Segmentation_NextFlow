#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RECONSTRUCT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process RECONSTRUCT_SEGMENTATION {
  tag "$meta.id"

  input:
   tuple val(meta), path(csv_files), path(json_files)

  output:
   tuple val(meta), path("merged_validated.csv"), path("merged.json"), emit: complete_segmentation 

  script:
  """
  csv_files=( ${csv_files.join(' ')} )
  json_files=( ${json_files.join(' ')} )

  # Initialize offset counter
  offset=0
  
  # Verify we have files to process
  if [ \${#csv_files[@]} -eq 0 ]; then
      echo "Error: No CSV files to process" >&2
      exit 1
  fi
  
  # Get the column index for "cell" from the first file's header
  # This will be consistent across all files
  cell_col=\$(head -n 1 "\${csv_files[0]}" | tr ',' '\n' | nl -v 1 | grep -w "cell" | awk '{print \$1}')
  
  if [ -z "\$cell_col" ]; then
      echo "Error: Could not find 'cell' column in CSV header" >&2
      exit 1
  fi
  
  # Process first file (no offset needed)
  echo "Processing first file: \${csv_files[0]}" >&2

  # Copy header from first CSV
  head -n 1 "\${csv_files[0]}" > merged.csv

  # Extract the cell ID prefix from the first file to use as the canonical prefix
  # This ensures all cells have the same prefix (required by Xenium Ranger)
  canonical_prefix=\$(tail -n +2 "\${csv_files[0]}" | awk -F',' -v col="\$cell_col" '{print \$col}' | grep -v "^[[:space:]]*\$" | grep -v "^NA\$" | head -1 | sed 's/-[0-9]*\$//')

  if [ -z "\$canonical_prefix" ]; then
      echo "Error: Could not extract cell ID prefix from first file" >&2
      exit 1
  fi

  echo "Using canonical cell ID prefix: \$canonical_prefix" >&2

  # Process each CSV/JSON pair
  first_file=true
  for i in "\${!csv_files[@]}"; do
      csv_file="\${csv_files[i]}"
      json_file="\${json_files[i]}"

      echo "Processing tile \$i: \$csv_file with offset \$offset" >&2
      
      # ALWAYS calculate the max cell ID from CSV file regardless of JSON content
      # This ensures offset increments correctly even for empty JSON files
      cell_count=\$(tail -n +2 "\$csv_file" | awk -F',' -v col="\$cell_col" '{print \$col}' | sed 's/.*-//' | grep -E '^[0-9]+\$' | sort -nu | tail -1)
      if [ -z "\$cell_count" ]; then
          cell_count=0
      fi
      echo "Tile \$i has max cell ID: \$cell_count" >&2
      
      if [ "\$first_file" = true ]; then
          # First file - no offset needed for data, but still calculate offset for next file
          first_file=false
          tail -n +2 "\$csv_file" >> merged.csv
          
          # Extract JSON content - handle empty files gracefully
          if [ -s "\$json_file" ]; then
              sed -E '
                  s#^\\{"geometries":\\[##;
                  s#\\],"type" *: *"GeometryCollection"\\}\$##;
              ' "\$json_file" > temp_json_\${i}.json
          else
              echo "Empty JSON file for tile \$i, creating empty temp file" >&2
              touch temp_json_\${i}.json
          fi
          
      else
          # Subsequent files - apply offset to CSV data and normalize prefix

          # Process CSV with offset and replace prefix with canonical prefix
          tail -n +2 "\$csv_file" | awk -F',' -v offset="\$offset" -v col="\$cell_col" -v canonical_prefix="\$canonical_prefix" '
          BEGIN {OFS=","}
          {
              if (\$col != "" && \$col != "NA" && \$col != "null") {
                  # Split on the last dash to get prefix and id
                  n = split(\$col, parts, "-")
                  if (n >= 2) {
                      # Extract the numeric ID (last part after dash)
                      old_id = parts[n]
                      # Add offset to the numeric ID and use canonical prefix
                      new_id = old_id + offset
                      \$col = canonical_prefix "-" new_id
                  }
              }
              print
          }' >> merged.csv
          
          # Process JSON with offset using the Python script
          offset_json_cells.py "\$json_file" "temp_json_\${i}.json" \$offset
      fi
      
      # Update offset for next tile using the cell count we calculated
      # This happens AFTER processing, so the offset is ready for the next tile
      offset=\$((offset + cell_count))
      echo "Next offset will be: \$offset" >&2
  done
  
  # Merge all JSON files into final GeometryCollection
  echo '{"geometries": [' > merged.json
  
  first_entry=true
  for i in "\${!json_files[@]}"; do
      if [ -f "temp_json_\${i}.json" ]; then
          # Check if file has content
          if [ -s "temp_json_\${i}.json" ]; then
              content=\$(cat "temp_json_\${i}.json")
              if [ -n "\$content" ]; then
                  if [ "\$first_entry" = false ]; then
                      echo ',' >> merged.json
                  fi
                  cat "temp_json_\${i}.json" >> merged.json
                  first_entry=false
              fi
          fi
      fi
  done
  
  echo '],"type": "GeometryCollection"}' >> merged.json
  
  # Clean up temp files
  rm -f temp_json_*.json
  
  echo "Reconstruction complete" >&2
  
  # Validate that all cells in CSV have corresponding polygons in JSON
  # This removes transcript rows for cells without polygons
  validate_csv.py \\
      --csv merged.csv \\
      --json merged.json \\
      --output merged_validated.csv \\
      --cell-column cell
  
  # Remove the unvalidated merged.csv to save space
  rm -f merged.csv
  
  echo "Validation and reconstruction fully complete" >&2
  """
}