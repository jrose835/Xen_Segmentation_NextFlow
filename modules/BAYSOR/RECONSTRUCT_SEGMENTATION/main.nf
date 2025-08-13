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
   tuple val(meta), path("merged.csv"), path("merged.json"), emit: complete_segmentation 

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
  
  # Process each CSV/JSON pair
  first_file=true
  for i in "\${!csv_files[@]}"; do
      csv_file="\${csv_files[i]}"
      json_file="\${json_files[i]}"
      
      echo "Processing tile \$i: \$csv_file with offset \$offset" >&2
      
      if [ "\$first_file" = true ]; then
          # First file - no offset needed
          first_file=false
          tail -n +2 "\$csv_file" >> merged.csv
          
          # Extract JSON content - sed will produce empty output for empty arrays, which is fine
          sed -E '
              s#^\\{"geometries":\\[##;
              s#\\],"type" *: *"GeometryCollection"\\}\$##;
          ' "\$json_file" > temp_json_\${i}.json
          
          # Count cells in this tile for next offset
          # Count unique cell IDs (excluding header and handling the PREFIX-ID format)
          # Use the dynamically determined column index
          cell_count=\$(tail -n +2 "\$csv_file" | awk -F',' -v col="\$cell_col" '{print \$col}' | sed 's/.*-//' | sort -nu | tail -1)
          if [ -z "\$cell_count" ]; then
              cell_count=0
          fi
          echo "Tile \$i has max cell ID: \$cell_count" >&2
          offset=\$((offset + cell_count))
          
      else
          # Subsequent files - apply offset
          
          # Process CSV with offset
          tail -n +2 "\$csv_file" | awk -F',' -v offset="\$offset" -v col="\$cell_col" '
          BEGIN {OFS=","}
          {
              if (\$col != "" && \$col != "NA" && \$col != "null") {
                  # Split on the last dash to get prefix and id
                  n = split(\$col, parts, "-")
                  if (n >= 2) {
                      # Extract the numeric ID (last part after dash)
                      old_id = parts[n]
                      # Reconstruct prefix (everything before last dash)
                      prefix = parts[1]
                      for (j = 2; j < n; j++) {
                          prefix = prefix "-" parts[j]
                      }
                      # Add offset to the numeric ID
                      new_id = old_id + offset
                      \$col = prefix "-" new_id
                  }
              }
              print
          }' >> merged.csv
          
          # Process JSON with offset using the Python script
          offset_json_cells.py "\$json_file" "temp_json_\${i}.json" \$offset
          
          # Update offset for next tile
          # Count cells in this tile using dynamically determined column
          cell_count=\$(tail -n +2 "\$csv_file" | awk -F',' -v col="\$cell_col" '{print \$col}' | sed 's/.*-//' | sort -nu | tail -1)
          if [ -z "\$cell_count" ]; then
              cell_count=0
          fi
          echo "Tile \$i has max cell ID: \$cell_count" >&2
          offset=\$((offset + cell_count))
      fi
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
  
  echo "Reconstruction complete >&2
  """
}