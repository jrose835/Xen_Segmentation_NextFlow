#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RECONSTRUCT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// TODO
// Change inputs for meta map
process RECONSTRUCT_SEGMENTATION {
  tag "$meta.id"

  input:
   tuple val(meta), path(csv_files), path(json_files)

  output:
   tuple path("merged.csv"), path("merged.json"), emit: complete_segmentation 

  script:
  """
  csv_files=( ${csv_files.join(' ')} )
  json_files=( ${json_files.join(' ')} )

  # Merge CSV files (keep only the first header)
  head -n 1 \${csv_files[0]} > merged.csv
  for csv in "\${csv_files[@]}"; do
      tail -n +2 "\$csv" >> merged.csv
  done

  # Merge JSON files into a single GeometryCollection
  echo '{"geometries": [' > merged.json
  count=\${#json_files[@]}
   first_entry=true
   for i in "\${!json_files[@]}"; do
       file=\${json_files[i]}
       # Extract content and check if it's non-empty after sed processing
       content=\$(sed -E '
           s#^\\{"geometries":\\[##;
           s#\\],"type" *: *"GeometryCollection"\\} *\$##;
       ' "\$file" | tr -d '[:space:]')
      
       # Only process if content is not empty
       if [ -n "\$content" ]; then
           # Add comma before entry if not the first
           if [ "\$first_entry" = false ]; then
               echo ',' >> merged.json
           fi
           # Re-run sed to get the formatted output (with whitespace preserved)
           sed -E '
               s#^\\{"geometries":\\[##;
               s#\\],"type" *: *"GeometryCollection"\\} *\$##;
           ' "\$file" >> merged.json
           first_entry=false
       fi
   done
  echo '],"type": "GeometryCollection"}' >> merged.json
  """
}