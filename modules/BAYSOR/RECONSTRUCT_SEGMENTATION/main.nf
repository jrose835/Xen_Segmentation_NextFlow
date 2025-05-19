#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RECONSTRUCT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// TODO
// Change inputs for meta map
process RECONSTRUCT_SEGMENTATION {

    input:
    val files_list

    output:
    tuple path("merged.csv"), path("merged.json")

    script:
    def csv_files = files_list.withIndex().findAll { it[1] % 2 == 0 }.collect { it[0] }
    def json_files = files_list.withIndex().findAll { it[1] % 2 != 0 }.collect { it[0] }

    """
    # Merge CSV files (odd-indexed: 1,3,5,...)
    head -n 1 ${csv_files[0]} > merged.csv
    for csv in ${csv_files.join(' ')}; do
        tail -n +2 \$csv >> merged.csv
    done

    # Merge JSON files (even-indexed: 2,4,6,...)
    # Start the wrapper
    echo '{"geometries": [' > merged.json

    # put all the JSON filenames into a bash array
    files=( ${json_files.join(' ')} )
    count=\${#files[@]}

    for i in \"\${!files[@]}\"; do
      file=\${files[i]}

      # strip off the outer wrapper, leaving just the inner array items
      sed -E '
        s#^\\{"geometries":\\[##; 
        s#\\],\"type\" *: *\"GeometryCollection\"\\} *\$##;
      ' \"\$file\" >> merged.json

      # add a comma between items (but not after the last one)
      if [ \$i -lt \$((count - 1)) ]; then
        echo ',' >> merged.json
      fi
    done

    # close out the wrapper
    echo '],"type": "GeometryCollection"}' >> merged.json
    """
}