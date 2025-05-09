#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETER VALUES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
params.input = null
params.outputdir = "results"
params.id = "nuclear"

// RESEGMENT_10X
params.expansion_distance = 5
params.boundary_stain= "disable" // Possible options are: "ATP1A1/CD45/E-Cadherin" (default) or "disable".
params.interior_stain= "disable" // Possible options are: "18S" (default) or "disable".
params.dapi_filter = 100

// CALC SPLITS 
params.csplit_x_bins = 2 // number of slices along the x axis (total number of bins is product of x_bins * y_bins)
params.csplit_y_bins = 2 // number of slices along the y axis

// BAYSOR
params.baysor_m = 1 // Minimal number of molecules for a cell to be considered as real
params.baysor_prior = 0.8 // Confidence of the prior_segmentation results. Value in [0; 1]
params.baysor_min_trans = 100 // Minimum number of transcripts in a baysor chunk to perform segmentation on

// Resource Mgmt
params.rangersegCPUs = 32
params.rangersegMem = 128
params.baysorCPUs = 8
params.rangerimportCPUs = 32
params.rangerimportMem = 128

// Validate that the input parameter is specified
if (!params.input) {
    error "The --input parameter is required but was not specified. Please provide a valid input path."
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RESEGMENT 10X
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


// Process for re-segmenting using Xenium algorithm...typically nuclear-only

process RESEGMENT_10X {
    publishDir params.outputdir, mode: "symlink"
    cpus params.rangersegCPUs
    memory "${params.rangersegMem} GB"
    //debug true

    input:
    path xen_dat     //Xenium output data path

    output:
    path "${params.id}_nucsegmented"

    script:
    """
    xeniumranger resegment \\
      --id="${params.id}_nucsegmented" \\
      --xenium-bundle=${xen_dat} \\
      --expansion-distance=$params.expansion_distance \\
      --boundary-stain=${params.boundary_stain} \\
      --interior-stain=${params.interior_stain} \\
      --localcores=${params.rangersegCPUs} \\
      --localmem=${params.rangersegMem} \\
      2>&1 | tee ranger_out.log
    """


}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CALC_SPLITS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Produces coordinates of quantile-based tiles to split transcripts file into for parallel baysor runs

process CALC_SPLITS {

    input:
    path resegmented_dir 

    output:
    path "splits.csv"

    script:
    """
    split_transcripts.py "${resegmented_dir}/outs/transcripts.parquet" "splits.csv" --x_bins ${params.csplit_x_bins} --y_bins ${params.csplit_y_bins} 
    """

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_TRANSCRIPTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Prepares transcripts for baysor (and does actual splitting)

process FILTER_TRANSCRIPTS {
    
    input:
    path resegmented_dir
    tuple val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) // Tuple from splits.csv

    output:
    path "*_filtered_transcripts.csv"

   script:
    """
    filter_transcripts_parquet_v3.py -transcript "${resegmented_dir}/outs/transcripts.parquet" \\
      -min_x ${x_min} -max_x ${x_max} \\
      -min_y ${y_min} -max_y ${y_max}
    """
 }

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process BAYSOR {
    cpus params.baysorCPUs

    input:
    path transcripts_csv

    output:
    tuple path("segmentation.csv"), path("segmentation_polygons_2d.json")

    script:
    """
    export JULIA_NUM_THREADS=$params.baysorCPUs

    # Count the number of rows in the CSV file (excluding the header)
    row_count=\$(tail -n +2 ${transcripts_csv} | wc -l)

    # Check if the transcript count is at least at specified minium
    if [ "\$row_count" -ge $params.baysor_min_trans ]; then
        echo "File ${transcripts_csv} has \$row_count rows. Running Baysor..."
        baysor run -x x_location -y y_location -z z_location -g feature_name \\
        -m $params.baysor_m -p --prior-segmentation-confidence $params.baysor_prior --polygon-format "GeometryCollectionLegacy" \\
        ${transcripts_csv} :cell_id
    else
        echo "File ${transcripts_csv} has fewer than ${params.baysor_min_trans} rows (\$row_count). Skipping Baysor run."
        echo "transcript_id,cell_id,overlaps_nucleus,gene,x,y,z,qv,fov_name,nucleus_distance,codeword_index,codeword_category,is_gene,molecule_id,prior_segmentation,confidence,cluster,cell,assignment_confidence,is_noise,ncv_color" > segmentation.csv
        touch segmentation_polygons_2d.json
    fi
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RECONSTRUCT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
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
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process IMPORT_SEGMENTATION {
    publishDir params.outputdir, mode: "symlink"
    cpus params.rangerimportCPUs
    memory "${params.rangerimportMem} GB"

    input:
    path reseg_ref
    tuple path(segmentation), path(polygons)

    output:
    path "${params.id}_baysor"

    script:
    """
    xeniumranger import-segmentation --id="${params.id}_baysor" \
                                 --xenium-bundle=${reseg_ref}/outs \
                                 --transcript-assignment=${segmentation} \
                                 --viz-polygons=${polygons} \
                                 --units=microns \
                                 --localcores=${params.rangerimportCPUs} \
                                 --localmem=${params.rangerimportMem}
    """
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


workflow {

    input_channel = Channel.fromPath(params.input)

    // Run the RESEGMENT_10X process
    resegmented_output = RESEGMENT_10X(input_channel)
    // resegmented_output.view()

    // Calculate splits
    splits_csv = CALC_SPLITS(resegmented_output)

    //Set resegmenated path as value channel
    Channel
        resegmented_output.first()
        .set {reseg_ref}

    // Set splits.csv into queue channel
    Channel
        splits_csv.splitCsv(header: true)
        .flatten()
        .set{ splits_channel }

    // Process and split transcripts file for Baysor
    filtered_transcripts = FILTER_TRANSCRIPTS(reseg_ref, splits_channel)
    // filtered_transcripts.view()

    //Baysor run in chunked parallel
    segments = BAYSOR(filtered_transcripts)
    
    // Reconstruct segmentation files
    complete_segmentation = RECONSTRUCT_SEGMENTATION(segments.collect())
    //complete_segmentation.view()

    baysor_output = IMPORT_SEGMENTATION(reseg_ref, complete_segmentation)
    baysor_output.view()
}
