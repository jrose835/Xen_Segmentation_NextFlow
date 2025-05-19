#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_TRANSCRIPTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Prepares transcripts for baysor (and does actual splitting)
// TODO
// Change inputs for meta map
process FILTER_TRANSCRIPTS {
    cpus params.filterCPUs
    memory "${params.filterMem} GB"
    
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