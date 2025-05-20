#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_TRANSCRIPTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Prepares transcripts for baysor (and does actual splitting)
process FILTER_TRANSCRIPTS {
    tag "$meta.id"

    cpus params.filterCPUs
    memory "${params.filterMem} GB"
    
    input:
    tuple val(meta), path(transcripts_path), val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max)
    //tuple val(meta), path(resegmented_dir)
    //tuple val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) // Tuple from splits.csv

    output:
    tuple val(meta), val(tile_id), path("*_filtered_transcripts.csv"), emit: transcripts_filtered

   script:
    """
    filter_transcripts_parquet_v3.py -transcript "${transcripts_path}" \\
      -min_x ${x_min} -max_x ${x_max} \\
      -min_y ${y_min} -max_y ${y_max}
    """
 }