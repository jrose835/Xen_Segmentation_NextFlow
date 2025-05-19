#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CALC_SPLITS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Produces coordinates of quantile-based tiles to split transcripts file into for parallel baysor runs
// TODO
process CALC_SPLITS {
    tag "$meta.id"
    
    input:
    tuple val(meta), path(xenium_bundle)

    output:
    path "splits.csv"

    script:
    """
    split_transcripts.py "${xenium_bundle}/outs/transcripts.parquet" "splits.csv" --x_bins ${params.csplit_x_bins} --y_bins ${params.csplit_y_bins} 
    """

}