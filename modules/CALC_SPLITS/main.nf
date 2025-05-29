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
    tuple val(meta), path(transcripts)

    output:
    tuple val(meta), path("splits.csv"), emit: ch_splits_csv

    script:
    """
    split_transcripts.py "${transcripts}" "splits.csv" --x_bins ${params.csplit_x_bins} --y_bins ${params.csplit_y_bins} 
    """

}