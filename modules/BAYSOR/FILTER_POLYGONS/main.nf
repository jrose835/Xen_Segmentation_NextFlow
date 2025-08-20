#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_POLYGONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes baysor generates empty polygons which xeniumranger import-segmentation does not like
This process avoids those issues
*/

process FILTER_POLYGONS {
    tag "$meta.id"
    
    cpus params.filterPolyCPUs
    memory "${params.filterPolyMem} GB"

    input:
    tuple val(meta), path(segmentation_csv), path(polygons_json)

    output:
    tuple val(meta), path(segmentation_csv), path("filtered_polygons.json"), emit: filtered_segmentation

    script:
    """
    filter_polygons.py \\
        --csv ${segmentation_csv} \\
        --json ${polygons_json} \\
        --output filtered_polygons.json
    """
}