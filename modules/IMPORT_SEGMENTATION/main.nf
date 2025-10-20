#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// TODO
// Change inputs for meta map

process IMPORT_SEGMENTATION {
    tag "$meta.id"
    publishDir params.outputdir, mode: "copy"
    cpus params.rangerimportCPUs
    memory "${params.rangerimportMem} GB"

    input:
    tuple val(meta), path(xenium_bundle)
    tuple val(meta), path(segmentation), path(polygons)

    output:
    path "${params.id}_baysor"

    script:
    """
    xeniumranger import-segmentation --id="${params.id}_baysor" \
                                 --xenium-bundle=${xenium_bundle} \
                                 --transcript-assignment=${segmentation} \
                                 --viz-polygons=${polygons} \
                                 --units=microns \
                                 --localcores=${params.rangerimportCPUs} \
                                 --localmem=${params.rangerimportMem}
    """
}