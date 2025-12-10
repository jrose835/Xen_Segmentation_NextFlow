#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process IMPORT_SEGMENTATION {
    tag "$meta.id"
    publishDir params.outputdir, mode: "copy"
    cpus params.rangerimportCPUs
    memory "${params.rangerimportMem} GB"

    input:
    tuple val(meta), path(xenium_bundle)
    tuple val(meta), path(segmentation), path(polygons)

    output:
    tuple val(meta), path("${prefix}"), emit: bundle

    script:
    def suffix = task.ext.suffix ?: '_baysor'
    prefix = "${meta.id}${suffix}"
    """
    xeniumranger import-segmentation --id="${prefix}" \
                                 --xenium-bundle=${xenium_bundle} \
                                 --transcript-assignment=${segmentation} \
                                 --viz-polygons=${polygons} \
                                 --units=microns \
                                 --localcores=${params.rangerimportCPUs} \
                                 --localmem=${params.rangerimportMem}
    """
}