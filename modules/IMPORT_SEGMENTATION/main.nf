#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// TODO
// Change inputs for meta map

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