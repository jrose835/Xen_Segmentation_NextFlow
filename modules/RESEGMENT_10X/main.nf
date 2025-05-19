#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RESEGMENT 10X
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


// Process for re-segmenting using Xenium algorithm...typically nuclear-only

process RESEGMENT_10X {
    tag "$meta.id"
    publishDir params.outputdir, mode: "symlink", pattern:"*_resegmented"
    cpus params.rangersegCPUs
    memory "${params.rangersegMem} GB"

    input:
    tuple val(meta), path(xenium_bundle)

    output:
    tuple val(meta), path("${meta.id}_resegmented")

    script:
    """
    xeniumranger resegment \\
      --id="${meta.id}_resegmented" \\
      --xenium-bundle=${xenium_bundle} \\
      --expansion-distance=$params.expansion_distance \\
      --boundary-stain=${params.boundary_stain} \\
      --interior-stain=${params.interior_stain} \\
      --localcores=${params.rangersegCPUs} \\
      --localmem=${params.rangersegMem} \\
      2>&1 | tee ranger_out.log
    """

}