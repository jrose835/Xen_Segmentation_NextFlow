#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SEGGER EXPLORER 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process SEGGER_EXPLORER {
    tag "$meta.id"
    label 'process_medium'
    publishDir params.outputdir, mode: "copy", pattern:"${meta.id}_xenium_explorer"
    cpus params.seggerExplorerCPUs
    memory "${params.seggerExplorerMem} GB"

    input:
    tuple val(meta), path(seg_df_parquet)
    tuple val(meta), path(source_path)

    output:
    tuple val(meta), path("${meta.id}_xenium_explorer")                           , emit: explorer_dir
    tuple val(meta), path("${meta.id}_xenium_explorer/*.zarr.zip")               , emit: zarr_files
    tuple val(meta), path("${meta.id}_xenium_explorer/*.xenium")                 , emit: xenium_file
    path("versions.yml")                                                          , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
        error "SEGGER_EXPLORER module does not support Conda. Please use Docker / Singularity / Podman instead."
    }

    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def output_dir = "${prefix}_xenium_explorer"
    def cells_filename = task.ext.cells_filename ?: "seg_cells"
    def analysis_filename = task.ext.analysis_filename ?: "seg_analysis"
    def xenium_filename = task.ext.xenium_filename ?: "seg_experiment.xenium"
    def cell_id_column = task.ext.cell_id_column ?: "cell_id"
    def area_low = task.ext.area_low ?: 10
    def area_high = task.ext.area_high ?: 100
    def script_path = task.ext.script_path ?: "/workspace/segger_dev/src/segger/cli/seg2explorer.py"

    """
    segger_xenium_explorer.py \\
        ${seg_df_parquet} \\
        ${source_path} \\
        ${output_dir} \\
        --cells-filename ${cells_filename} \\
        --analysis-filename ${analysis_filename} \\
        --xenium-filename ${xenium_filename} \\
        --cell-id-column ${cell_id_column} \\
        --area-low ${area_low} \\
        --area-high ${area_high} \\
        --verbose \\
        ${args}
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
        seg2explorer: 1.0.0
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    def output_dir = "${prefix}_xenium_explorer"
    """
    mkdir -p ${output_dir}/
    touch ${output_dir}/seg_cells.zarr.zip
    touch ${output_dir}/seg_analysis.zarr.zip
    touch ${output_dir}/seg_experiment.xenium

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
        seg2explorer: 1.0.0
    END_VERSIONS
    """
}