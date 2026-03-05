process PROSEG {
    tag "$meta.id"
    label 'process_high'
    cpus params.prosegCPUs
    memory "${params.prosegMem} GB"

    input:
    tuple val(meta), path(transcripts)

    output:
    tuple val(meta), path("${prefix}/cell-polygons.geojson.gz"), path("${prefix}/transcript-metadata.csv.gz"), emit: seg_outs
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    // Exit if running this module with -profile conda / -profile mamba
    if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
        error("PROSEG module does not support Conda. Please use Docker / Singularity / Podman instead.")
    }

    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"

    // check for platform values
    if (!(params.format in ['xenium', 'cosmx', 'merscope'])) {
        error("${params.format} is an invalid platform type. Please specify xenium, cosmx, or merscope")
    }

    """
    mkdir -p ${prefix}

    proseg \\
        --${params.format} \\
        ${transcripts} \\
        --nthreads ${task.cpus} \\
        --output-expected-counts ${prefix}/expected-counts.csv.gz \\
        --output-cell-metadata ${prefix}/cell-metadata.csv.gz \\
        --output-transcript-metadata ${prefix}/transcript-metadata.csv.gz \\
        --output-gene-metadata ${prefix}/gene-metadata.csv.gz \\
        --output-rates ${prefix}/rates.csv.gz \\
        --output-cell-polygons ${prefix}/cell-polygons.geojson.gz \\
        --output-cell-polygon-layers ${prefix}/cell-polygons-layers.geojson.gz \\
        --output-cell-hulls ${prefix}/cell-hulls.geojson.gz \\
        ${args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        proseg: \$(proseg --version | sed 's/proseg //')
    END_VERSIONS
    """

    stub:
    // Exit if running this module with -profile conda / -profile mamba
    if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
        error("PROSEG module does not support Conda. Please use Docker / Singularity / Podman instead.")
    }

    prefix = task.ext.prefix ?: "${meta.id}"

    """
    mkdir -p ${prefix}/
    touch "${prefix}/expected-counts.csv.gz"
    touch "${prefix}/cell-metadata.csv.gz"
    touch "${prefix}/transcript-metadata.csv.gz"
    touch "${prefix}/gene-metadata.csv.gz"
    touch "${prefix}/rates.csv.gz"
    touch "${prefix}/cell-polygons.geojson.gz"
    touch "${prefix}/cell-polygons-layers.geojson.gz"
    touch "${prefix}/cell-hulls.geojson.gz"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        proseg: \$(proseg --version | sed 's/proseg //')
    END_VERSIONS
    """
}
