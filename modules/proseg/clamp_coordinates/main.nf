process CLAMP_COORDINATES {
    tag "$meta.id"
    label 'process_medium'
    cpus 2
    memory "16 GB"

    input:
    tuple val(meta), path(xenium_bundle), path(segmentation), path(polygons)

    output:
    tuple val(meta), path("clamped/fixed_bundle"), path("clamped/transcript-metadata.csv"), path("clamped/cell-polygons.geojson"), emit: clamped

    script:
    def args = task.ext.args ?: ''
    """
    clamp_coordinates.py ${args} \
        --bundle ${xenium_bundle} \
        --csv ${segmentation} \
        --geojson ${polygons} \
        --output-dir clamped
    """
}
