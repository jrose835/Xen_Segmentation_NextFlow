#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
process BAYSOR_RUN {
    tag "$meta.id"
    
    cpus params.baysorCPUs
    memory "${params.baysorMem} GB"

    input:
    tuple val(meta), val(tile_id), path(transcripts_csv)

    output:
    tuple val(meta), path("${tile_id}_segmentation.csv"), emit: csv
    tuple val(meta), path("${tile_id}_segmentation_polygons_2d.json"), emit: json

    script:
    """
    export JULIA_NUM_THREADS=$params.baysorCPUs

    # Count the number of rows in the CSV file (excluding the header)
    row_count=\$(tail -n +2 ${transcripts_csv} | wc -l)

    # Check if the transcript count is at least at specified minium
    if [ "\$row_count" -ge $params.baysor_min_trans ]; then
        echo "File ${transcripts_csv} has \$row_count rows. Running Baysor..."
        baysor run -x x_location -y y_location -z z_location -g feature_name \\
        -o ${tile_id}_segmentation.csv \\
        -m $params.baysor_m -p --prior-segmentation-confidence $params.baysor_prior --polygon-format "GeometryCollectionLegacy" \\
        ${transcripts_csv} :cell_id
    else
        echo "File ${transcripts_csv} has fewer than ${params.baysor_min_trans} rows (\$row_count). Skipping Baysor run."
        echo "transcript_id,cell_id,overlaps_nucleus,gene,x,y,z,qv,fov_name,nucleus_distance,codeword_index,codeword_category,is_gene,molecule_id,prior_segmentation,confidence,cluster,cell,assignment_confidence,is_noise,ncv_color" > ${tile_id}_segmentation.csv
        touch ${tile_id}_segmentation_polygons_2d.json
    fi
    """
}