#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETER VALUES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
params.input = null
params.outputdir = "results"
params.id = "nuclear"

// Cell Ranger Resegment
params.expansion_distance = 5
params.boundary_stain= "disable" // Possible options are: "ATP1A1/CD45/E-Cadherin" (default) or "disable".
params.interior_stain= "disable" // Possible options are: "18S" (default) or "disable".
params.dapi_filter = 100

// CALC SPLITS 
params.csplit_x_bins = 10 // number of slices along the x axis (default: 10)
params.csplit_y_bins = 10 // number of slices along the y axis (default: 10)

// Resource Mgmt
params.rangersegCPUs = 32
params.rangersegMem = 128


// Validate that the input parameter is specified
if (!params.input) {
    error "The --input parameter is required but was not specified. Please provide a valid input path."
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RESEGMENT 10X
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


// Process for re-segmenting using Xenium algorithm...typically nuclear-only

process RESEGMENT_10X {
    publishDir params.outputdir, mode: "symlink"
    cpus params.rangersegCPUs
    memory "${params.rangersegMem} GB"
    //debug true

    input:
    path xen_dat     //Xenium output data path

    output:
    path "${params.id}_resegmented"

    script:
    """
    xeniumranger resegment \\
      --id="${params.id}_resegmented" \\
      --xenium-bundle=${xen_dat} \\
      --expansion-distance=$params.expansion_distance \\
      --boundary-stain=${params.boundary_stain} \\
      --interior-stain=${params.interior_stain} \\
      --localcores=${params.rangersegCPUs} \\
      --localmem=${params.rangersegMem} \\
      2>&1 | tee ranger_out.log
    """


}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CALC_SPLITS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Produces coordinates of quantile-based tiles to split transcripts file into for parallel baysor runs

process CALC_SPLITS {

    input:
    path resegmented_dir 

    output:
    path "splits.csv"

    script:
    """
    split_transcripts.py "${resegmented_dir}/outs/transcripts.parquet" "splits.csv" --x_bins ${params.csplit_x_bins} --y_bins ${params.csplit_y_bins} 
    """

}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_TRANSCRIPTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Prepares transcripts for baysor (and does actual splitting)

process FILTER_TRANSCRIPTS {
    publishDir params.outputdir, mode: "symlink"

    input:
    path resegmented_dir
    tuple val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) // Tuple from splits.csv

    output:
    path "X${x_min}-${x_max}_Y${y_min}-${y_max}_filtered_transcripts.csv"

   script:
    """
    filter_transcripts_parquet_v3.py -transcript "${resegmented_dir}/outs/transcripts.parquet" \\
      -min_x ${x_min} -max_x ${x_max} \\
      -min_y ${y_min} -max_y ${y_max}
    """
 }

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/



/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RECONSTRUCT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/




/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT_SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/




/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


workflow {

    input_channel = Channel.fromPath(params.input)

    // Run the RESEGMENT_10X process
    resegmented_output = RESEGMENT_10X(input_channel)
    // resegmented_output.view()

    // Calculate splits
    splits_csv = CALC_SPLITS(resegmented_output)

    Channel
        resegmented_output.first()
        .set {reseg_ref}

    // Read splits.csv into channel
    Channel
        splits_csv.splitCsv(header: true)
        .flatten()
        .set{ splits_channel }

    // Process and split transcripts file for Baysor
    filtered_transcripts = FILTER_TRANSCRIPTS(reseg_ref, splits_channel)
    // filtered_transcripts.view()

}
