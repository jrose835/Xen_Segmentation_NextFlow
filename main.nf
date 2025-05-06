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


// Need to Add expansion distance parameter here

process RESEGMENT_10X {
    publishDir params.outputdir, mode: "symlink"
    cpus params.rangersegCPUs
    memory "${params.rangersegMem} GB"
    debug true

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
    CHUNKIFY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/





/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FILTER_TRANSCRIPTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process FILTER_TRANSCRIPTS {
    publishDir params.outputdir, mode: "symlink"

    input:
    path resegmented_dir

    output:
    path 'X0.0-24000.0_Y0.0-24000.0_filtered_transcripts.csv'

   script:
    """
    filter_transcripts_parquet_v3.py -transcript "${resegmented_dir}/outs/transcripts.parquet"
    """
 }

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR
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
    // Process transcripts file for Baysor
    filtered_transcripts = FILTER_TRANSCRIPTS(resegmented_output)
    filtered_transcripts.view()

}
