#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT FUNCTIONS & MODULES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { samplesheetToList        } from 'plugin/nf-schema'

//XeniumRanger
include { RESEGMENT_10X            } from 'modules/RESEGMENT_10X/main'
include { IMPORT_SEGMENTATION      } from 'modules/IMPORT_SEGMENTATION/main'

//Baysor
include { CALC_SPLITS              } from 'modules/CALC_SPLITS/main'
include { FILTER_TRANSCRIPTS       } from 'modules/BAYSOR/FILTER_TRANSCRIPTS/main'
include { BAYSOR_RUN               } from 'modules/BAYSOR/BAYSOR_RUN/main'
include { RECONSTRUCT_SEGMENTATION } from 'modules/BAYSOR/RECONSTRUCT_SEGMENTATION/main'


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow BAYSOR_PARALLEL {

    take:
    ch_bundle_path          // channel: [ val(meta), ["xenium-bundle"] ]
    ch_splits_csv          // channel: [ val(meta), ["splits.csv"]]

    main:

        // Set bundle channel into value
        Channel
            ch_bundle_path.first()
            .set {reseg_ref} // channel: [val(meta), ["xenium-bundle"] ]

        // Set splits.csv into queue channel
        Channel
            ch_splits_csv.map{ _meta, splits -> return[ splits ] }
            .splitCsv(header: true)
            .flatten()
            .set{ ch_splits } // channel: [ val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) ]


        // Process and split transcripts file for Baysor
        filtered_transcripts = FILTER_TRANSCRIPTS(reseg_ref, ch_splits)

        //Baysor run in chunked parallel
        BAYSOR_RUN(filtered_transcripts)

        // Prepare inputs for reconstruction
        grouped_csvs = BAYSOR_RUN.out.csv.groupTuple(by: 0).map { meta, vals -> tuple(meta, vals.collect{ it[1] }) }
        grouped_jsons = BAYSOR_RUN.out.json.groupTuple(by: 0).map { meta, vals -> tuple(meta, vals.collect{ it[1] }) }
        merged_inputs = grouped_csvs.join(grouped_jsons, by: 0)

        // Reconstruct segmentation files
        RECONSTRUCT_SEGMENTATION(merged_inputs)


    emit:
    segmentation = RECONSTRUCT_SEGMENTATION.out.complete_segmentation


}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {

    /*
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        INPUTS
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    */

    // Validate that the input parameter is specified

    if (!params.input) {
        error "The --input parameter is required but was not specified. Please provide a valid input path."
    }

    if (!params.runRanger & params.runBaysor) {
        error "No method set. Please set either runRanger or runBaysor to true."
    }

    // Set channels
    //TODO: Make sure this isn't broken if file has additional metadata columns
    Channel
        .fromList(samplesheetToList(params.input, "${projectDir}/assets/schema_input.json"))
        .map {
            meta, bundle, image -> return [ [id: meta.id], bundle, image ]
        }
        .set { ch_samplesheet }

    // get samplesheet fields
    ch_bundle_path = ch_samplesheet.map { meta, bundle, _image ->
        return [ meta, file(bundle)]
    }

    // get transcript.parquet
    ch_transcripts_parquet = ch_samplesheet.map { meta, bundle, _image ->
        def transcripts_parquet = file(bundle.replaceFirst(/\/$/, '') + "/transcripts.parquet")
        return [ meta, transcripts_parquet ]
    }

    /*
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Workflow
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    */

    if ( params.runRanger ) {
        // Run the RESEGMENT_10X process
        resegmented_output = RESEGMENT_10X(ch_bundle_path)
    } else {
        resegmented_output = ch_bundle_path
    }

    if ( params.runBaysor ) {
        // Calculate splits for tiling transcript file
        splits_csv = CALC_SPLITS(resegmented_output)

        complete_segmentation = BAYSOR_PARALLEL(resegmented_output, splits_csv) 

        baysor_output = IMPORT_SEGMENTATION(resegmented_output, complete_segmentation) //TODO fix input channels
    }
    
}


