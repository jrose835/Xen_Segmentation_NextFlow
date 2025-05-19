#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE PARAMETER VALUES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Validate that the input parameter is specified

if (!params.input) {
    error "The --input parameter is required but was not specified. Please provide a valid input path."
}

if (!params.runRanger & params.runBaysor) {
    error "No method set. Please set either runRanger or runBaysor to true."
}

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
    ch_bundle_path      // channel: [ val(meta), ["xenium-bundle"] ]
    splits_channel      // channel: [ val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) ]

    main:

        ch_bundle_path

        // Process and split transcripts file for Baysor
        filtered_transcripts = FILTER_TRANSCRIPTS(ch_bundle_path, splits_channel)

        //Baysor run in chunked parallel
        segments = BAYSOR_RUN(filtered_transcripts)
        
        // Reconstruct segmentation files
        complete_segmentation = RECONSTRUCT_SEGMENTATION(segments.collect())

    emit:
    segmentation = complete_segmentation


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
        Main Workflow
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

        //Set resegmenated path as value channel
        //Channel
        //    resegmented_output.first()
        //    .set {reseg_ref}

    //TODO Find a way to convert resegmented_output channel into value channel...FOR EACH ID in meta

        // Set splits.csv into queue channel
        Channel
            splits_csv.splitCsv(header: true)
            .flatten()
            .set{ splits_channel }

        complete_segmentation = BAYSOR_PARALLEL(resegmented_output, splits_channel) 

        baysor_output = IMPORT_SEGMENTATION(reseg_ref, complete_segmentation) //TODO fix input channel for reseg_ref
    }
    
}


