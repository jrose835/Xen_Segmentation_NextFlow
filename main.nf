#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT FUNCTIONS & MODULES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { samplesheetToList        } from 'plugin/nf-schema'

//XeniumRanger
include { RESEGMENT_10X            } from './modules/RESEGMENT_10X/main'
include { IMPORT_SEGMENTATION      } from './modules/IMPORT_SEGMENTATION/main'

//Baysor
include { CALC_SPLITS              } from './modules/CALC_SPLITS/main'
include { FILTER_TRANSCRIPTS       } from './modules/BAYSOR/FILTER_TRANSCRIPTS/main'
include { BAYSOR_RUN               } from './modules/BAYSOR/BAYSOR_RUN/main'
include { RECONSTRUCT_SEGMENTATION } from './modules/BAYSOR/RECONSTRUCT_SEGMENTATION/main'


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    BAYSOR SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow BAYSOR_PARALLEL {

    take:
    ch_transcripts_parquet          // channel: [ val(meta), ["xenium-bundle" + "/transcripts.parquet"] ]
    ch_splits_csv          // channel: [ val(meta), ["splits.csv"]]

    main:

        // Set splits.csv into tuple queue channel
        Channel
            ch_splits_csv
            .flatMap { meta, splits_file ->
                splits_file.splitCsv(header: true).collect { row ->
                    tuple(meta, row.tile_id, row.x_min, row.x_max, row.y_min, row.y_max)
                }
            }
            .set { ch_splits } // channel: [ val(tile_id), val(x_min), val(x_max), val(y_min), val(y_max) ]

        //Add in sample path for each split value
        transcripts_input = ch_transcripts_parquet.combine(ch_splits, by: 0)

        // Process and split transcripts file for Baysor
        FILTER_TRANSCRIPTS(transcripts_input)

        //Baysor run in chunked parallel
        BAYSOR_RUN(FILTER_TRANSCRIPTS.out.transcripts_filtered)
        
        // Combine baysor file channels for reconstruction 
        grouped_csvs = BAYSOR_RUN.out.csv.groupTuple(by: 0)
        grouped_jsons = BAYSOR_RUN.out.json.groupTuple(by: 0)
        merged_inputs = grouped_csvs.join(grouped_jsons, by: 0)

        // Reconstruct segmentation files
        RECONSTRUCT_SEGMENTATION(merged_inputs)


    emit:
    segmentation = RECONSTRUCT_SEGMENTATION.out.complete_segmentation


}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SEGGER SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// Adapted from nf-core/spatialxe

include { SEGGER_TRAIN          } from './modules/segger/train/main'
include { SEGGER_PREDICT        } from './modules/segger/predict/main'
include { SEGGER_CREATE_DATASET } from './modules/segger/create_dataset/main'
// include { PARQUET_TO_CSV        } from './modules/spatialconverter/parquet_to_csv/main'

workflow SEGGER_CREATE_TRAIN_PREDICT {

    take:

    ch_basedir              // channel: [ val(meta), [ "basedir" ] ]
    ch_transcripts_parquet  // channel: [ val(meta), [bundle + "/transcripts.parquet"]]

    main:

    ch_versions = Channel.empty()

    // create dataset
    SEGGER_CREATE_DATASET ( ch_basedir )
    ch_versions = ch_versions.mix ( SEGGER_CREATE_DATASET.out.versions )

    // train a model with the dataset created
    SEGGER_TRAIN ( SEGGER_CREATE_DATASET.out.datasetdir )
    ch_versions = ch_versions.mix ( SEGGER_TRAIN.out.versions )

    // run prediction with the trained models
    ch_just_trained_models = SEGGER_TRAIN.out.trained_models.map {
                _meta, models -> return [ models ]
    }
    ch_just_transcripts_parquet = ch_transcripts_parquet.map {
                _meta, transcripts -> return [ transcripts ]
    }
    SEGGER_PREDICT ( SEGGER_CREATE_DATASET.out.datasetdir, ch_just_trained_models, ch_just_transcripts_parquet )
    ch_versions = ch_versions.mix ( SEGGER_PREDICT.out.versions )

    // convert parquet to csv
    //PARQUET_TO_CSV( SEGGER_PREDICT.out.transcripts )
    //ch_versions = ch_versions.mix( PARQUET_TO_CSV.out.versions )

    emit:

    datasetdir     = SEGGER_CREATE_DATASET.out.datasetdir // channel: [ val(meta), [ datasetdir ] ]
    trained_models = SEGGER_TRAIN.out.trained_models      // channel: [ val(meta), [ trained_models ] ]
    benchmarks     = SEGGER_PREDICT.out.benchmarks        // channel: [ val(meta), [ benchmarks ] ]
    //ch_transcripts = PARQUET_TO_CSV.out.transcripts_csv   // channel: [ val(meta), [ transcripts ] ]

    versions       = ch_versions                          // channel: [ versions.yml ]


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

    // Validate that the input and workflow parameters are specified

    if (!params.input) {
        error "The --input parameter is required but was not specified. Please provide a valid input path."
    }

    if (!params.runRanger && !params.runBaysor) {
        error "No method set. Please set either runRanger or runBaysor to true."
    }

    // Set channels
    //TODO: Make sure this isn't broken if file has additional metadata columns
    Channel
        .fromList(samplesheetToList(params.input, "${projectDir}/assets/schema_input.json"))
        .map {
            meta, bundle, image, splits -> return [ [id: meta.id], bundle, image, splits ]
        }
        .set { ch_samplesheet }

    // get samplesheet fields
    ch_bundle_path = ch_samplesheet.map { meta, bundle, _image , _splits->
        return [ meta, file(bundle)]
    }
    
    // get transcript.parquet
    ch_transcripts_parquet = ch_samplesheet.map { meta, bundle, _image, _splits ->
        def transcripts_parquet = file(bundle.replaceFirst(/\/$/, '') + "/transcripts.parquet")
        return [ meta, transcripts_parquet ]
    }

    // get user defined splits
    if (params.preset_splits) {
        ch_splits = ch_samplesheet.map { meta, _bundle, _image , splits->
            return [ meta, file(splits)]
        }
    }

    /*
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Workflow
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    */

    if ( params.runRanger ) {
        // Run the RESEGMENT_10X process
        RESEGMENT_10X(ch_bundle_path)
        ch_transcripts_parquet = RESEGMENT_10X.out.parquet
        ch_bundle_path = RESEGMENT_10X.out.bundle
    }

    if ( params.runBaysor ) {
        // Calculate splits for tiling transcript file
        if (!params.preset_splits) {
            CALC_SPLITS(ch_transcripts_parquet)
            ch_splits = CALC_SPLITS.out.ch_splits_csv
        }
        //Baysor segmentation (using parallel processing workflow)
        BAYSOR_PARALLEL(ch_transcripts_parquet, ch_splits)

        //Importing baysor segmentation into new Xenium bundle
        IMPORT_SEGMENTATION(ch_bundle_path, BAYSOR_PARALLEL.out.segmentation)
    }
    
    if (params.runSegger ) {
        SEGGER_CREATE_TRAIN_PREDICT (ch_bundle_path, ch_transcripts_parquet)
    }

}