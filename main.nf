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
include { IMPORT_SEGMENTATION as IMPORT_SEGMENTATION_PROSEG } from './modules/IMPORT_SEGMENTATION/main'

//Baysor
include { CALC_SPLITS              } from './modules/CALC_SPLITS/main'
include { FILTER_TRANSCRIPTS       } from './modules/BAYSOR/FILTER_TRANSCRIPTS/main'
include { BAYSOR_RUN               } from './modules/BAYSOR/BAYSOR_RUN/main'
include { RECONSTRUCT_SEGMENTATION } from './modules/BAYSOR/RECONSTRUCT_SEGMENTATION/main'
include { FILTER_POLYGONS          } from './modules/BAYSOR/FILTER_POLYGONS'

//Segger
include { SEGGER_TRAIN             } from './modules/segger/train/main'
include { SEGGER_PREDICT           } from './modules/segger/predict/main'
include { SEGGER_CREATE_DATASET    } from './modules/segger/create_dataset/main'
include { SEGGER_EXPLORER          } from './modules/segger/explorer/main'
// include { PARQUET_TO_CSV        } from './modules/spatialconverter/parquet_to_csv/main'

//Proseg
include { PROSEG                   } from './modules/proseg/preset/main'
include { PROSEG2BAYSOR            } from './modules/proseg/proseg2baysor/main'

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

        // Filter polygons to only include cells present in the CSV
        FILTER_POLYGONS(RECONSTRUCT_SEGMENTATION.out.complete_segmentation)


    emit:
    segmentation = FILTER_POLYGONS.out.filtered_segmentation


}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SEGGER SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// Adapted from nf-core/spatialxe



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
    // Join all channels by metadata to ensure correct model/dataset/transcripts matching
    ch_predict_input = SEGGER_CREATE_DATASET.out.datasetdir
        .join(SEGGER_TRAIN.out.trained_models, by: 0)
        .join(ch_transcripts_parquet, by: 0)
        .map { meta, dataset, num_tokens, models, transcripts ->
            return [ meta, dataset, num_tokens, models, transcripts ]
        }

    SEGGER_PREDICT (
        ch_predict_input.map { meta, dataset, num_tokens, _models, _transcripts -> [ meta, dataset, num_tokens ] },
        ch_predict_input.map { _meta, _dataset, _num_tokens, models, _transcripts -> models },
        ch_predict_input.map { _meta, _dataset, _num_tokens, _models, transcripts -> transcripts }
    )
    ch_versions = ch_versions.mix ( SEGGER_PREDICT.out.versions )

    // Extract the segger transcripts parquet from the nested directory structure
    ch_segger_transcripts = SEGGER_PREDICT.out.transcripts.map { meta, transcripts_files ->
        def transcript_file = transcripts_files instanceof List ? transcripts_files[0] : transcripts_files
        return [ meta, transcript_file ]
    }

    // Run SEGGER_EXPLORER to create Xenium Explorer compatible files
    SEGGER_EXPLORER ( ch_segger_transcripts, ch_basedir )
    ch_versions = ch_versions.mix ( SEGGER_EXPLORER.out.versions )

    emit:
    datasetdir     = SEGGER_CREATE_DATASET.out.datasetdir
    trained_models = SEGGER_TRAIN.out.trained_models
    benchmarks     = SEGGER_PREDICT.out.benchmarks
    versions       = ch_versions
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PROSEG SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// Adapted from nf-core/spatialxe

workflow PROSEG_RUN {

    take:
    ch_bundle_path          // channel: [ val(meta), [ "basedir" ] ]
    ch_transcripts_parquet  // channel: [ val(meta), [bundle + "/transcripts.parquet"]]

    main:
    ch_versions = Channel.empty()

    // Run proseg segmentation on transcripts
    PROSEG(ch_transcripts_parquet)
    ch_versions = ch_versions.mix(PROSEG.out.versions)

    // Convert proseg output to Baysor-compatible format for import
    PROSEG2BAYSOR(PROSEG.out.seg_outs)
    ch_versions = ch_versions.mix(PROSEG2BAYSOR.out.versions)

    // Prepare input for IMPORT_SEGMENTATION
    // PROSEG2BAYSOR.out.converted emits: tuple val(meta), path(transcript-metadata.csv), path(cell-polygons.geojson)
    ch_proseg_segmentation = PROSEG2BAYSOR.out.converted

    emit:
    segmentation = ch_proseg_segmentation  // [ meta, csv, geojson ] - compatible with IMPORT_SEGMENTATION
    versions     = ch_versions
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
    
    // Validate that the input and workflow parameters are specified
    if (!params.input) {
        error "The --input parameter is required but was not specified. Please provide a valid input path."
    }
    
    if (!params.runRanger && !params.runBaysor && !params.runSegger && !params.runProseg) {
        error "No method set. Please set runRanger, runBaysor, runSegger, or runProseg to true."
    }

    // If Ranger is not running but Baysor is, force baysor_from_resegment to false
    def effective_baysor_from_resegment = params.baysor_from_resegment
    if (!params.runRanger && params.runBaysor && params.baysor_from_resegment) {
        log.warn "Warning: baysor_from_resegment is set to true but runRanger is false. Setting baysor_from_resegment to false."
        effective_baysor_from_resegment = false
    }

    // If Ranger is not running but Proseg is with proseg_from_resegment, force it to false
    def effective_proseg_from_resegment = params.proseg_from_resegment
    if (!params.runRanger && params.runProseg && params.proseg_from_resegment) {
        log.warn "Warning: proseg_from_resegment is set to true but runRanger is false. Setting proseg_from_resegment to false."
        effective_proseg_from_resegment = false
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
        ch_transcripts_parquet_ranger = RESEGMENT_10X.out.parquet
        ch_bundle_path_ranger = RESEGMENT_10X.out.bundle
    }
    
    if ( params.runBaysor ) {
        if (effective_baysor_from_resegment) {
            // Calculate splits for tiling transcript file
            if (!params.preset_splits) {
                CALC_SPLITS(ch_transcripts_parquet_ranger)
                ch_splits = CALC_SPLITS.out.ch_splits_csv
            }
            //Baysor segmentation (using parallel processing workflow)
            BAYSOR_PARALLEL(ch_transcripts_parquet_ranger, ch_splits)

            // Join channels by metadata to ensure correct bundle-segmentation pairing
            ch_baysor_import_input = ch_bundle_path_ranger.join(BAYSOR_PARALLEL.out.segmentation, by: 0)

            //Importing baysor segmentation into new Xenium bundle
            IMPORT_SEGMENTATION(
                ch_baysor_import_input.map { meta, bundle, csv, geojson -> tuple(meta, bundle) },
                ch_baysor_import_input.map { meta, bundle, csv, geojson -> tuple(meta, csv, geojson) }
            )
        }
        else {
            // Calculate splits for tiling transcript file
            if (!params.preset_splits) {
                CALC_SPLITS(ch_transcripts_parquet)
                ch_splits = CALC_SPLITS.out.ch_splits_csv
            }
            //Baysor segmentation (using parallel processing workflow)
            BAYSOR_PARALLEL(ch_transcripts_parquet, ch_splits)

            // Join channels by metadata to ensure correct bundle-segmentation pairing
            ch_baysor_import_input = ch_bundle_path.join(BAYSOR_PARALLEL.out.segmentation, by: 0)

            //Importing baysor segmentation into new Xenium bundle
            IMPORT_SEGMENTATION(
                ch_baysor_import_input.map { meta, bundle, csv, geojson -> tuple(meta, bundle) },
                ch_baysor_import_input.map { meta, bundle, csv, geojson -> tuple(meta, csv, geojson) }
            )
        }
    }
    
    if (params.runSegger ) {
        SEGGER_CREATE_TRAIN_PREDICT (ch_bundle_path, ch_transcripts_parquet)
    }

    if (params.runProseg) {
        if (effective_proseg_from_resegment) {
            // Run proseg on resegmented transcripts
            PROSEG_RUN(ch_bundle_path_ranger, ch_transcripts_parquet_ranger)

            // Join channels by metadata to ensure correct bundle-segmentation pairing
            ch_proseg_import_input = ch_bundle_path_ranger.join(PROSEG_RUN.out.segmentation, by: 0)

            // Import proseg segmentation into new Xenium bundle
            IMPORT_SEGMENTATION_PROSEG(
                ch_proseg_import_input.map { meta, bundle, csv, geojson -> tuple(meta, bundle) },
                ch_proseg_import_input.map { meta, bundle, csv, geojson -> tuple(meta, csv, geojson) }
            )
        }
        else {
            // Run proseg on original transcripts
            PROSEG_RUN(ch_bundle_path, ch_transcripts_parquet)

            // Join channels by metadata to ensure correct bundle-segmentation pairing
            ch_proseg_import_input = ch_bundle_path.join(PROSEG_RUN.out.segmentation, by: 0)

            // Import proseg segmentation into new Xenium bundle
            IMPORT_SEGMENTATION_PROSEG(
                ch_proseg_import_input.map { meta, bundle, csv, geojson -> tuple(meta, bundle) },
                ch_proseg_import_input.map { meta, bundle, csv, geojson -> tuple(meta, csv, geojson) }
            )
        }
    }
}