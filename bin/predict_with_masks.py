#!/usr/bin/env python3
"""
Modified Segger predict script that includes save_cell_masks option.
This wraps the original predict_fast.py functionality with the additional parameter.

Usage: This script is called by the SEGGER_PREDICT Nextflow module when
       save_cell_masks is enabled in nextflow.config.
"""

import click
from segger.training.segger_data_module import SeggerDataModule
from segger.prediction.predict_parquet import segment, load_model
from pathlib import Path
import logging

help_msg = "Run the Segger segmentation model with save_cell_masks option."


@click.command(name="run_segmentation", help=help_msg)
@click.option("--segger_data_dir", type=Path, required=True, help="Directory containing the processed Segger dataset.")
@click.option("--models_dir", type=Path, required=True, help="Directory containing the trained models.")
@click.option("--benchmarks_dir", type=Path, required=True, help="Directory to save the segmentation results.")
@click.option("--transcripts_file", type=str, required=True, help="Path to the transcripts file.")
@click.option("--batch_size", type=int, default=1, help="Batch size for processing.")
@click.option("--num_workers", type=int, default=1, help="Number of workers for data loading.")
@click.option("--model_version", type=int, default=0, help="Model version to load.")
@click.option("--save_tag", type=str, default="segger_embedding_1001_0.5", help="Tag for saving segmentation results.")
@click.option("--min_transcripts", type=int, default=5, help="Minimum number of transcripts for segmentation.")
@click.option("--cell_id_col", type=str, default="segger_cell_id", help="Column name for cell IDs.")
@click.option("--use_cc", type=bool, default=False, help="Use connected components if specified.")
@click.option("--knn_method", type=str, default="cuda", help="Method for KNN computation.")
@click.option("--file_format", type=str, default="anndata", help="File format for output data.")
@click.option("--k_bd", type=int, default=4, help="K value for boundary computation.")
@click.option("--dist_bd", type=float, default=12.0, help="Distance for boundary computation.")
@click.option("--k_tx", type=int, default=5, help="K value for transcript computation.")
@click.option("--dist_tx", type=float, default=5.0, help="Distance for transcript computation.")
@click.option("--save_cell_masks", is_flag=True, default=False, help="Save cell masks/boundaries as GeoParquet.")
def run_segmentation(
    segger_data_dir,
    models_dir,
    benchmarks_dir,
    transcripts_file,
    batch_size,
    num_workers,
    model_version,
    save_tag,
    min_transcripts,
    cell_id_col,
    use_cc,
    knn_method,
    file_format,
    k_bd,
    dist_bd,
    k_tx,
    dist_tx,
    save_cell_masks
):
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Initializing Segger data module...")
    dm = SeggerDataModule(
        data_dir=segger_data_dir,
        batch_size=batch_size,
        num_workers=num_workers,
    )
    dm.setup()

    logger.info("Loading the model...")
    model_path = Path(models_dir) / "lightning_logs" / f"version_{model_version}"
    model = load_model(model_path / "checkpoints")

    logger.info(f"Running segmentation with save_cell_masks={save_cell_masks}...")

    try:
        segment(
            model,
            dm,
            save_dir=benchmarks_dir,
            seg_tag=save_tag,
            transcript_file=transcripts_file,
            file_format=file_format,
            receptive_field={"k_bd": k_bd, "dist_bd": dist_bd, "k_tx": k_tx, "dist_tx": dist_tx},
            min_transcripts=min_transcripts,
            cell_id_col=cell_id_col,
            use_cc=use_cc,
            knn_method=knn_method,
            save_cell_masks=save_cell_masks,
            verbose=True,
        )
    except Exception as e:
        if save_cell_masks and "qhull" in str(e).lower():
            # Retry without save_cell_masks if boundary generation fails
            logger.warning(f"Cell mask generation failed: {e}")
            logger.warning("Retrying without save_cell_masks...")
            segment(
                model,
                dm,
                save_dir=benchmarks_dir,
                seg_tag=save_tag,
                transcript_file=transcripts_file,
                file_format=file_format,
                receptive_field={"k_bd": k_bd, "dist_bd": dist_bd, "k_tx": k_tx, "dist_tx": dist_tx},
                min_transcripts=min_transcripts,
                cell_id_col=cell_id_col,
                use_cc=use_cc,
                knn_method=knn_method,
                save_cell_masks=False,
                verbose=True,
            )
        else:
            raise

    logger.info("Segmentation completed.")


if __name__ == "__main__":
    run_segmentation()
