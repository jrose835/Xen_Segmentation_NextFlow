#!/usr/bin/env python3
"""
Command-line tool for converting segmentation results into Xenium Explorer-compatible Zarr datasets.
"""

import os
import sys
import argparse
import json
from pathlib import Path
import gzip
import pandas as pd
import numpy as np
from scipy.spatial import ConvexHull
from shapely.geometry import MultiPolygon, Polygon
import matplotlib.pyplot as plt
from tqdm import tqdm
from typing import Dict, Any, Optional, List, Tuple
from segger.prediction.boundary import generate_boundary
from zarr.storage import ZipStore
import zarr


def get_flatten_version(polygon_vertices: List[List[Tuple[float, float]]], max_value: int = 21) -> np.ndarray:
    """Standardize list of polygon vertices to a fixed shape.

    Args:
        polygon_vertices (List[List[Tuple[float, float]]]): List of polygon coordinate lists.
        max_value (int): Max number of coordinates per polygon.

    Returns:
        np.ndarray: Padded or truncated list of polygon vertices.
    """
    flattened = []
    
    for vertices in polygon_vertices:
        # Convert to numpy array if not already
        if isinstance(vertices, np.ndarray):
            vertices_array = vertices
        else:
            vertices_array = np.array(vertices)
        
        # Ensure it's 2D with shape (n, 2)
        if vertices_array.ndim == 1:
            # Handle empty or malformed arrays
            if len(vertices_array) == 0:
                vertices_array = np.array([]).reshape(0, 2)
            else:
                # Try to reshape if it's a flattened array
                try:
                    vertices_array = vertices_array.reshape(-1, 2)
                except:
                    # If reshape fails, skip this polygon
                    print(f"Warning: Could not reshape vertices array with shape {vertices_array.shape}")
                    vertices_array = np.array([[0.0, 0.0]])
        
        # Get the number of vertices
        n_vertices = len(vertices_array)
        
        if n_vertices > max_value:
            # Truncate to max_value
            result = vertices_array[:max_value]
        elif n_vertices < max_value:
            # Pad with zeros to reach max_value
            padding = np.zeros((max_value - n_vertices, 2), dtype=np.float32)
            if n_vertices > 0:
                result = np.vstack([vertices_array, padding])
            else:
                result = padding
        else:
            # Exactly max_value vertices
            result = vertices_array
        
        # Ensure the result has the correct shape and type
        result = np.array(result, dtype=np.float32)
        if result.shape != (max_value, 2):
            print(f"Warning: Unexpected shape {result.shape}, expected ({max_value}, 2)")
            # Force to correct shape
            result = np.zeros((max_value, 2), dtype=np.float32)
        
        flattened.append(result)
    
    # Stack all polygons into a single array
    return np.array(flattened, dtype=np.float32)


def seg2explorer(
    seg_df: pd.DataFrame,
    source_path: str,
    output_dir: str,
    cells_filename: str = "seg_cells",
    analysis_filename: str = "seg_analysis",
    xenium_filename: str = "seg_experiment.xenium",
    analysis_df: Optional[pd.DataFrame] = None,
    draw: bool = False,
    cell_id_columns: str = "seg_cell_id",
    area_low: float = 10,
    area_high: float = 100,
) -> None:
    """Convert segmentation results into a Xenium Explorer-compatible Zarr dataset.

    Args:
        seg_df (pd.DataFrame): Segmented transcript dataframe.
        source_path (str): Path to the original Zarr store.
        output_dir (str): Output directory to save new Zarr and Xenium files.
        cells_filename (str): Filename prefix for cell Zarr file.
        analysis_filename (str): Filename prefix for cell group Zarr file.
        xenium_filename (str): Output experiment filename for Xenium.
        analysis_df (Optional[pd.DataFrame]): Optional dataframe with cluster annotations.
        draw (bool): Whether to draw polygons (not used currently).
        cell_id_columns (str): Column containing cell IDs.
        area_low (float): Minimum area threshold to include cells.
        area_high (float): Maximum area threshold to include cells.
    """
    source_path = Path(source_path)
    storage = Path(output_dir)
    
    # Create output directory if it doesn't exist
    storage.mkdir(parents=True, exist_ok=True)

    cell_id2old_id: Dict[int, Any] = {}
    cell_id: List[int] = []
    cell_summary: List[Dict[str, Any]] = []
    polygon_num_vertices: List[List[int]] = [[], []]
    polygon_vertices: List[List[Any]] = [[], []]
    seg_mask_value: List[int] = []

    grouped_by = seg_df.groupby(cell_id_columns)

    for cell_incremental_id, (seg_cell_id, seg_cell) in tqdm(
        enumerate(grouped_by), total=len(grouped_by), desc="Processing cells"
    ):
        if len(seg_cell) < 5:
            continue

        cell_convex_hull = generate_boundary(seg_cell)
        if cell_convex_hull is None or not isinstance(cell_convex_hull, Polygon):
            continue

        if not (area_low <= cell_convex_hull.area <= area_high):
            continue

        uint_cell_id = cell_incremental_id + 1
        cell_id2old_id[uint_cell_id] = seg_cell_id

        seg_nucleous = seg_cell[seg_cell["overlaps_nucleus"] == 1]
        nucleus_convex_hull = None
        if len(seg_nucleous) >= 3:
            try:
                nucleus_convex_hull = ConvexHull(seg_nucleous[["x_location", "y_location"]])
            except Exception:
                pass

        cell_id.append(uint_cell_id)
        cell_summary.append(
            {
                "cell_centroid_x": seg_cell["x_location"].mean(),
                "cell_centroid_y": seg_cell["y_location"].mean(),
                "cell_area": cell_convex_hull.area,
                "nucleus_centroid_x": seg_cell["x_location"].mean(),
                "nucleus_centroid_y": seg_cell["y_location"].mean(),
                "nucleus_area": cell_convex_hull.area,
                "z_level": (seg_cell.z_location.mean() // 3).round(0) * 3,
            }
        )
        polygon_num_vertices[0].append(len(cell_convex_hull.exterior.coords))
        polygon_num_vertices[1].append(
            len(nucleus_convex_hull.vertices) if nucleus_convex_hull else 0
        )
        polygon_vertices[0].append(list(cell_convex_hull.exterior.coords))
        
        # Handle nucleus vertices properly
        if nucleus_convex_hull is not None:
            nucleus_vertices = seg_nucleous[["x_location", "y_location"]].values[nucleus_convex_hull.vertices]
            polygon_vertices[1].append(nucleus_vertices.tolist())
        else:
            # Append empty array with correct shape for nucleus
            polygon_vertices[1].append([])
        seg_mask_value.append(uint_cell_id)

    cell_polygon_vertices = get_flatten_version(polygon_vertices[0], max_value=128)
    nucl_polygon_vertices = get_flatten_version(polygon_vertices[1], max_value=128)

    cells = {
        "cell_id": np.array(
            [np.array(cell_id), np.ones(len(cell_id))], dtype=np.uint32
        ).T,
        "cell_summary": pd.DataFrame(cell_summary).values.astype(np.float64),
        "polygon_num_vertices": np.array(
            [
                [min(x + 1, x + 1) for x in polygon_num_vertices[1]],
                [min(x + 1, x + 1) for x in polygon_num_vertices[0]],
            ],
            dtype=np.int32,
        ),
        "polygon_vertices": np.array(
            [nucl_polygon_vertices, cell_polygon_vertices], dtype=np.float32
        ),
        "seg_mask_value": np.array(seg_mask_value, dtype=np.int32),
    }

    source_zarr_store = ZipStore(source_path / "cells.zarr.zip", mode="r")
    existing_store = zarr.open(source_zarr_store, mode="r")
    new_store = zarr.open(storage / f"{cells_filename}.zarr.zip", mode="w")
    new_store["cell_id"] = cells["cell_id"]
    new_store["polygon_num_vertices"] = cells["polygon_num_vertices"]
    new_store["polygon_vertices"] = cells["polygon_vertices"]
    new_store["seg_mask_value"] = cells["seg_mask_value"]
    new_store.attrs.update(existing_store.attrs)
    new_store.attrs["number_cells"] = len(cells["cell_id"])
    new_store.store.close()

    if analysis_df is None:
        analysis_df = pd.DataFrame(
            [cell_id2old_id[i] for i in cell_id], columns=[cell_id_columns]
        )
        analysis_df["default"] = "seg"

    zarr_df = pd.DataFrame(
        [cell_id2old_id[i] for i in cell_id], columns=[cell_id_columns]
    )
    clustering_df = pd.merge(zarr_df, analysis_df, how="left", on=cell_id_columns)
    clusters_names = [col for col in analysis_df.columns if col != cell_id_columns]

    clusters_dict = {
        cluster: {
            label: idx + 1
            for idx, label in enumerate(
                sorted(np.unique(clustering_df[cluster].dropna()))
            )
        }
        for cluster in clusters_names
    }

    new_zarr = zarr.open(storage / f"{analysis_filename}.zarr.zip", mode="w")
    new_zarr.create_group("/cell_groups")
    for i, cluster in enumerate(clusters_names):
        new_zarr["cell_groups"].create_group(str(i))
        group_values = [clusters_dict[cluster].get(x, 0) for x in clustering_df[cluster]]
        indices, indptr = get_indices_indptr(np.array(group_values))
        new_zarr["cell_groups"][str(i)]["indices"] = indices
        new_zarr["cell_groups"][str(i)]["indptr"] = indptr

    new_zarr["cell_groups"].attrs.update(
        {
            "major_version": 1,
            "minor_version": 0,
            "number_groupings": len(clusters_names),
            "grouping_names": clusters_names,
            "group_names": [
                sorted(clusters_dict[cluster], key=clusters_dict[cluster].get)
                for cluster in clusters_names
            ],
        }
    )
    new_zarr.store.close()

    generate_experiment_file(
        template_path=source_path / "experiment.xenium",
        output_path=storage / xenium_filename,
        cells_name=cells_filename,
        analysis_name=analysis_filename,
    )
    
    print(f"✓ Successfully created Xenium Explorer files in {output_dir}")
    print(f"  - Cells: {cells_filename}.zarr.zip")
    print(f"  - Analysis: {analysis_filename}.zarr.zip")
    print(f"  - Experiment: {xenium_filename}")


def str_to_uint32(cell_id_str: str) -> Tuple[int, int]:
    """Convert a string cell ID back to uint32 format.

    Args:
        cell_id_str (str): The cell ID in string format.

    Returns:
        Tuple[int, int]: The cell ID in uint32 format and the dataset suffix.
    """
    prefix, suffix = cell_id_str.split("-")
    str_to_hex_mapping = {
        "a": "0",
        "b": "1",
        "c": "2",
        "d": "3",
        "e": "4",
        "f": "5",
        "g": "6",
        "h": "7",
        "i": "8",
        "j": "9",
        "k": "a",
        "l": "b",
        "m": "c",
        "n": "d",
        "o": "e",
        "p": "f",
    }
    hex_prefix = "".join([str_to_hex_mapping[char] for char in prefix])
    cell_id_uint32 = int(hex_prefix, 16)
    dataset_suffix = int(suffix)
    return cell_id_uint32, dataset_suffix


def get_indices_indptr(input_array: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Get the indices and indptr arrays for sparse matrix representation.

    Args:
        input_array (np.ndarray): The input array containing cluster labels.

    Returns:
        Tuple[np.ndarray, np.ndarray]: The indices and indptr arrays.
    """
    clusters = sorted(np.unique(input_array[input_array != 0]))
    indptr = np.zeros(len(clusters), dtype=np.uint32)
    indices = []

    for cluster in clusters:
        cluster_indices = np.where(input_array == cluster)[0]
        indptr[cluster - 1] = len(indices)
        indices.extend(cluster_indices)

    indices.extend(-np.zeros(len(input_array[input_array == 0])))
    indices = np.array(indices, dtype=np.int32).astype(np.uint32)
    return indices, indptr


def generate_experiment_file(
    template_path: str,
    output_path: str,
    cells_name: str = "seg_cells",
    analysis_name: str = "seg_analysis",
) -> None:
    """Generate the experiment file for Xenium.

    Args:
        template_path (str): The path to the template file.
        output_path (str): The path to the output file.
        cells_name (str): The name of the cells file.
        analysis_name (str): The name of the analysis file.
    """
    import json

    with open(template_path) as f:
        experiment = json.load(f)

    experiment["images"].pop("morphology_filepath", None)
    experiment["images"].pop("morphology_focus_filepath", None)

    experiment["xenium_explorer_files"][
        "cells_zarr_filepath"
    ] = f"{cells_name}.zarr.zip"
    experiment["xenium_explorer_files"].pop("cell_features_zarr_filepath", None)
    experiment["xenium_explorer_files"][
        "analysis_zarr_filepath"
    ] = f"{analysis_name}.zarr.zip"

    with open(output_path, "w") as f:
        json.dump(experiment, f, indent=2)


def main():
    """Main function to parse arguments and run seg2explorer."""
    parser = argparse.ArgumentParser(
        description="Convert segmentation results into Xenium Explorer-compatible Zarr datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with required arguments
  %(prog)s segmentation.parquet /path/to/source ./output

  # With custom filenames and thresholds
  %(prog)s segmentation.parquet /path/to/source ./output \\
    --cells-filename my_cells \\
    --analysis-filename my_analysis \\
    --xenium-filename my_experiment.xenium \\
    --area-low 5 \\
    --area-high 200

  # With analysis dataframe
  %(prog)s segmentation.parquet /path/to/source ./output \\
    --analysis-df clusters.parquet \\
    --cell-id-column custom_cell_id
        """
    )
    
    # Required arguments
    parser.add_argument(
        "seg_df",
        type=str,
        help="Path to segmented transcript dataframe (Parquet format)"
    )
    parser.add_argument(
        "source_path",
        type=str,
        help="Path to the original Zarr store directory"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Output directory to save new Zarr and Xenium files"
    )
    
    # Optional arguments
    parser.add_argument(
        "--cells-filename",
        type=str,
        default="seg_cells",
        help="Filename prefix for cell Zarr file (default: seg_cells)"
    )
    parser.add_argument(
        "--analysis-filename",
        type=str,
        default="seg_analysis",
        help="Filename prefix for cell group Zarr file (default: seg_analysis)"
    )
    parser.add_argument(
        "--xenium-filename",
        type=str,
        default="seg_experiment.xenium",
        help="Output experiment filename for Xenium (default: seg_experiment.xenium)"
    )
    parser.add_argument(
        "--analysis-df",
        type=str,
        default=None,
        help="Optional path to dataframe with cluster annotations (Parquet format)"
    )
    parser.add_argument(
        "--draw",
        action="store_true",
        help="Whether to draw polygons (currently not used)"
    )
    parser.add_argument(
        "--cell-id-column",
        type=str,
        default="seg_cell_id",
        help="Column containing cell IDs (default: seg_cell_id)"
    )
    parser.add_argument(
        "--area-low",
        type=float,
        default=10,
        help="Minimum area threshold to include cells (default: 10)"
    )
    parser.add_argument(
        "--area-high",
        type=float,
        default=100,
        help="Maximum area threshold to include cells (default: 100)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not args.seg_df.endswith('.parquet'):
        raise ValueError(f"Input file must be in Parquet format (*.parquet). Got: {args.seg_df}")
    
    if not Path(args.seg_df).exists():
        raise FileNotFoundError(f"Segmentation file not found: {args.seg_df}")
    
    # Load segmentation dataframe
    if args.verbose:
        print(f"Loading segmentation data from {args.seg_df}...")
    
    try:
        seg_df = pd.read_parquet(args.seg_df)
    except Exception as e:
        raise ValueError(f"Failed to read Parquet file {args.seg_df}: {e}")
    
    if args.verbose:
        print(f"Loaded {len(seg_df):,} rows from segmentation dataframe")
        print(f"Columns: {', '.join(seg_df.columns)}")
    
    # Load analysis dataframe if provided
    analysis_df = None
    if args.analysis_df:
        if not args.analysis_df.endswith('.parquet'):
            raise ValueError(f"Analysis file must be in Parquet format (*.parquet). Got: {args.analysis_df}")
        
        if not Path(args.analysis_df).exists():
            raise FileNotFoundError(f"Analysis file not found: {args.analysis_df}")
        
        if args.verbose:
            print(f"Loading analysis data from {args.analysis_df}...")
        
        try:
            analysis_df = pd.read_parquet(args.analysis_df)
        except Exception as e:
            raise ValueError(f"Failed to read Parquet file {args.analysis_df}: {e}")
        
        if args.verbose:
            print(f"Loaded analysis dataframe with {len(analysis_df):,} rows")
            print(f"Columns: {', '.join(analysis_df.columns)}")
    
    # Validate source path
    source_path = Path(args.source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source path does not exist: {args.source_path}")
    
    if not (source_path / "cells.zarr.zip").exists():
        raise FileNotFoundError(f"cells.zarr.zip not found in {args.source_path}")
    
    if not (source_path / "experiment.xenium").exists():
        raise FileNotFoundError(f"experiment.xenium not found in {args.source_path}")
    
    # Run seg2explorer
    if args.verbose:
        print(f"\nStarting conversion...")
        print(f"  Source: {args.source_path}")
        print(f"  Output: {args.output_dir}")
        print(f"  Cell ID column: {args.cell_id_column}")
        print(f"  Area thresholds: {args.area_low} - {args.area_high}")
    
    try:
        seg2explorer(
            seg_df=seg_df,
            source_path=args.source_path,
            output_dir=args.output_dir,
            cells_filename=args.cells_filename,
            analysis_filename=args.analysis_filename,
            xenium_filename=args.xenium_filename,
            analysis_df=analysis_df,
            draw=args.draw,
            cell_id_columns=args.cell_id_column,
            area_low=args.area_low,
            area_high=args.area_high,
        )
    except Exception as e:
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()