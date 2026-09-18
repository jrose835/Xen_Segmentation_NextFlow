#!/usr/bin/env python3
"""
Fix negative grid tile indices in xenium bundle for XeniumRanger compatibility.

XeniumRanger import-segmentation fails when the xenium bundle's
transcripts.zarr.zip contains grid tiles with negative indices (caused by
transcripts at slightly negative spatial coordinates). This script:

1. Creates a fixed xenium bundle where transcripts.zarr.zip has negative grid
   tiles removed and stale density arrays stripped (XeniumRanger recomputes them).
2. Filters the proseg transcript-metadata.csv to remove any transcript IDs that
   were in the removed grid tiles (prevents TranscriptIdNotFound errors).
3. Passes cell-polygons.geojson through unchanged — XeniumRanger can handle
   slightly negative polygon coordinates, and clamping them to 0 would distort
   cell shapes and corrupt centroid/area calculations.

Note: XeniumRanger only reads (is_noise, transcript_id, cell) from the CSV.
The x/y/z coordinate columns are ignored entirely, so coordinate clamping in
the CSV is unnecessary.

If no negative grid tiles are found, all files are symlinked through unchanged.
"""

import argparse
import csv
import json
import os
import sys
import zipfile


def has_negative_grid_tiles(zarr_zip_path):
    """Check if transcripts.zarr.zip has any negative-indexed grid tiles."""
    with zipfile.ZipFile(zarr_zip_path, "r") as zf:
        for name in zf.namelist():
            if name.startswith("grids/0/-"):
                return True
    return False


def _collect_removed_transcript_ids(zarr_zip_path, neg_tile_names):
    """Read transcript IDs from negative grid tiles using zarr.

    The id array has shape (N, 2) with dtype uint32. Column 0 is the low 32
    bits and column 1 is the FOV index. The full 64-bit transcript_id is
    reconstructed as (fov_index << 32) | id_low.
    """
    import zarr
    from zarr.storage import ZipStore

    ids = set()
    store = ZipStore(zarr_zip_path, mode="r")
    try:
        root = zarr.open(store, mode="r")
        grid = root["grids/0"]
        for tile_name in neg_tile_names:
            if tile_name not in grid:
                continue
            id_arr = grid[tile_name]["id"][:]
            for i in range(id_arr.shape[0]):
                id_low = int(id_arr[i, 0])
                fov_index = int(id_arr[i, 1])
                transcript_id = (fov_index << 32) | id_low
                ids.add(str(transcript_id))
    finally:
        store.close()

    return ids


def fix_transcripts_zarr(src_path, dst_path):
    """Copy transcripts.zarr.zip, removing negative grid tiles and stale density arrays.

    Returns a set of transcript IDs that were in the removed tiles (so they can
    be filtered from the CSV).

    Removes:
    - Grid tiles with negative indices (grids/0/-*)
    - metrics_density array (pre-computed over full spatial extent)

    Keeps density/gene and density/codeword — XeniumRanger needs their attributes
    for spatial grid layout, and recomputes the actual CSR data from scratch.

    XeniumRanger recomputes metrics_density during the relabel step, so stripping
    it avoids the "failed to compute metrics density chunk sub grid" error.
    """
    neg_prefixes = []
    removed_count = 0
    removed_ids = set()

    # Only strip metrics_density (stale 3D array). Keep density/gene and
    # density/codeword — XeniumRanger needs their .zattrs for grid layout.
    density_prefixes = ("metrics_density/",)

    with zipfile.ZipFile(src_path, "r") as src:
        # Identify negative grid tile prefixes and tile names
        neg_tile_names = []
        for name in src.namelist():
            if name.startswith("grids/0/-"):
                parts = name.split("/")
                if len(parts) >= 3:
                    prefix = f"grids/0/{parts[2]}/"
                    tile_name = parts[2]
                    if prefix not in neg_prefixes:
                        neg_prefixes.append(prefix)
                        neg_tile_names.append(tile_name)

        if not neg_prefixes:
            return removed_ids

        # Collect transcript IDs from tiles being removed (using zarr for proper decompression)
        removed_ids = _collect_removed_transcript_ids(src_path, neg_tile_names)

        # Count transcripts being removed
        for prefix in neg_prefixes:
            id_zarray = f"{prefix}id/.zarray"
            if id_zarray in src.namelist():
                meta = json.loads(src.read(id_zarray))
                shape = meta.get("shape", [0])
                removed_count += shape[0]

        print(
            f"Removing {len(neg_prefixes)} negative grid tiles "
            f"({removed_count} transcripts, IDs: {removed_ids})",
            file=sys.stderr,
        )

        with zipfile.ZipFile(dst_path, "w", compression=zipfile.ZIP_STORED) as dst:
            for item in src.infolist():
                # Skip entries under negative grid tiles
                skip = False
                for prefix in neg_prefixes:
                    if item.filename.startswith(prefix):
                        skip = True
                        break
                if skip:
                    continue

                # Skip pre-computed density arrays (will be recomputed)
                if item.filename.startswith(density_prefixes):
                    continue

                data = src.read(item.filename)

                # Update number_rnas in root .zattrs
                if item.filename == ".zattrs" and removed_count > 0:
                    attrs = json.loads(data)
                    if "number_rnas" in attrs:
                        attrs["number_rnas"] -= removed_count
                        data = json.dumps(attrs).encode()

                dst.writestr(item, data)

    return removed_ids


def filter_csv(src_path, dst_path, excluded_ids):
    """Copy CSV, removing rows whose transcript_id is in excluded_ids.

    XeniumRanger only reads (is_noise, transcript_id, cell) from this CSV —
    the x/y/z columns are completely ignored. So we don't modify coordinates,
    we only filter out transcripts that no longer exist in the patched zarr.
    """
    removed = 0
    kept = 0
    with open(src_path, "r") as fin, open(dst_path, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row["transcript_id"] in excluded_ids:
                removed += 1
                continue
            writer.writerow(row)
            kept += 1

    if removed > 0:
        print(
            f"Filtered {removed} transcripts from CSV (removed from zarr), "
            f"{kept} remaining",
            file=sys.stderr,
        )
    return removed > 0


def link_or_copy(src, dst):
    """Materialise `src` at `dst` as a real filesystem entry, never a symlink.

    Symlinks are resolved first (Nextflow stages inputs as symlinks to their resolved paths).
    Files are hard-linked when the filesystem and permissions allow (same device, and
    fs.protected_hardlinks lets the owner link read-only files), else copied. Directories are
    recreated with the same rule per file. A symlinked bundle only works inside a container when
    every link target happens to be bound under the same path; XeniumRanger 4.0 resolves the
    bundle path and then finds nothing (seen 2026-09-17 on PROTSEQ under apptainer:
    "Expected output bundle to contain 'transcripts.zarr.zip'"), so links are avoided entirely.
    """
    import shutil

    real = os.path.realpath(src)
    if os.path.isdir(real):
        os.makedirs(dst, exist_ok=True)
        for name in os.listdir(real):
            link_or_copy(os.path.join(real, name), os.path.join(dst, name))
        return
    try:
        os.link(real, dst)
    except OSError:
        shutil.copy2(real, dst)


def create_fixed_bundle(original_bundle, output_bundle, fix_zarr=False):
    """Create a bundle directory holding every file of the original as a hard link (or copy),
    never a symlink; see link_or_copy.

    If fix_zarr is True, transcripts.zarr.zip is replaced with a patched copy.
    Returns a set of transcript IDs that were removed from the zarr.
    """
    os.makedirs(output_bundle, exist_ok=True)
    removed_ids = set()

    for item in os.listdir(original_bundle):
        src = os.path.join(original_bundle, item)
        dst = os.path.join(output_bundle, item)

        if item == "transcripts.zarr.zip" and fix_zarr:
            # Will be created separately
            continue

        if not os.path.exists(dst):
            link_or_copy(src, dst)

    if fix_zarr:
        src_zarr = os.path.join(original_bundle, "transcripts.zarr.zip")
        dst_zarr = os.path.join(output_bundle, "transcripts.zarr.zip")
        removed_ids = fix_transcripts_zarr(src_zarr, dst_zarr)

    return removed_ids


def main():
    parser = argparse.ArgumentParser(
        description="Fix negative grid tiles in xenium bundle for XeniumRanger"
    )
    parser.add_argument("--bundle", required=True, help="Path to xenium bundle")
    parser.add_argument("--csv", required=True, help="Path to transcript-metadata.csv")
    parser.add_argument("--geojson", required=True, help="Path to cell-polygons.geojson")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for fixed files",
    )
    parser.add_argument(
        "--drop-experiment-keys",
        default="",
        help="comma-separated keys to remove from experiment.xenium in the fixed bundle: fields "
        "the installed XeniumRanger does not recognise (e.g. segmented_cell_boundary_large_frac "
        "written by xenium-4.0.2.2 onboard analysis, rejected by XeniumRanger 4.0.1 with "
        "'unrecognized keys ... for PAFinalizeExperimentXenium')",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    out_csv = os.path.join(args.output_dir, "transcript-metadata.csv")
    out_geojson = os.path.join(args.output_dir, "cell-polygons.geojson")
    out_bundle = os.path.join(args.output_dir, "fixed_bundle")

    # Check if the bundle has negative grid tiles
    zarr_path = os.path.join(args.bundle, "transcripts.zarr.zip")
    needs_zarr_fix = os.path.exists(zarr_path) and has_negative_grid_tiles(zarr_path)

    if needs_zarr_fix:
        print(
            "Negative grid tiles detected in transcripts.zarr.zip, creating fixed bundle",
            file=sys.stderr,
        )

    # Create fixed bundle (symlinks + optional zarr fix)
    # Returns IDs of transcripts removed from zarr
    removed_ids = create_fixed_bundle(args.bundle, out_bundle, fix_zarr=needs_zarr_fix)
    drop = [k for k in args.drop_experiment_keys.split(",") if k.strip()]
    if drop:
        exp = os.path.join(out_bundle, "experiment.xenium")
        with open(exp) as fh:
            meta = json.load(fh)
        present = [k for k in drop if k in meta]
        for k in present:
            del meta[k]
        # the fixed bundle holds a hard link or copy; write a fresh file so the original is untouched
        tmp = exp + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(meta, fh, indent=2)
        os.replace(tmp, exp)
        print(f"experiment.xenium: dropped {len(present)} key(s) unknown to the installed XeniumRanger: {present}")

    # Filter CSV to remove transcripts that no longer exist in the zarr
    if removed_ids:
        filter_csv(args.csv, out_csv, removed_ids)
    else:
        # No filtering needed — pass through as a hard link or copy, never a symlink
        link_or_copy(args.csv, out_csv)

    # Pass GeoJSON through unchanged — XeniumRanger handles slightly negative
    # polygon coordinates fine, and clamping to 0 would distort cell shapes
    link_or_copy(args.geojson, out_geojson)

    if not needs_zarr_fix:
        print("No negative grid tiles found, all files passed through", file=sys.stderr)
    else:
        print("Bundle fix complete", file=sys.stderr)


if __name__ == "__main__":
    main()
