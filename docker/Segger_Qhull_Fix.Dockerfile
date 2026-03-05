# Dockerfile to patch segger_dev image with the Qhull precision fix
#
# This fix addresses GitHub issue #107:
# https://github.com/EliHei2/segger_dev/issues/107
#
# The fix adds proper error handling for degenerate point configurations
# that cause Qhull precision errors during Delaunay triangulation when
# generating cell boundaries with save_cell_masks=True.
#
# Build with:
#   cd /path/to/MTA_pipeline_segmentation/docker
#   docker build -f Segger_Qhull_Fix.Dockerfile -t segger_dev:cuda121-fixed .
#
# Then update nextflow.config to use the new image:
#   Change: container = 'danielunyi42/segger_dev:cuda121'
#   To:     container = 'segger_dev:cuda121-fixed'

FROM danielunyi42/segger_dev:cuda121

# Copy the fixed boundary.py file over the existing one
COPY boundary_fixed.py /workspace/segger_dev/src/segger/prediction/boundary.py

# Verify the patch was applied successfully
RUN python -c "from segger.prediction.boundary import generate_boundary; print('Patched boundary module loaded successfully')"
