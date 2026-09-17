# XeniumRanger-only container
# Lightweight image for running xeniumranger commands (resegment, import-segmentation, etc.)
#
# Usage:
#   1. Get a fresh download URL from 10x Genomics (they expire quickly)
#   2. Build with:
#      docker build -f docker/XenRanger_v4.0.0.Dockerfile \
#        --build-arg XENIUMRANGER_URL="<paste-fresh-url-here>" \
#        -t xenranger:v4.0.0 .
#   3. When a new version is released, copy this file, update the VERSION arg, and provide the new URL:
#      cp docker/XenRanger_v4.0.0.Dockerfile docker/XenRanger_v4.1.0.Dockerfile
#      docker build -f docker/XenRanger_v4.1.0.Dockerfile \
#        --build-arg XENIUMRANGER_URL="<new-url>" \
#        --build-arg VERSION="4.1.0" \
#        -t xenranger:v4.1.0 .

FROM ubuntu:22.04

ARG XENIUMRANGER_URL
ARG VERSION=4.0.0

# Fail early if no URL provided
RUN if [ -z "$XENIUMRANGER_URL" ]; then \
      echo "ERROR: XENIUMRANGER_URL build arg is required. Get a fresh signed URL from 10x Genomics." >&2; \
      exit 1; \
    fi

# Install minimal system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install XeniumRanger
RUN wget -O xeniumranger.tar.gz "$XENIUMRANGER_URL" && \
    tar -xzf xeniumranger.tar.gz && \
    rm xeniumranger.tar.gz && \
    mkdir -p /opt/xeniumranger && \
    mv xeniumranger-* /opt/xeniumranger/ && \
    # Find and symlink the xeniumranger binary (directory name varies by version)
    ln -s /opt/xeniumranger/*/xeniumranger /usr/local/bin/xeniumranger

# Verify installation
RUN xeniumranger --version

LABEL maintainer="jrrose5" \
      description="XeniumRanger ${VERSION} for Xenium spatial transcriptomics" \
      xeniumranger.version="${VERSION}"
