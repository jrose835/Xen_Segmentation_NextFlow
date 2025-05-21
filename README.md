# Xen_Segmentation_NextFlow
A custom Nextflow pipeline for generating alternative cell segmentations for 10x Xenium spatial transcriptomic data

> [!NOTE]
> This is workflow follows SOME but NOT ALL of the nf-core template/guidelines. It's really just a custom Nextflow built for specific needs

## Overivew 
This workflow allows for re-segmenting 10x Xenium data via:

- Alternative settings from `xeniumranger resegment`
    - i.e. DAPI/nuclear only
- Baysor (https://github.com/kharchenkolab/Baysor)

## Features

Transcript inputs for Baysor are split into relatively even sized "chunks" and run in parrallel. 

This **greatly improves runtime** for large Xenium experiments at the cost of some oversegmentation for cells found along chunk boundaries
 
## Workflow DAG

```mermaid
flowchart TB
    subgraph " "
    subgraph params
    v0["input"]
    v1["runRanger"]
    v2["runBaysor"]
    end
    v6([RESEGMENT_10X])
    v9([CALC_SPLITS])
    v10([BAYSOR_PARALLEL])
    v11([IMPORT_SEGMENTATION])
    v0 --> v6
    v6 --> v9
    v6 --> v10
    v9 --> v10
    v6 --> v11
    v10 --> v11
    end
```

### Baysor Parallel DAG

```mermaid
flowchart TB
    subgraph BAYSOR_PARALLEL
    subgraph take
    v0["ch_transcripts_parquet"]
    v1["ch_splits_csv"]
    end
    v4([FILTER_TRANSCRIPTS])
    v5([BAYSOR_RUN])
    v9([RECONSTRUCT_SEGMENTATION])
    subgraph emit
    v10["segmentation"]
    end
    v0 --> v4
    v1 --> v4
    v4 --> v5
    v5 --> v9
    v9 --> v10
    end
```