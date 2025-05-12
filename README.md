# Xen_Segmentation_NextFlow
A custom Nextflow pipeline for generating alternative cell segmenations for 10x Xenium spatial transcriptomic data

## Overivew 
This workflow allows for re-segmenting 10x Xenium data via:

- Alternative settings from `xeniumranger resegment`
    - i.e. DAPI/nuclear only
- Baysor (https://github.com/kharchenkolab/Baysor)
 
## Workflow DAG
![dag](assets/dag.png)

> [!NOTE]
> This is **NOT** intended to be a modular/nf-core-templated workflow. Just a custom Nextflow built for specific needs
