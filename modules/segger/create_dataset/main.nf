process SEGGER_CREATE_DATASET {
    tag "$meta.id"
    cpus params.seggerCreateCPUs
    memory "${params.seggerCreateMem} GB"

    input:
    tuple val(meta), path(base_dir)

    output:
    tuple val(meta), path("${meta.id}"), path("num_tx_tokens.txt") , emit: datasetdir
    path("versions.yml")                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
        error "SEGGER_CREATE_DATASET module does not support Conda. Please use Docker / Singularity / Podman instead."
    }

    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def script_path = "/workspace/segger_dev/src/segger/cli/create_dataset_fast.py"
    
    // Check if we should auto-detect or use manual value
    def detect_tokens = params.segger_num_tx_tokens == 0 || params.segger_num_tx_tokens == null

    // check for platform values
    if ( !(params.format in ['xenium']) ) {
        error "${params.format} is an invalid platform type. Please specify xenium, cosmx, or merscope"
    }

    """
    # Detect or use provided num_tx_tokens
    if [ "${detect_tokens}" = "true" ]; then
        echo "Auto-detecting num_tx_tokens from Xenium bundle..."
        NUM_TX_TOKENS=\$(detect_num_tokens.py ${base_dir} --buffer 10)
        
        if [ -z "\$NUM_TX_TOKENS" ]; then
            echo "Warning: Could not detect tokens, using default 313"
            NUM_TX_TOKENS=313
        fi
    else
        echo "Using manually specified num_tx_tokens: ${params.segger_num_tx_tokens}"
        NUM_TX_TOKENS=${params.segger_num_tx_tokens}
    fi
    
    echo "Using num_tx_tokens: \$NUM_TX_TOKENS"
    
    # Save for downstream processes
    echo "\$NUM_TX_TOKENS" > num_tx_tokens.txt
    
    # Create the dataset
    python3 ${script_path} \\
        --base_dir ${base_dir} \\
        --data_dir ${prefix} \\
        --sample_type ${params.format} \\
        --k_bd ${params.segger_k_bd} \\
        --dist_bd ${params.segger_dist_bd} \\
        --k_tx ${params.segger_k_tx} \\
        --dist_tx ${params.segger_dist_tx} \\
        --frac ${params.segger_frac} \\
        --n_workers ${task.cpus} \\
        --tile_width ${params.segger_tile_width} \\
        --tile_height ${params.segger_tile_height} \\
        ${args}

    # Remove tiles with dimensions below minimum threshold to prevent empty edge errors
    # Tiles at tissue boundaries can be very small (e.g., h=1) causing KNN edge computation to fail
    MIN_DIM=${params.segger_min_tile_dim}
    echo "Filtering out tiles with width or height < \${MIN_DIM} pixels..."
    REMOVED=0
    for tiledir in ${prefix}/*/processed; do
        if [ -d "\$tiledir" ]; then
            for f in "\$tiledir"/tiles_*.pt; do
                [ -e "\$f" ] || continue
                fname=\$(basename "\$f")
                # Extract width and height from filename pattern: tiles_x=N_y=N_w=N_h=N.pt
                w=\$(echo "\$fname" | sed -n 's/.*_w=\\([0-9]*\\)_.*/\\1/p')
                h=\$(echo "\$fname" | sed -n 's/.*_h=\\([0-9]*\\)\\.pt/\\1/p')
                if [ -n "\$w" ] && [ -n "\$h" ]; then
                    if [ "\$w" -lt "\$MIN_DIM" ] || [ "\$h" -lt "\$MIN_DIM" ]; then
                        rm "\$f"
                        # Also remove corresponding raw file if it exists
                        rawfile="\${tiledir%/processed}/raw/\$fname"
                        [ -f "\$rawfile" ] && rm "\$rawfile"
                        REMOVED=\$((REMOVED + 1))
                    fi
                fi
            done
        fi
    done
    echo "Removed \${REMOVED} tiles with dimensions below \${MIN_DIM} pixels"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    mkdir -p ${prefix}/
    echo "313" > num_tx_tokens.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
    END_VERSIONS
    """
}