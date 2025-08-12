process SEGGER_TRAIN {
    tag "$meta.id"
    label 'process_high'
    cpus params.seggerTrainCPUs
    memory "${params.seggerTrainMem} GB"

    input:
    tuple val(meta), path(dataset_dir), path(num_tokens_file)

    output:
    tuple val(meta), path("${meta.id}_trained_models")   , emit: trained_models
    path("versions.yml")                                 , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
        error "SEGGER_TRAIN module does not support Conda. Please use Docker / Singularity / Podman instead."
    }

    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def script_path = "/workspace/segger_dev/src/segger/cli/train_model.py"

    """
    # Read the pre-calculated num_tx_tokens
    NUM_TX_TOKENS=\$(cat ${num_tokens_file})
    echo "Training with num_tx_tokens: \$NUM_TX_TOKENS"

    # Run training with the determined num_tx_tokens
    python3 ${script_path} \\
        --dataset_dir ${dataset_dir} \\
        --models_dir ${prefix}_trained_models \\
        --sample_tag ${prefix} \\
        --num_workers ${task.cpus} \\
        --batch_size ${params.segger_batch_size} \\
        --max_epochs ${params.segger_max_epochs} \\
        --devices ${params.segger_devices} \\
        --accelerator ${params.segger_accelerator} \\
        --num_tx_tokens \$NUM_TX_TOKENS \\
        ${args}

    # Save the token count with the model for reference
    echo "\$NUM_TX_TOKENS" > ${prefix}_trained_models/num_tx_tokens.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    mkdir -p ${prefix}_trained_models/
    touch ${prefix}_trained_models/fakefile.txt
    echo "313" > ${prefix}_trained_models/num_tx_tokens.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        segger: 0.1.0
    END_VERSIONS
    """
}