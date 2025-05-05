FROM rocker/tidyverse

# Install system dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y \
        wget \
        curl \
        unzip \
        libglpk40 \
        python3.9 \
        python3.9-venv \
        python3.9-dev \
        python3-pip \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Set Python 3.9 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1

# Create and activate a Python 3.9 virtual environment
RUN python3.9 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Now pip install into the venv is safe
RUN pip install --no-cache-dir pandas==1.4.4 scipy==1.9.1

# Install XeniumRanger (assumes static URL – update if needed)
RUN wget -O xeniumranger-3.1.1.tar.gz "https://cf.10xgenomics.com/releases/xeniumranger/xeniumranger-3.1.1.tar.gz?Expires=1746153607&Key-Pair-Id=APKAI7S6A5RYOXBWRPDA&Signature=GRuYTIzZL7WR1EPap3EH1Ct2qEBgoqdPN0wGweM-lvzvq4OKrpDOAKwylRuCmCHukP8mKI4G-2IC1M6FTo9FOmZiXTJNm2WI3HqUnxYmPIolGS2sz~ARPePq4b7HXF6qjg07uMCUDNYxuQiqPnVlIM0wnls6mlBr1FcS3pfquzewsT68CC2ncTjzWm9cEiY1I~aqrtDHp-p7Ur3kwC1teAuxmhNNP8n8HstdBbcTdXVa8njLqqkESShskqp7IeOvPz5BZJXYMobPFZiGQekUiL8gVoVmpKXVgHSN6Hdrd2OC1X9e-hqtynKkBcwFr~aLvI7iLe0gJ1HmvOXIdOBO9A__" && \
    tar -xzf xeniumranger-3.1.1.tar.gz && \
    mkdir /opt/xeniumranger && \
    mv xeniumranger-* /opt/xeniumranger && \
    ln -s /opt/xeniumranger/xeniumranger-xenium3.1/xeniumranger /usr/local/bin/

#Install julia
RUN curl -fsSL https://install.julialang.org | sh -s -- -y && \
    ln -s /root/.juliaup/bin/julia /usr/local/bin/julia

# Install Baysor using Julia
RUN julia -e 'using Pkg; Pkg.add(PackageSpec(url="https://github.com/kharchenkolab/Baysor.git")); Pkg.build()' && \
    ln -s ~/.julia/bin/baysor /usr/local/bin/

# R packages
RUN Rscript -e 'install.packages(c("igraph","Seurat", "patchwork", "here", "future", "clustree", "uwot", "Matrix", "nanoparquet"))' 
