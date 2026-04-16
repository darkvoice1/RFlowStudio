FROM r-base:latest

RUN apt-get update && apt-get install -y \
    file \
    libcurl4-openssl-dev \
    libsodium-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN Rscript -e "install.packages(c('jsonlite', 'plumber'), repos='https://cloud.r-project.org')"

WORKDIR /opt/rflow/r-analysis

COPY backend/app/services/dataset/analysis/r /opt/rflow/r-analysis

EXPOSE 8090

CMD ["Rscript", "/opt/rflow/r-analysis/dataset_analysis_api_server.R", "8090"]
