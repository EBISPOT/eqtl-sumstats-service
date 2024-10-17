#!/bin/bash

COMMIT_SHA=$1

# Load environment file and variables
ENV_FILE="/hps/software/users/parkinso/spot/gwas/dev/scripts/cron/sumstats_service/cel_envs_sandbox"
LOG_LEVEL="info"
MEM="8000"
CLUSTER_QUEUE="standard"
SINGULARITY_CACHEDIR="/nfs/public/rw/gwas/deposition/singularity_cache"
SINGULARITY_REPO="ebispot"
SINGULARITY_IMAGE="eqtl-sumstats-service"

source $ENV_FILE

# Module commands for required tools
lmod_cmd="module load singularity-3.6.4-gcc-9.3.0; module load openjdk-16.0.2-gcc-9.3.0; module load nextflow-21.10.6-gcc-9.3.0; module load curl-7.81.0-gcc-11.2.0; module load wget/1.21.3"

# Pull the Singularity image from the Docker registry
if [ ! -z "${COMMIT_SHA}" ]; then
    echo "START pulling Singularity Image"
    sed -i "s/SINGULARITY_TAG=.*/SINGULARITY_TAG=\"${COMMIT_SHA}\"/g" $ENV_FILE
    singularity pull --dir $SINGULARITY_CACHEDIR docker://${SINGULARITY_REPO}/${SINGULARITY_IMAGE}:${COMMIT_SHA}
    SINGULARITY_TAG=$COMMIT_SHA
    echo "DONE pulling Singularity Image"
else
    echo "COMMIT_SHA not set"
    exit 1
fi

# Set Singularity command
singularity_cmd="singularity exec --env-file $ENV_FILE $SINGULARITY_CACHEDIR/spark-etl-pipeline_${SINGULARITY_TAG}.sif"

# Define the command to run the Spark application inside the Singularity container
# spark_app_cmd="python3 /app/spark_app.py"
spark_app_cmd="python3 --version"

# Gracefully stop any running jobs related to the application
echo "Sending SIGTERM signal to existing jobs"
scancel --name=spark_etl_job --signal=TERM --full

# Submit the Spark ETL job using Slurm
echo "START submitting Spark ETL job"
sbatch --parsable --output="spark_etl_output.o" --error="spark_etl_error.e" --mem=${MEM} --time=2-00:00:00 --job-name=spark_etl_job --wrap="${lmod_cmd}; ${singularity_cmd} ${spark_app_cmd}"
echo "DONE submitting Spark ETL job"
