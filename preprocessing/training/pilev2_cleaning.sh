#!/bin/bash -l
# ======= SLURM OPTIONS ======= (user input required)
### See inline comments for what each option means
#SBATCH --partition=cpu128
### Set the job name
#SBATCH --job-name=bigscience_processing
### Specify the # of cpus for your job.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=64gb
#SBATCH --time=01:01:01
#SBATCH --output=%x\_%j.o
#SBATCH --error=%x\_%j.e
### pass the full environment
#SBATCH --comment=carper
# ===== END SLURM OPTIONS =====
# if you're using anaconda
# activate the conda environment
conda activate bigscience_processing
echo "loaded module"
### Go to the directory of the sample.sh file
cd $SLURM_SUBMIT_DIR
### Make a folder for job_logs if one doesn't exist
mkdir -p job_logs

### Run the cleaning file
echo "running cleaning script"

# Setup the environment
DATASET_NAME="CarperAI/the_pile_v2"
PREPROCESSINGS="dedup_document dedup_template_soft filter_remove_empty_docs filter_small_docs"

WORKING_DIR=$SLURM_SUBMIT_DIR/01a_catalogue_cleaning_and_filtering
pushd $WORKING_DIR

BASE_PATH=$WORKING_DIR/data/$DATASET_NAME
CLEAN_SAVE_PATH=$BASE_PATH/cleaned_dataset.json
CHECKS_SAVE_PATH=$BASE_PATH/checks

mkdir -p $(dirname $CLEAN_SAVE_PATH)
mkdir -p $CHECKS_SAVE_PATH

python clean.py \
    --dataset-path $DATASET_NAME \
    --dataset_config "Ubuntu IRC" \
    --preprocessings $PREPROCESSINGS \
    --save-path $CLEAN_SAVE_PATH \
    --checks-save-path $CHECKS_SAVE_PATH \
    --num-proc 96 \
    --batch-size 100 \
    --sampling-size-map-checks 1000 \
    --sampling-size-filter-checks 1000 \
    --save-to-json

echo "finished cleaning script"

### Run the filtering file
echo "running filtering script"

# Setup the environment
WORKING_DIR=$SLURM_SUBMIT_DIR/01b_oscar_cleaning_and_filtering
pushd $WORKING_DIR

FILTER_SAVE_PATH=$BASE_PATH/filtered_dataset
MODEL_PATHS=$WORKING_DIR/models

mkdir -p $FILTER_SAVE_PATH
mkdir -p $MODEL_PATHS

curl https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -o $MODEL_PATHS/lid.176.bin
python download_sentencepiece_kenlm_models.py \
    --output_dir_path $MODEL_PATHS

python main_filtering.py \
    --dataset_name "json" \
    --data_files $CLEAN_SAVE_PATH \
    --lang_dataset_id "en" \
    --path_fasttext_model $MODEL_PATHS/lid.176.bin \
    --path_sentencepiece_model $MODEL_PATHS/en.sp.model \
    --path_kenlm_model $MODEL_PATHS/en.arpa.bin \
    --path_dir_save_dataset $FILTER_SAVE_PATH \
    --num_proc 96

echo "finished filtering script"

### Run the pii removal file
echo "running pii removal script"

# Setup the environment
PII_SAVE_PATH=$BASE_PATH/pii_removed_dataset.json

WORKING_DIR=$SLURM_SUBMIT_DIR
pushd $WORKING_DIR

python 02_pii/pii_processor.py \
    --save-to-json \
    --save-check-to-json \
    --dataset-path "json" \
    --dataset-name $FILTER_SAVE_PATH/en/dataset.json \
    --save-path $PII_SAVE_PATH \
    --save-check-path $CHECKS_SAVE_PATH/pii \
    --num-proc 96 \
    --batch-size 1000 \
    --save-batch-size 10000 \
    --check-sampling-size 10000 \
    --check-only-modified

# ### move the log files inside the folder
# mv $SLURM_SUBMIT_DIR/bigscience_processing_$SLURM_JOB_ID.o $SLURM_SUBMIT_DIR/job_logs/bigscience_processing_$SLURM_JOB_ID.o
# mv $SLURM_SUBMIT_DIR/bigscience_processing_$SLURM_JOB_ID.e $SLURM_SUBMIT_DIR/job_logs/bigscience_processing_$SLURM_JOB_ID.e