#!/bin/bash
#SBATCH --job-name=bigscience_dedup
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1          # crucial - only 1 task per dist per node!
#SBATCH --cpus-per-task=40         # number of cores per tasks
#SBATCH --hint=nomultithread         # we get physical cores not logical
#SBATCH --partition=cpu128
#SBATCH --time 20:00:00              # maximum execution time (HH:MM:SS)
#SBATCH --output=logs/bigscience_dedup/%x-%j.out           # output file name
#SBATCH --comment=carper

srun --job-name=bigscience_processing --partition=cpu64 --nodes=1 --ntasks-per-node=1 --cpus-per-task=32 --pty --comment carper zsh

set -x -e
# setup the environment using the script we created before
source /fsx/$(whoami)/setup.sh

# activate the conda environment
conda activate bigscience_processing

WORKING_DIR=/fsx/home-$(whoami)/work/data-preparation/preprocessing/training/01a_catalogue_cleaning_and_filtering
pushd $WORKING_DIR

DATASET_NAME="CarperAI/the_pile_v2"

# Possible Preprocessings:
# Map functions: function(batch: Dict) -> Dict
# MAPS = {
#     "replace_newline_with_space": replace_newline_with_space,
#     "remove_lines_with_code": build_line_with_substring_remover(["{", "}", "[if", "<script"]),
#     "remove_html_spans": build_line_with_substring_remover(["<span", "</span>", "<div", "</div>", "<a", "</a>", "br>"]),
#     "remove_html_spans_sanad": build_line_with_substring_remover(["<img", "]]>", "<![CDATA", "//DW", "var ", "xtImg", "To view this video please enable JavaScript"]),
#     "remove_wiki_mojibake": build_line_with_substring_remover(["À À"]),
#     "strip_substrings_en_wiktionary": en_wiktionary_stripper,
#     ** {
#         f"remove_references_{lang}": build_reference_remover(lang) for lang in set(stopwords.keys())
#     },
#     ** {f"split_sentences_{lang}": build_sentence_splitter(lang) for lang in sentence_split_langs}
# }
# # Filter functions: function(batch: Dict) -> Dict
# FILTERS = {
#     "filter_remove_empty_docs": filter_remove_empty_docs,
#     "filter_wiki_user_titles": filter_wiki_user_titles,
#     "filter_wiki_non_text_type": filter_wiki_non_text_type,
#     "filter_small_docs": build_small_docs_filter(min_word=15),
#     ** {
#         f"filter_small_docs_bytes_{i}": build_small_docs_bytes_filter(min_bytes=i) for i in [300, 1024]
#     },
# }
# # Deduplication functions and boolean to save a sample of the modifications: function(ds: Dataset, num_proc: int, batch_size: int) -> Dataset
# DEDUPS = {
#     "dedup_template_soft": (build_dedup_template(
#         min_template_line_size=15,
#         min_template_line_occurence=10,
#     ), True),
#     "dedup_pseudocrawl_newspapers": (build_dedup_template(
#         min_template_line_size=0,
#         min_template_line_occurence=2,
#     ), True),
#     "dedup_document": (build_dedup_document(document_batch_normalizer), True),
#     "dedup_document_on_url": (build_dedup_document(url_host_and_path_batch_normalizer), True),
#     "dedup_document_on_url_lm_es_pseudocrawl-filtered_341_es_cointelegraph_com": (build_dedup_document(
#         url_lm_es_pseudocrawl_filtered_341_es_cointelegraph_com
#     ), True),
#     "dedup_document_on_url_lm_en_pseudocrawl_filtered_619_www_qut_edu_au": (build_dedup_document(
#         url_lm_en_pseudocrawl_filtered_619_www_qut_edu_au
#     ), True),
#     "concatenate_lm_fr_ester": (concatenate_lm_fr_ester, False)
# }
PREPROCESSINGS="dedup_document filter_remove_empty_docs filter_small_docs filter_wiki_non_text_type"


# # ====== RUN PYTHON SCRIPT =======

BASE_PATH=$WORKING_DIR/data/$DATASET_NAME
SAVE_PATH=$BASE_PATH/final
CHECKS_SAVE_PATH=$BASE_PATH/checks
LOGS_PATH=$BASE_PATH/logs.txt

mkdir -p $(dirname $SAVE_PATH)
mkdir -p $CHECKS_SAVE_PATH

# export HF_DATASETS_OFFLINE=1

python clean.py \
    --dataset-path $DATASET_NAME \
    --preprocessings dedup_document \
    --save-path $SAVE_PATH \
    --checks-save-path $CHECKS_SAVE_PATH \
    --num-proc 10 \
    --batch-size 100 \
    --sampling-size-map-checks 1000 \
    --sampling-size-filter-checks 1000 \
    2>&1 | tee $LOGS_PATH
