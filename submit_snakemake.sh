#!/bin/bash
# Submit Snakemake workflow as LSF job - Optimized for Parallel Processing
# LSF job parameters for the main Snakemake controller
#BSUB -q long
#BSUB -R "rusage[mem=24G] span[hosts=1]"
#BSUB -n 2
#BSUB -W 720:00
#BSUB -J "snakemake_609-611"
#BSUB -o "logs/snakemake_1278-609-611_%J.out"
#BSUB -e "logs/snakemake_1278-609-611_%J.err"

# Create logs directory if it doesn't exist
mkdir -p logs

echo "==============================================="
echo "Starting Optimized Snakemake Mutect2 Workflow"
echo "==============================================="
echo "Job started at: $(date)"
echo "Working directory: $(pwd)"
echo "Job ID: $LSB_JOBID"

# Load conda environment for Snakemake
export PIWUDIR="/pi/michael.lodato-umw/home/waka.ujita-umw"
virtual_env_path="/pi/michael.lodato-umw/home/waka.ujita-umw/miniconda3/envs/snakemake_project12"
. $PIWUDIR/sourceconda
conda activate "$virtual_env_path"

echo "Checking Snakemake installation..."
which snakemake
if [ $? -ne 0 ]; then
    echo "Error: Snakemake not found in environment: $virtual_env_path"
    exit 1
fi

snakemake --version
echo "Snakemake found successfully!"

# Create necessary directories
mkdir -p logs envs

# Check precomputed files configuration
CONFIG_FILE="config/config.yaml"
echo "==============================================="
echo "Checking precomputed files configuration..."
echo "==============================================="

# Verify precomputed files exist
USE_PRECOMPUTED=$(grep -A5 "pre_executed:" "$CONFIG_FILE" | grep "use_pre_executed:" | awk '{print $2}')
FILE_LIST_PATH=$(grep -A5 "pre_executed:" "$CONFIG_FILE" | grep "file_list_path:" | awk '{print $2}' | tr -d '"')

echo "Use precomputed files: $USE_PRECOMPUTED"
echo "File list path: $FILE_LIST_PATH"

if [ "$USE_PRECOMPUTED" = "true" ]; then
    echo "Verifying precomputed file lists exist..."
    for file_type in getpileupsummaries calculatecontamination segments; do
        file_path="${FILE_LIST_PATH}_${file_type}_paths.txt"
        if [ -f "$file_path" ]; then
            file_count=$(wc -l < "$file_path")
            echo "  ✅ $file_type: $file_count files found in $file_path"
        else
            echo "  ❌ $file_type: File list not found: $file_path"
            echo "Error: Required precomputed file list missing"
            exit 1
        fi
    done
    echo "All precomputed file lists verified!"
else
    echo "⚠️ Warning: Precomputed files disabled - will run full contamination analysis"
fi

# Dry run to check the workflow
echo "==============================================="
echo "Performing dry run..."
echo "==============================================="
snakemake --dryrun --printshellcmds

# Check if dry run was successful
if [ $? -ne 0 ]; then
    echo "Error: Dry run failed. Please check your configuration."
    exit 1
fi

echo "Dry run completed successfully!"

# Display workflow statistics
echo "==============================================="
echo "Workflow Statistics"
echo "==============================================="
snakemake --dryrun --quiet | tail -10


echo "==============================================="
echo "Starting parallel workflow execution..."
echo "==============================================="

# Show the pair range used in this run
CONFIG_FILE="config/config.yaml"
echo "Sample pair range from $CONFIG_FILE:"
grep -A1 "sample_pair_range" "$CONFIG_FILE"

# Advanced cluster configuration for different rule types
cluster_config() {
    local rule=$1
    local wildcards=$2
    
    case $rule in
        "mutect2_per_chromosome")
            echo "bsub -q long -R 'rusage[mem=8000M] span[hosts=1]' -n 4 -W 1440:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
        "learn_read_orientation")
            echo "bsub -q long -R 'rusage[mem=12000M] span[hosts=1]' -n 4 -W 720:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
        "filter_mutect_calls")
            echo "bsub -q long -R 'rusage[mem=10000M] span[hosts=1]' -n 4 -W 480:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
        "get_pileup_summaries")
            echo "bsub -q long -R 'rusage[mem=6000M] span[hosts=1]' -n 4 -W 480:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
        "merge_vcfs"|"merge_mutect_stats"|"merge_f1r2_files")
            echo "bsub -q short -R 'rusage[mem=4000M] span[hosts=1]' -n 2 -W 180:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
        *)
            echo "bsub -q long -R 'rusage[mem={resources.mem_mb}M] span[hosts=1]' -n {threads} -W 720:00 -J ${rule}_${wildcards} -o logs/${rule}_${wildcards}_%J.out -e logs/${rule}_${wildcards}_%J.err"
            ;;
    esac
}

# Run with optimized cluster settings
snakemake \
    --snakefile snakefile \
    --cluster "bsub -q long -R 'rusage[mem={resources.mem_mb}M] span[hosts=1]' -n {threads} -W 720:00 -J {rule}_{wildcards} -o logs/{rule}_{wildcards}_%J.out -e logs/{rule}_{wildcards}_%J.err" \
    --configfile "$CONFIG_FILE" \
    --jobs 80 \
    --max-jobs-per-second 2 \
    --use-conda \
    --conda-frontend conda \
    --latency-wait 120 \
    --keep-going

# Store the exit code
workflow_exit_code=$?

echo "==============================================="
echo "Workflow Summary"
echo "==============================================="

# Check if workflow completed successfully
if [ $workflow_exit_code -eq 0 ]; then
    echo "✅ Workflow completed successfully!"
    echo "All parallel chromosome processing jobs finished successfully."
    
    # Display final output summary
    echo ""
    echo "Final Output Summary:"
    echo "--------------------"
    
    # Count output files
    output_dir=$(snakemake --dryrun --quiet 2>/dev/null | grep -o "OUTPUT_DIR.*" | head -1 | cut -d'=' -f2 | tr -d '"' || echo "output")
    if [ -d "$output_dir" ]; then
        echo "📁 Output directory: $output_dir"
        echo "📄 Unfiltered VCFs: $(find $output_dir -name "*_unfiltered.vcf.gz" 2>/dev/null | wc -l)"
        echo "🔍 Filtered VCFs: $(find $output_dir/Filtered_vcf -name "*_filtered.vcf.gz" 2>/dev/null | wc -l)"
        echo "📊 Contamination tables: $(find $output_dir -name "*_calculatecontamination.table" 2>/dev/null | wc -l)"
        echo "🧬 Read orientation models: $(find $output_dir -name "*_read-orientation-model.tar.gz" 2>/dev/null | wc -l)"
    fi
    
else
    echo "❌ Workflow completed with errors (exit code: $workflow_exit_code)"
    echo "Check individual job logs in the logs/ directory for details."
    echo ""
    echo "Common troubleshooting steps:"
    echo "1. Check resource allocation in config.yaml"
    echo "2. Verify input file paths and permissions"
    echo "3. Check GATK conda environment setup"
    echo "4. Review individual job logs for specific errors"
fi

echo ""
echo "Job finished at: $(date)"
echo "Total runtime: $(($(date +%s) - start_time)) seconds" 

# Clean up if requested
if [ $workflow_exit_code -eq 0 ] && [ "${CLEANUP_TEMP:-true}" = "true" ]; then
    echo ""
    echo "🧹 Starting cleanup of temporary files..."
    snakemake cleanup_temp_files --quiet
    echo "✅ Cleanup completed"
fi

echo "==============================================="

exit $workflow_exit_code
