import os
import re
import glob
import sys
from pathlib import Path
from datetime import datetime

configfile: "config/config.yaml"

# Add at the beginning of snakefile (after imports)
def get_bam_index(bam_path):
    """Automatically detect index file corresponding to BAM file with priority"""
    import os
    
    # Priority: .bam.bai > .bai > .csi
    candidates = [
        (bam_path + ".bai", "bam.bai"),
        (bam_path.replace('.bam', '.bai'), "bai"),
        (bam_path.replace('.bam', '.csi'), "csi")
    ]
    
    for file_path, format_type in candidates:
        if os.path.exists(file_path):
            return file_path
    
    raise FileNotFoundError(f"No index found for BAM file: {bam_path}")

def load_precomputed_files():
    """Load path list of precomputed files"""
    precomputed_files = {}
    
    # Get directory of precomputed files from configuration
    pre_exec_config = config.get("pre_executed", {})
    
    if pre_exec_config.get("use_pre_executed", False):
        base_path = pre_exec_config.get("file_list_path", "")
        
        # Load path lists for each file type
        file_types = ["getpileupsummaries", "calculatecontamination", "segments"]
        
        for file_type in file_types:
            file_path = f"{base_path}_{file_type}_paths.txt"
            if os.path.exists(file_path):
                print(f"[LOAD] Loading {file_type} files from: {file_path}")
                with open(file_path, 'r') as f:
                    paths = [line.strip() for line in f if line.strip()]
                
                # Create dictionary with sample names as keys
                sample_dict = {}
                for path in paths:
                    filename = os.path.basename(path)
                    if file_type == "getpileupsummaries":
                        sample = filename.replace("_getpileupsummaries.table", "")
                    elif file_type == "calculatecontamination":
                        sample = filename.replace("_calculatecontamination.table", "")
                    elif file_type == "segments":
                        sample = filename.replace("_segments.table", "")
                    sample_dict[sample] = path
                
                precomputed_files[file_type] = sample_dict
                print(f"[SUCCESS] Loaded {len(sample_dict)} {file_type} files")
            else:
                print(f"[WARNING] Warning: {file_type} file list not found: {file_path}")
                precomputed_files[file_type] = {}
    
    return precomputed_files

# Check if project profile is specified and use it
if "project" in config and config["project"] in config:
    project_config = config[config["project"]]
    TARGET_DIRS = project_config["directories"]["input_dirs"]
    OUTPUT_DIR = project_config["directories"]["output_dir"]
else:
    # Use direct configuration
    TARGET_DIRS = config["directories"]["input_dirs"]
    OUTPUT_DIR = config["directories"]["output_dir"]

# Define other parameters from config
FILTER_PATTERN = config["file_patterns"]["bam_filter"]
LOG_DIR = f"{OUTPUT_DIR}/logs"

# Reference files from config
REF_GENOME = config["references"]["genome"]
GERMLINE_RESOURCE = config["references"]["germline_resource"]
PANEL_OF_NORMALS = config["references"]["panel_of_normals"]
SMALL_EXAC_COMMON_3 = config["references"]["small_exac_common_3"]

# Chromosome list for parallel processing
CHROMOSOMES = [str(i) for i in range(1, 23)] + ['X', 'Y', 'MT']

# Load precomputed files
PRECOMPUTED_FILES = load_precomputed_files()

def collect_bam_files():
    """Collect BAM files from all target directories"""
    input_files = []
    for directory in TARGET_DIRS:
        if os.path.exists(directory):
            pattern = os.path.join(directory, f"*{FILTER_PATTERN}*.bam")
            files = glob.glob(pattern)

            # DEBUG: Debug output starts here
            print(f"\n[SCAN] Scanning directory: {directory}")
            print(f"[PATTERN] Using glob pattern: {pattern}")
            if files:
                print(f"[FOUND] Matched {len(files)} BAM files:")
                for f in files:
                    print(f"  - {f}")
            else:
                print("[WARNING] No files matched in this directory.")
            # DEBUG: Debug output ends here

            input_files.extend(sorted(files))
        else:
            print(f"[WARNING] Warning: Directory '{directory}' does not exist, skipping")

    print(f"\n[TOTAL] Total BAM files found: {len(input_files)}")
    return input_files

def get_sample_name(bam_path):
    """
    Extract sample name from BAM filename, stripping .bam, .recal, and version suffixes like .b37.
    """
    name = os.path.splitext(os.path.basename(bam_path))[0]  # remove .bam
    name = re.sub(r'\.recal$', '', name)                     # remove .recal
    name = re.sub(r'\.b\d+$', '', name)                      # remove .b37, .b38, etc.
    return name

def generate_combinations_from_file(pair_file, start=1, end=None):
    bam_files = collect_bam_files()
    sample_to_bam = {get_sample_name(bam): bam for bam in bam_files}

    # DEBUG: Display sample names extracted from BAMs and corresponding paths
    print("\n[DEBUG] Sample names extracted from BAMs:")
    for sample, path in sample_to_bam.items():
        print(f"  - {sample}: {path}")
    print(f"[TOTAL] Total BAM files: {len(sample_to_bam)}\n")

    combinations = []

    try:
        with open(pair_file) as f:
            all_lines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[ERROR] Pair file not found: {pair_file}")
        sys.exit(1)

    selected_lines = all_lines[start-1:end]
    print(f"Using pair lines {start} to {end or len(all_lines)} (total: {len(selected_lines)})")

    for line in selected_lines:
        parts = line.strip().split()
        if len(parts) != 2:
            print(f"[WARNING] Invalid line (expecting 2 columns): {line}")
            continue
        tumor, normal = parts
        if tumor in sample_to_bam and normal in sample_to_bam:
            combinations.append({
                "sample1": tumor,
                "sample2": normal,
                "file1": sample_to_bam[tumor],
                "file2": sample_to_bam[normal],
                "output_prefix": f"{tumor}_{normal}"
            })
        else:
            print(f"[WARNING] Missing BAM for tumor={tumor} or normal={normal}")

    if not combinations:
        print("[ERROR] No valid sample pairs found in the specified range.")
        sys.exit(1)

    print(f"[SUCCESS] Total valid sample pairs: {len(combinations)}")
    return combinations

def get_precomputed_file(sample, file_type):
    """Get path of precomputed file"""
    if file_type in PRECOMPUTED_FILES and sample in PRECOMPUTED_FILES[file_type]:
        return PRECOMPUTED_FILES[file_type][sample]
    else:
        raise FileNotFoundError(f"Precomputed {file_type} file not found for sample: {sample}")

# Generate combinations and get unique BAM files
SAMPLE_PAIR_FILE = config["pair_list"]

pair_range = config.get("sample_pair_range", {})
start = int(pair_range.get("start", 1))
end = pair_range.get("end")
end = int(end) if end is not None else None

COMBINATIONS = generate_combinations_from_file(pair_file=SAMPLE_PAIR_FILE, start=start, end=end)
BAM_FILES = collect_bam_files()
SAMPLE_NAMES = list({combo['sample1'] for combo in COMBINATIONS} | {combo['sample2'] for combo in COMBINATIONS})

# Create output directory structure
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/Filtered_vcf", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/temp_vcf", exist_ok=True)

print("[INFO] COMBINATIONS:", COMBINATIONS)
print("[INFO] OUTPUT_DIR:", OUTPUT_DIR)
print("[INFO] Sample Names:", SAMPLE_NAMES)

# Check precomputed files availability
print("\n[DEBUG] Checking precomputed files availability:")
use_precomputed = config.get("pre_executed", {}).get("use_pre_executed", False)
if use_precomputed:
    print("[SUCCESS] Using precomputed contamination files")
    for sample in SAMPLE_NAMES:
        for file_type in ["calculatecontamination", "segments"]:
            try:
                file_path = get_precomputed_file(sample, file_type)
                print(f"  {sample} {file_type}: {file_path}")
            except FileNotFoundError as e:
                print(f"  [WARNING] {e}")
else:
    print("[WARNING] Precomputed files disabled - will run contamination analysis")

# Rule all - define final outputs (excluding precomputed files)
rule all:
    input:
        # Merged VCF outputs (unfiltered)
        expand(f"{OUTPUT_DIR}/{{combo}}_unfiltered.vcf.gz", 
               combo=[combo['output_prefix'] for combo in COMBINATIONS]),
        # Merged stats files
        expand(f"{OUTPUT_DIR}/{{combo}}_unfiltered.vcf.gz.stats",
               combo=[combo['output_prefix'] for combo in COMBINATIONS]),
        # Read orientation model outputs
        expand(f"{OUTPUT_DIR}/{{combo}}_read-orientation-model.tar.gz",
               combo=[combo['output_prefix'] for combo in COMBINATIONS]),
        # FilterMutectCalls outputs (filtered VCFs)
        expand(f"{OUTPUT_DIR}/Filtered_vcf/{{combo}}_filtered.vcf.gz",
               combo=[combo['output_prefix'] for combo in COMBINATIONS])

# Skip contamination-related rules when using precomputed files
# get_pileup_summaries, calculate_contamination rules are removed

# Rule to run Mutect2 per chromosome (parallel processing)
rule mutect2_per_chromosome:
    input:
        file1=lambda wildcards: next(combo['file1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        file2=lambda wildcards: next(combo['file2'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        file1_bai=lambda wildcards: get_bam_index(next(combo['file1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair)),
        file2_bai=lambda wildcards: get_bam_index(next(combo['file2'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair)),
        ref=REF_GENOME,
        germline=GERMLINE_RESOURCE,
        pon=PANEL_OF_NORMALS
    output:
        vcf=f"{OUTPUT_DIR}/temp_vcf/{{sample_pair}}_{{chr}}_unfiltered.vcf.gz",
        stats=f"{OUTPUT_DIR}/temp_vcf/{{sample_pair}}_{{chr}}_unfiltered.vcf.gz.stats",
        f1r2=f"{OUTPUT_DIR}/temp_vcf/{{sample_pair}}_{{chr}}_f1r2.tar.gz"
    params:
        sample1=lambda wildcards: next(combo['sample1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        sample2=lambda wildcards: next(combo['sample2'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        command_mem=lambda wildcards, resources, threads: max(int(resources.mem_mb * 0.8), 2000),
        chr="{chr}"
    log:
        f"{LOG_DIR}/mutect2_{{sample_pair}}_{{chr}}.log"
    threads: config["resources"]["mutect2"]["threads"]
    resources:
        mem_mb=config["resources"]["mutect2"]["mem_mb"],
        runtime=config["resources"]["mutect2"]["runtime"]
    conda:
        "envs/gatk.yaml"
    shell:
        """
        echo "Processing combination: {params.sample1} vs {params.sample2} for chromosome {params.chr}" > {log}
        echo "File 1: {input.file1}" >> {log}
        echo "File 2: {input.file2}" >> {log}
        echo "Output: {output.vcf}" >> {log}
        echo "Started at: $(date)" >> {log}
        
        # Create interval for the chromosome
        interval_arg=""
        if [ "{params.chr}" == "MT" ]; then
            interval_arg="-L MT"
        else
            interval_arg="-L {params.chr}"
        fi
        
        # Check if output directory exists
        mkdir -p $(dirname {output.vcf})
        
        gatk --java-options "-Xms{params.command_mem}m -Xmx{params.command_mem}m -XX:ParallelGCThreads={threads}" \
            Mutect2 \
            -R {input.ref} \
            -I {input.file1} \
            -I {input.file2} \
            -normal {params.sample2} \
            --germline-resource {input.germline} \
            --panel-of-normals {input.pon} \
            --f1r2-tar-gz {output.f1r2} \
            $interval_arg \
            --interval-padding 100 \
            -O {output.vcf} \
            2>&1 | tee -a {log}
        
        echo "Job completed successfully for {params.sample1} vs {params.sample2} chromosome {params.chr}" >> {log}
        echo "Finished at: $(date)" >> {log}
        """

# Rule to merge chromosome VCFs
rule merge_vcfs:
    input:
        vcfs=lambda wildcards: [f"{OUTPUT_DIR}/temp_vcf/{wildcards.sample_pair}_{chr}_unfiltered.vcf.gz" for chr in CHROMOSOMES]
    output:
        args=f"{OUTPUT_DIR}/{{sample_pair}}_vcf_merge.args",
        vcf=f"{OUTPUT_DIR}/{{sample_pair}}_unfiltered.vcf.gz",
        idx=f"{OUTPUT_DIR}/{{sample_pair}}_unfiltered.vcf.gz.tbi"
    log:
        f"{LOG_DIR}/merge_vcfs_{{sample_pair}}.log"
    threads: config["resources"]["merge_vcfs"]["threads"]
    resources:
        mem_mb=config["resources"]["merge_vcfs"]["mem_mb"],
        runtime=config["resources"]["merge_vcfs"]["runtime"]
    conda:
        "envs/samtools.yaml"
    shell:
        """
        echo "Merging VCFs for {wildcards.sample_pair}" > {log}
        echo "Started at: $(date)" >> {log}
        
        # Verify all input files exist
        echo "Checking input files..." >> {log}
        missing_files=0
        for vcf_file in {input.vcfs}; do
            if [ ! -e "$vcf_file" ]; then
                echo "Error: Missing file $vcf_file" >> {log}
                missing_files=$((missing_files + 1))
            else
                echo "Found: $vcf_file" >> {log}
            fi
        done
        
        if [ $missing_files -gt 0 ]; then
            echo "Error: $missing_files input files are missing" >> {log}
            exit 1
        fi
        
        # Create args file with proper chromosome order
        echo "Creating sorted file list..." >> {log}
        > {output.args}  # Clear the file first
        for chr in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y MT; do
            vcf_file="{OUTPUT_DIR}/temp_vcf/{wildcards.sample_pair}_${{chr}}_unfiltered.vcf.gz"
            if [ -e "$vcf_file" ]; then
                echo "$vcf_file" >> {output.args}
                echo "Added to merge list: $vcf_file" >> {log}
            else
                echo "Warning: File not found: $vcf_file" >> {log}
            fi
        done
        
        echo "Final files to merge:" >> {log}
        cat {output.args} >> {log}
        
        # Check tools availability
        echo "Checking tool versions..." >> {log}
        samtools --version >> {log}
        bcftools --version >> {log}
        tabix --version >> {log}
        
        # Merge VCFs using bcftools
        echo "Starting VCF concatenation with bcftools..." >> {log}
        bcftools concat \
            --file-list {output.args} \
            --output-type z \
            --output {output.vcf} \
            --threads {threads} \
            2>&1 | tee -a {log}
        
        # Check if merge was successful
        if [ $? -ne 0 ]; then
            echo "Error: bcftools concat failed" >> {log}
            exit 1
        fi
        
        # Index merged VCF
        echo "Indexing merged VCF..." >> {log}
        tabix -p vcf {output.vcf} 2>&1 | tee -a {log}
        
        if [ $? -ne 0 ]; then
            echo "Error: tabix indexing failed" >> {log}
            exit 1
        fi
        
        # Verify output files
        if [ -e "{output.vcf}" ] && [ -e "{output.idx}" ]; then
            echo "VCF merging completed successfully for {wildcards.sample_pair}" >> {log}
            echo "Output VCF size: $(ls -lh {output.vcf} | awk '{{print $5}}')" >> {log}
            echo "Number of variants: $(bcftools view -H {output.vcf} | wc -l)" >> {log}
        else
            echo "Error: Output files not created properly" >> {log}
            ls -la {OUTPUT_DIR}/{wildcards.sample_pair}_* >> {log}
            exit 1
        fi
        
        echo "Finished at: $(date)" >> {log}
        """

# Rule to merge Mutect stats
rule merge_mutect_stats:
    input:
        stats=lambda wildcards: [f"{OUTPUT_DIR}/temp_vcf/{wildcards.sample_pair}_{chr}_unfiltered.vcf.gz.stats" for chr in CHROMOSOMES]
    output:
        merged_stats=f"{OUTPUT_DIR}/{{sample_pair}}_unfiltered.vcf.gz.stats"
    params:
        stats_args=lambda wildcards: ' '.join([f"--stats {OUTPUT_DIR}/temp_vcf/{wildcards.sample_pair}_{chr}_unfiltered.vcf.gz.stats" for chr in CHROMOSOMES]),
        command_mem=lambda wildcards, resources, threads: max(int(resources.mem_mb * 0.8), 1000)
    log:
        f"{LOG_DIR}/merge_mutect_stats_{{sample_pair}}.log"
    threads: config["resources"]["merge_mutect_stats"]["threads"]
    resources:
        mem_mb=config["resources"]["merge_mutect_stats"]["mem_mb"],
        runtime=config["resources"]["merge_mutect_stats"]["runtime"]
    conda:
        "envs/gatk.yaml"
    shell:
        """
        echo "Merging Mutect stats for {wildcards.sample_pair}" > {log}
        echo "Started at: $(date)" >> {log}
        
        gatk --java-options "-Xms{params.command_mem}m -Xmx{params.command_mem}m -XX:ParallelGCThreads={threads}" \
            MergeMutectStats \
            {params.stats_args} \
            -O {output.merged_stats} \
            2>&1 | tee -a {log}
        
        echo "Stats merging completed successfully for {wildcards.sample_pair}" >> {log}
        echo "Finished at: $(date)" >> {log}
        """

# Rule to merge F1R2 files
rule merge_f1r2_files:
    input:
        f1r2_files=lambda wildcards: [f"{OUTPUT_DIR}/temp_vcf/{wildcards.sample_pair}_{chr}_f1r2.tar.gz" for chr in CHROMOSOMES]
    output:
        merged_f1r2=f"{OUTPUT_DIR}/{{sample_pair}}_f1r2.tar.gz"
    log:
        f"{LOG_DIR}/merge_f1r2_{{sample_pair}}.log"
    threads: config["resources"]["merge_f1r2_files"]["threads"]
    resources:
        mem_mb=config["resources"]["merge_f1r2_files"]["mem_mb"],
        runtime=config["resources"]["merge_f1r2_files"]["runtime"]
    shell:
        """
        echo "Merging F1R2 files for {wildcards.sample_pair}" > {log}
        echo "Started at: $(date)" >> {log}
        
        # Create temporary directory
        temp_dir=$(mktemp -d)
        
        # Extract all F1R2 files
        for f1r2_file in {input.f1r2_files}; do
            tar -xzf $f1r2_file -C $temp_dir
        done
        
        # Create merged F1R2 archive
        cd $temp_dir
        tar -czf {output.merged_f1r2} *
        
        # Clean up
        rm -rf $temp_dir
        
        echo "F1R2 merging completed successfully for {wildcards.sample_pair}" >> {log}
        echo "Finished at: $(date)" >> {log}
        """

# Rule: LearnReadOrientationModel
rule learn_read_orientation:
    input:
        f1r2=f"{OUTPUT_DIR}/{{sample_pair}}_f1r2.tar.gz"
    output:
        read_orientation_model=f"{OUTPUT_DIR}/{{sample_pair}}_read-orientation-model.tar.gz"
    log:
        f"{LOG_DIR}/learn_read_orientation_{{sample_pair}}.log"
    threads: config["resources"]["learn_read_orientation"]["threads"]
    resources:
        mem_mb=config["resources"]["learn_read_orientation"]["mem_mb"],
        runtime=config["resources"]["learn_read_orientation"]["runtime"]
    params:
        command_mem=lambda wildcards, resources, threads: max(int(resources.mem_mb * 0.8), 2000)
    conda:
        "envs/gatk.yaml"
    shell:
        """
        echo "Learning read orientation model for: {wildcards.sample_pair}" > {log}
        echo "Input F1R2 file: {input.f1r2}" >> {log}
        echo "Output model: {output.read_orientation_model}" >> {log}
        echo "Started at: $(date)" >> {log}
        
        gatk --java-options "-Xms{params.command_mem}m -Xmx{params.command_mem}m -XX:ParallelGCThreads={threads}" \
            LearnReadOrientationModel \
            -I {input.f1r2} \
            -O {output.read_orientation_model} \
            2>&1 | tee -a {log}
        
        echo "LearnReadOrientationModel completed successfully for {wildcards.sample_pair}" >> {log}
        echo "Finished at: $(date)" >> {log}
        """

# Rule: FilterMutectCalls (using precomputed files)
rule filter_mutect_calls:
    input:
        unfiltered_vcf=f"{OUTPUT_DIR}/{{sample_pair}}_unfiltered.vcf.gz",
        read_orientation_model=f"{OUTPUT_DIR}/{{sample_pair}}_read-orientation-model.tar.gz",
        ref=REF_GENOME
    output:
        filtered_vcf=f"{OUTPUT_DIR}/Filtered_vcf/{{sample_pair}}_filtered.vcf.gz"
    params:
        sample1=lambda wildcards: next(combo['sample1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        sample2=lambda wildcards: next(combo['sample2'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair),
        command_mem=lambda wildcards, resources, threads: max(int(resources.mem_mb * 0.8), 2000),
        # Get paths of precomputed files
        contamination_table=lambda wildcards: get_precomputed_file(next(combo['sample1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair), 'calculatecontamination'),
        segments_table=lambda wildcards: get_precomputed_file(next(combo['sample1'] for combo in COMBINATIONS if combo['output_prefix'] == wildcards.sample_pair), 'segments')
    log:
        f"{LOG_DIR}/filter_mutect_calls_{{sample_pair}}.log"
    threads: config["resources"]["filter_mutect_calls"]["threads"]
    resources:
        mem_mb=config["resources"]["filter_mutect_calls"]["mem_mb"],
        runtime=config["resources"]["filter_mutect_calls"]["runtime"]
    conda:
        "envs/gatk.yaml"
    shell:
        """
        # Create output directory
        mkdir -p $(dirname {output.filtered_vcf})
        
        echo "Running FilterMutectCalls for: {wildcards.sample_pair}" > {log}
        echo "Case (tumor) sample: {params.sample1}" >> {log}
        echo "Normal sample: {params.sample2}" >> {log}
        echo "UnfilteredVCF: {input.unfiltered_vcf}" >> {log}
        echo "ReadOrientationModel: {input.read_orientation_model}" >> {log}
        echo "Contamination table (precomputed): {params.contamination_table}" >> {log}
        echo "Segments table (precomputed): {params.segments_table}" >> {log}
        echo "Output filtered VCF: {output.filtered_vcf}" >> {log}
        echo "Started at: $(date)" >> {log}
        
        # Check if all required input files exist
        missing_files=()
        [ ! -e "{input.unfiltered_vcf}" ] && missing_files+=("UnfilteredVCF: {input.unfiltered_vcf}")
        [ ! -e "{input.read_orientation_model}" ] && missing_files+=("ReadOrientationModel: {input.read_orientation_model}")
        [ ! -e "{params.contamination_table}" ] && missing_files+=("contamination_table: {params.contamination_table}")
        [ ! -e "{params.segments_table}" ] && missing_files+=("segments_table: {params.segments_table}")
        
        if [ ${{#missing_files[@]}} -ne 0 ]; then
            echo "Error: The following input file(s) do not exist:" >> {log}
            for file in "${{missing_files[@]}}"; do
                echo "$file" >> {log}
            done
            echo "Skipping FilterMutectCalls for {wildcards.sample_pair}" >> {log}
            exit 1
        fi
        
        # Check if output file already exists
        if [ -e "{output.filtered_vcf}" ]; then
            echo "Output file {output.filtered_vcf} already exists. Skipping." >> {log}
            exit 0
        fi
        
        echo "Starting FilterMutectCalls with precomputed contamination files" >> {log}
        
        # Execute GATK command (using precomputed files)
        gatk --java-options "-Xms{params.command_mem}m -Xmx{params.command_mem}m -XX:ParallelGCThreads={threads}" \
            FilterMutectCalls \
            -V {input.unfiltered_vcf} \
            -R {input.ref} \
            --tumor-segmentation {params.segments_table} \
            --contamination-table {params.contamination_table} \
            --ob-priors {input.read_orientation_model} \
            -O {output.filtered_vcf} \
            >> {log} 2>&1
        
        # Check GATK exit code
        GATK_EXIT_CODE=$?
        echo "GATK exit code: $GATK_EXIT_CODE" >> {log}
        
        if [ $GATK_EXIT_CODE -ne 0 ]; then
            echo "GATK FilterMutectCalls failed" >> {log}
            exit 1
        fi
        
        # Check output file existence
        if [ ! -f "{output.filtered_vcf}" ]; then
            echo "Error: Output VCF file was not created" >> {log}
            exit 1
        fi
        
        echo "VCF file size: $(ls -lh {output.filtered_vcf})" >> {log}
        echo "FilterMutectCalls completed successfully for {wildcards.sample_pair}" >> {log}
        echo "Output saved to: {output.filtered_vcf}" >> {log}
        echo "Finished at: $(date)" >> {log}
        """

# Cleanup rule to remove temporary files
rule cleanup_temp_files:
    input:
        # Wait for all final outputs to be completed
        expand(f"{OUTPUT_DIR}/Filtered_vcf/{{combo}}_filtered.vcf.gz",
               combo=[combo['output_prefix'] for combo in COMBINATIONS])
    output:
        cleanup_flag=f"{OUTPUT_DIR}/cleanup_completed.txt"
    shell:
        """
        echo "Cleaning up temporary files..." > {output.cleanup_flag}
        echo "Started at: $(date)" >> {output.cleanup_flag}
        
        # Remove temporary VCF directory
        if [ -d "{OUTPUT_DIR}/temp_vcf" ]; then
            rm -rf {OUTPUT_DIR}/temp_vcf
            echo "Removed temp_vcf directory" >> {output.cleanup_flag}
        fi
        
        # Remove VCF merge args files
        rm -f {OUTPUT_DIR}/*_vcf_merge.args
        echo "Removed VCF merge args files" >> {output.cleanup_flag}
        
        echo "Cleanup completed at: $(date)" >> {output.cleanup_flag}
        """

# Rule to generate input files list (for reference)
rule generate_input_list:
    output:
        f"{OUTPUT_DIR}/input_files_list_{FILTER_PATTERN.replace('.', '_')}.txt"
    run:
        bam_files = collect_bam_files()
        with open(output[0], 'w') as f:
            f.write(f"Total BAM files: {len(bam_files)}\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Chromosomes processed in parallel: {', '.join(CHROMOSOMES)}\n")
            f.write("----------------------------------------\n")
            for i, bam_file in enumerate(bam_files):
                sample_name = get_sample_name(bam_file)
                f.write(f"[{i}] {bam_file} -> {sample_name}\n")
            f.write("----------------------------------------\n")
            f.write(f"Total combinations to process: {len(COMBINATIONS)}\n")
            f.write(f"Expected chromosome-specific VCFs per combination: {len(CHROMOSOMES)}\n")
            f.write(f"Total chromosome-specific jobs: {len(COMBINATIONS) * len(CHROMOSOMES)}\n")
            f.write(f"Expected merged VCFs: {len(COMBINATIONS)}\n")
            f.write(f"Expected F1R2 files: {len(COMBINATIONS)}\n")
            f.write(f"Expected read orientation models: {len(COMBINATIONS)}\n")
            f.write(f"Expected filtered VCFs: {len(COMBINATIONS)}\n")
            f.write("----------------------------------------\n")
            f.write("Precomputed contamination files:\n")
            if config.get("pre_executed", {}).get("use_pre_executed", False):
                f.write("Using precomputed contamination analysis files\n")
                for sample in SAMPLE_NAMES:
                    try:
                        cont_file = get_precomputed_file(sample, 'calculatecontamination')
                        seg_file = get_precomputed_file(sample, 'segments')
                        f.write(f"  {sample}: contamination={cont_file}, segments={seg_file}\n")
                    except FileNotFoundError:
                        f.write(f"  {sample}: Missing precomputed files\n")
            else:
                f.write("Not using precomputed files - contamination analysis will be performed\n")
