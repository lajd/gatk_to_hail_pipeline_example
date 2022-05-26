import os
from os.path import join, basename
from typing import List, Union
from copy import deepcopy

from loguru import logger

from src.gatk.base import GatkCommon


class ExtractGenomicVariants(GatkCommon):
    """ Performs the GATK-best-practices reads preprocessing pipeline

    - Downloading data
        - Reads
        - Reference
        - Known sites
    - Read preparation
        - Filtering unmapped reads
        - Indexing
        - Sorting
    - ReadsPipelineSpark
        - Takes unaligned or aligned reads and runs BWA (if specified), MarkDuplicates, BQSR,
          and HaplotypeCaller to generate a VCF file of variants
        - See https://gatk.broadinstitute.org/hc/en-us/articles/360036897552-ReadsPipelineSpark-BETA-
    """

    READS_DIR = "reads"
    REFERENCE_DIR = "reference"
    KNOWN_SITES_DIR = "known_sites"
    SAMPLE_GVCFS_DIR = "sample_gvcfs"
    ORGANISM_NAME = "human"

    def __init__(
            self,
            # Data save directory on host machine
            volume_hostpath: str,
            # Input files
            paired_reads_file_bam_uris: Union[str, List[str]],
            ref_file_fasta_uri: str,
            known_sites_vcf_uris: Union[str, List[str]],
            # Whether to perform a dry run, showing execution stages
            dry_run: bool = True,
            # ReadsPipelineSpark options
            spark_driver_memory: str = '6g',
            spark_executor_memory: str = '10g',
            java_options: str = '-Xmx48g',
            hmm_threads: int = 16
    ):
        """
        :param volume_hostpath: path where data is stored on the host
        :param paired_reads_file_bam_uris: Unmapped paired reads in (BAM format)
        :param ref_file_fasta_uri: URI of reference genome (FASTA format)
        :param known_sites_vcf_uris: URIs of known sites used for extracting variants in the reads data (VCF format)
        :param dry_run: Whether to show the execution stages but not run any jobs
        :param spark_driver_memory: Memory for spark driver
        :param spark_executor_memory: Memory for spark executor
        :param java_options: Java memory options
        :param hmm_threads: Threads for HMM model in ReadsPipelineSpark
        """

        super().__init__(volume_hostpath=volume_hostpath, dry_run=dry_run)
        # Files downloaded on the host
        self.reads_dir = join(volume_hostpath, ExtractGenomicVariants.READS_DIR, ExtractGenomicVariants.ORGANISM_NAME)
        self.ref_dir = join(volume_hostpath, ExtractGenomicVariants.REFERENCE_DIR, ExtractGenomicVariants.ORGANISM_NAME)
        self.known_sites_dir = join(volume_hostpath, ExtractGenomicVariants.KNOWN_SITES_DIR, ExtractGenomicVariants.ORGANISM_NAME)
        # Files created in the container
        self.output_gvcf_dir = join(self._mount_path, ExtractGenomicVariants.SAMPLE_GVCFS_DIR, ExtractGenomicVariants.ORGANISM_NAME)

        self._paired_reads_file_bam_uris: List[str] = paired_reads_file_bam_uris if isinstance(paired_reads_file_bam_uris, list) else [paired_reads_file_bam_uris]
        self._ref_file_fa_gz_uri: str = ref_file_fasta_uri
        self._known_sites_vcf_uris: List[str] = known_sites_vcf_uris if isinstance(known_sites_vcf_uris, list) else [known_sites_vcf_uris]
        self.output_gvcf_paths: List[str] = [
            join(self.output_gvcf_dir, filename) for filename in
            self._get_gvcf_output_filenames()
        ]

        self._dry_run = dry_run

        self._spark_driver_memory = spark_driver_memory
        self._spark_executor_memory = spark_executor_memory
        self._java_options = java_options
        self._hmm_threads = hmm_threads

        self._raw_data_host_paths_map = {
            ExtractGenomicVariants.READS_DIR: [],
            ExtractGenomicVariants.REFERENCE_DIR: "",
            ExtractGenomicVariants.KNOWN_SITES_DIR: [],
        }

        self.raw_data_container_paths_map = deepcopy(self._raw_data_host_paths_map)
        self._processed_container_data_paths_map = {
            ExtractGenomicVariants.READS_DIR: {"indexed": [], "paired": [], 'sorted': []},
            ExtractGenomicVariants.REFERENCE_DIR: {},
            ExtractGenomicVariants.KNOWN_SITES_DIR: {},
            ExtractGenomicVariants.SAMPLE_GVCFS_DIR: {}
        }

    def _check_create_dirs(self) -> None:
        """ Create all required directories if needed """
        for path in [self.reads_dir, self.ref_dir, self.known_sites_dir] \
                    + [os.path.dirname(p) for p in self.output_gvcf_paths]:
            os.makedirs(path, exist_ok=True)

    def _get_gvcf_output_filenames(self) -> List[str]:
        # Generate the output save path

        output_files = []
        for paired_reads_file_bam_uris in self._paired_reads_file_bam_uris:
            file_basename = basename(paired_reads_file_bam_uris).split('.')[0]
            output_filepath = f"{file_basename}.g.vcf"
            output_files.append(output_filepath)
        return output_files

    def _check_download_raw_data(self, decompress_gz: bool = True) -> None:
        """ Download required data

        If path already exists, data will not be downloaded.

        Downloaded files will be placed onto the host in the self._volume_hostpath location.

        Download data for:
            - reads
            - reference
            - known_sites
        """
        for download_dir, download_uris in [
            (self.reads_dir, self._paired_reads_file_bam_uris),
            (self.ref_dir, self._ref_file_fa_gz_uri),
            (self.known_sites_dir, self._known_sites_vcf_uris)
        ]:
            if isinstance(download_uris, str):
                download_uris = [download_uris]

            saved_filepaths = self.download_uris(
                download_uris,
                download_dir,
                decompress_gz=decompress_gz
            )
            for save_path in saved_filepaths:
                if download_dir == self.reads_dir:
                    self._raw_data_host_paths_map[ExtractGenomicVariants.READS_DIR].append(save_path)
                elif download_dir == self.ref_dir:
                    self._raw_data_host_paths_map[ExtractGenomicVariants.REFERENCE_DIR] = save_path
                elif download_dir == self.known_sites_dir:
                    self._raw_data_host_paths_map[ExtractGenomicVariants.KNOWN_SITES_DIR].append(save_path)
                else:
                    raise NotImplementedError

        # Validation
        for key, v in self._raw_data_host_paths_map.items():
            assert v, f"Failed to download all files:\n" \
                      f"File of type `{key}` could not be downloaded: {self._raw_data_host_paths_map}"

    def _create_raw_data_container_paths_map(self):
        for k, path in self._raw_data_host_paths_map.items():
            if isinstance(path, str):
                container_path = self.container_path(path)
            else:
                container_path = [self.container_path(p) for p in path]
            self.raw_data_container_paths_map[k] = container_path

    def initialize(self):
        self._check_create_dirs()
        self._check_download_raw_data()
        self._create_raw_data_container_paths_map()

    def create_reads_index(self, inputs: List[str], threads: int = 12, extension: str = '.bai') -> List[str]:
        """ Create samtools indices for input BAM files """
        self._log_operation_header('Stage: Indexing Reads BAM')

        indexed_bam_filepaths = []
        for reads_bam_file in inputs:
            indexed_bam_filepath = reads_bam_file + extension

            _ = self._run_command_if_file_not_exists(
                output_file_save_path=indexed_bam_filepath,
                cmd=f'samtools index {reads_bam_file} -@ {threads}'
            )
            indexed_bam_filepaths.append(indexed_bam_filepath)
        self._processed_container_data_paths_map['reads']['indexed'].extend(indexed_bam_filepaths)
        return indexed_bam_filepaths

    def filter_unmapped_reads(self, inputs: List[str]) -> List[str]:
        """ Filter unmapped reads from input BAM files """

        self._log_operation_header('Stage: Filtering Unmapped Reads')

        paired_filepaths = []
        for reads_bam_file in inputs:
            reads_bam_file = self.container_path(reads_bam_file)
            paired_filepath = self._add_suffix(reads_bam_file, 'paired')

            _ = self._run_command_if_file_not_exists(
                output_file_save_path=paired_filepath,
                cmd=f'samtools view -b -f 2 -F 524 {reads_bam_file} > {paired_filepath}'
            )

            self._processed_container_data_paths_map['reads']['paired'].append(paired_filepath)
            paired_filepaths.append(paired_filepath)
        return paired_filepaths

    def sort_reads(self, inputs: List[str], threads: int = 12, thread_memory_gb: int = 1) -> List[str]:
        """ Sort input read BAM files """
        self._log_operation_header('Stage: Sorting Reads')

        sorted_filepaths = []
        for reads_bam_file in inputs:
            sorted_filepath = self._add_suffix(reads_bam_file, 'sorted')

            _ = self._run_command_if_file_not_exists(
                output_file_save_path=sorted_filepath,
                cmd=f'samtools sort -m {thread_memory_gb}G -n {reads_bam_file} -o {sorted_filepath} --threads {threads}'
            )

            self._processed_container_data_paths_map['reads']['sorted'].append(sorted_filepath)
            sorted_filepaths.append(sorted_filepath)

        return sorted_filepaths

    def create_sample_gvcf(self, input_bam: str, reference_path: str, known_sites_paths: List[str], output_path: str) -> str:
        """ Perform BWA, duplicate marking, BQSR and Haplotype calling in input BAM files

        Utilizes GATK's ReadsPipelineSpark beta tool. See
        https://gatk.broadinstitute.org/hc/en-us/articles/360036897552-ReadsPipelineSpark-BETA-
        for details
        """

        self._log_operation_header('Stage: Creating sample GVCF')

        if not self._dry_run:
            self.validate_reads_pipeline_spark_args(
                input_bam_path=input_bam,
                reference_path=reference_path,
                known_sites_paths=known_sites_paths,
                output_path=output_path
            )

        command_list = ['gatk', 'ReadsPipelineSpark']
        command_list.extend([f'--input {input_bam}'])
        command_list.extend([f'--reference {reference_path}'])
        command_list.extend([f'--known-sites {p}' for p in known_sites_paths])
        command_list.extend([f'--output {output_path}'])
        command_list.extend([f'-align'])
        command_list.extend([f"--conf 'spark.driver.memory={self._spark_driver_memory}'"])
        command_list.extend([f"--conf 'spark.executor.memory={self._spark_executor_memory}'"])
        command_list.extend([f"--remove-all-duplicates true"])
        command_list.extend([f"--java-options {self._java_options}"])
        command_list.extend([f"--native-pair-hmm-threads {self._hmm_threads}"])
        command_list.extend([f"--emit-ref-confidence GVCF"])

        cmd_str = ' '.join(command_list)
        logger.info(f"Running command: {cmd_str}")

        _ = self._run_command_if_file_not_exists(
            output_file_save_path=output_path,
            cmd=cmd_str
        )

        return output_path

    def validate_reads_pipeline_spark_args(
            self,
            input_bam_path: str,
            reference_path: str,
            known_sites_paths: Union[str, List[str]],
            output_path: str
    ):
        input_bam_path = self.host_path(input_bam_path)
        known_sites_paths = [self.host_path(p) for p in known_sites_paths]
        reference_path = self.host_path(reference_path)

        # Input file is BAM/CRAM
        assert os.path.isfile(input_bam_path)
        assert sum([input_bam_path.endswith(i) for i in ['bam', 'cram']]) == 1, input_bam_path

        # Reference fasta exists
        assert os.path.isfile(reference_path), reference_path
        # Reference is non-compressed fasta
        assert sum([reference_path.endswith(i) for i in ['fasta', 'fa']]) == 1, reference_path
        # Dict file exists
        assert os.path.isfile(ExtractGenomicVariants._replace_extension(reference_path, 'dict')), reference_path
        # Fasta index file exists
        assert os.path.isfile(reference_path + '.fai'), reference_path

        # Known sites are VCF and index file exists
        for p in known_sites_paths:
            assert os.path.isfile(p)
            assert p.endswith('.vcf'), p
            assert os.path.isfile(p + '.idx'), p

        # Output path does not exist and is a VCF path
        assert not os.path.exists(output_path), output_path
        assert output_path.endswith(".vcf"), output_path

    def run_pipeline(self) -> List[str]:
        """ Run the entire pipeline

        - Downloading and preprocessing input files
        - Creating indices for reads
        - Filtering unmapped reads
        - Sorting reads
        - Creating indices for known sites VCF files
        - Creating reference dicts
        - Creating BWA mem img
        - Performing ReadsPipelineSpark to obtain analysis-ready variants
        """
        self.initialize()

        raw_read_bam_filepaths = self.raw_data_container_paths_map[ExtractGenomicVariants.READS_DIR]
        reference_filepath = self.raw_data_container_paths_map[ExtractGenomicVariants.REFERENCE_DIR]
        known_sites = self.raw_data_container_paths_map[ExtractGenomicVariants.KNOWN_SITES_DIR]

        # Index reads
        self.create_reads_index(raw_read_bam_filepaths)

        # Filter unmapped reads
        paired_only_reads_filepaths = self.filter_unmapped_reads(raw_read_bam_filepaths)

        # Sort filtered reads
        sorted_reads_filepaths = self.sort_reads(paired_only_reads_filepaths)

        # Create index features for known sites
        self.create_index_features(known_sites)

        # Create reference dict for reference
        self.create_reference_dict(reference_filepath)

        # Create index for reference
        self.create_reference_index(reference_filepath)

        # Create BWA Mem Img for reference
        self.create_reference_bwa_mem_img(reference_filepath)

        for sorted_read_bam_filepath, output_gvcf_filepath in zip(
                sorted_reads_filepaths,
                self.output_gvcf_paths
        ):
            # We create GVCFs for each sample so that we have the option of selecting which samples we would
            # like to include in our final merged GVCF
            # TODO: This can be done in parallel
            self.create_sample_gvcf(
                sorted_read_bam_filepath,
                reference_filepath,
                known_sites,
                output_gvcf_filepath
            )
        return self.output_gvcf_paths
