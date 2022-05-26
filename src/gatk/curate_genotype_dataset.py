import os
from os.path import join, basename
from typing import List, Tuple

from src.gatk.base import GatkCommon


class CurateGenotypeDataset(GatkCommon):
    """ Combine sample GVCFs and perform quality filtering

    - Downloading data
        - Known sites training data
    - GenomicsDBImport
        - Import single-sample GVCFs into GenomicsDB before joint genotyping.
        - See https://gatk.broadinstitute.org/hc/en-us/articles/360036883491-GenomicsDBImport
    - GenotypeGVCFs
        - Perform joint genotyping on one or more samples pre-called with HaplotypeCaller
        - See https://gatk.broadinstitute.org/hc/en-us/articles/360037057852-GenotypeGVCFs
    - VariantRecalibrator
        - Build a recalibration model to score variant quality for filtering purposes
        - See https://gatk.broadinstitute.org/hc/en-us/articles/360036510892-VariantRecalibrator
    - ApplyVQSR
        - Apply a score cutoff to filter variants based on a recalibration table
        - https://gatk.broadinstitute.org/hc/en-us/articles/360037056912-ApplyVQSR
    """
    CURATED_GVCFS_DIR = "curated_gvcfs"
    VQSR_DIR = "vqsr"
    KNOWN_SITES_DIR = "known_sites"
    ORGANISM_NAME = "human"


    def __init__(
            self,
            volume_hostpath: str,
            gvcf_paths: List[str],
            intervals: List[str],
            annotations: List[str],
            resources: List[Tuple[str, str]],
            known_sites_vqsr: List[str],
            reference_path: str,
            dry_run: bool = False,
            mode: str = "SNP"
    ):
        super().__init__(volume_hostpath=volume_hostpath, dry_run=dry_run)

        self._gvcf_paths = gvcf_paths
        self._intervals = intervals
        self._annotations = annotations
        self._reference_path = reference_path
        self._known_sites_vqsr = known_sites_vqsr
        self._resources = resources
        self._mode = mode

        self._curated_gvcf_dir = join(
            self._mount_path, CurateGenotypeDataset.CURATED_GVCFS_DIR, CurateGenotypeDataset.ORGANISM_NAME
        )
        self._filtered_variants_output_path = join(
            self._curated_gvcf_dir,
            self.get_experiment_filename("filtered_variants.g.vcf")
        )

        self._vqsr_dir = join(
            self._mount_path, CurateGenotypeDataset.VQSR_DIR, CurateGenotypeDataset.ORGANISM_NAME
        )

        self._known_sites_dir = join(self._mount_path, CurateGenotypeDataset.KNOWN_SITES_DIR, CurateGenotypeDataset.ORGANISM_NAME)

    def initialize(self):
        """ Prepare input data

        - Download input data
        - Create index features for known sites
        - Create directories
        """
        self.create_index_features(self._gvcf_paths)

        # Download known sites
        saved_filepaths = self.download_uris(
            self._known_sites_vqsr,
            self.host_path(self._known_sites_dir),
            decompress_gz=True
        )

        for d in [self._curated_gvcf_dir, self._vqsr_dir]:
            os.makedirs(self.host_path(d), exist_ok=True)

    def combine_gvcfs(self, gvcf_paths: List[str], gendb_save_path: str, intervals: List[str]):
        """Combine sample GVCF files into a Genomics DB

        See https://gatk.broadinstitute.org/hc/en-us/articles/360036883491-GenomicsDBImport for details
        :param gvcf_paths: Sample GVCF paths (obtained from the extract_sample_variants.py pipeline)
        :param gendb_save_path: Path to save Genomics DB obtained from combining sample GVCFs
        :param intervals: Intervals consider combine when creating the GenomicsDB
        :return: Path to the saved Genomics DB
        """
        command_list = ['gatk', 'GenomicsDBImport']
        command_list.extend([f'-V {gvcf_path}' for gvcf_path in gvcf_paths])
        command_list.extend([f'--genomicsdb-workspace-path {gendb_save_path}'])
        command_list.extend([f'--tmp-dir /tmp'])
        command_list.extend([f'-L {i}' for i in intervals])

        cmd_str = ' '.join(command_list)

        _ = self._run_command_if_file_not_exists(
            output_file_save_path=gendb_save_path,
            cmd=cmd_str
        )
        return gendb_save_path

    def genotype_gendb(self, reference_path: str, gendb_path: str, genotyped_gvcf_save_path: str):
        """ Call genotypes on a GenomicsDB

        See https://gatk.broadinstitute.org/hc/en-us/articles/360037057852-GenotypeGVCFs for details

        :param reference_path: Path to reference fasta file
        :param gendb_path: Path to Genomics DB
        :param genotyped_gvcf_save_path: Path to save the called genotype GVCF to
        :return: Path to the saved genotyped GVCF file
        """
        command_list = ['gatk', 'GenotypeGVCFs']
        command_list.extend([f'-R {reference_path}'])
        command_list.extend([f'-V {gendb_path}'])
        command_list.extend([f'-O {genotyped_gvcf_save_path}'])

        cmd_str = ' '.join(command_list)
        _ = self._run_command_if_file_not_exists(
            output_file_save_path=genotyped_gvcf_save_path,
            cmd=cmd_str
        )

        return genotyped_gvcf_save_path

    def recalibrate_variants(
            self,
            reference_path: str,
            genotyped_gvcf_path: str,
            resources: List[Tuple[str, str]],
            recalibration_path: str,
            tranches_path: str,
            rscript_path: str,
            annotations: List[str],
            mode: str = 'SNP',
    ):
        """ Build and apply recalibration model to score variant quality

        See https://gatk.broadinstitute.org/hc/en-us/articles/360036510892-VariantRecalibrator for details
        :param reference_path: Reference genome FASTA path
        :param genotyped_gvcf_path: Genotyped GVCF path
        :param resources: Known sites resources to use, along with metadata indicating
            whether they're training/test sets, etc. See documentation for details
        :param recalibration_path: Path to where the recalibration file should be saved
        :param tranches_path: Path to where the tranches file should be saved
        :param rscript_path: Path to where the rscript should be saved
        :param annotations: Annotations used for modelling
        :param mode: Mode to run recalibrator with
        :return: Recalibration path, tranches path and rscript path
        """

        command_list = ['gatk', 'VariantRecalibrator']
        command_list.extend([f'-R {reference_path}'])
        command_list.extend([f'-V {genotyped_gvcf_path}'])
        command_list.extend([f'--resource:{r[0].strip()} {join(self._known_sites_dir, r[1].strip())}' for r in resources])
        command_list.extend([f'-mode {mode}'])
        command_list.extend([f'-an {a}' for a in annotations])
        command_list.extend([f'--tranches-file {tranches_path}'])
        command_list.extend([f'--rscript-file {rscript_path}'])
        command_list.extend([f'-O {recalibration_path}'])

        cmd_str = ' '.join(command_list)
        _ = self._run_command_if_file_not_exists(
            output_file_save_path=tranches_path,
            cmd=cmd_str
        )

        return recalibration_path, tranches_path, rscript_path

    def filter_based_on_vqsr(
            self,
            reference_path: str,
            genotyped_gvcf_path: str,
            filtered_vcf_save_path: str,
            tranches_path: str,
            recal_path: str,
            mode: str = "SNP"
    ):
        """ Apply a score cutoff to filter variants based on a recalibration table

        See https://gatk.broadinstitute.org/hc/en-us/articles/360037056912-ApplyVQSR for details
        :param reference_path: Reference genome FASTA
        :param genotyped_gvcf_path: Genotyped GVCF path
        :param filtered_vcf_save_path: Path to save the filtered VCF output file
        :param tranches_path: Tranches file path
        :param recal_path: Recalibration file path
        :param mode: Mode to apply the filtering under
        :return: Path to filtered VCF output file
        """

        command_list = ['gatk', 'ApplyVQSR']
        command_list.extend([f'-R {reference_path}'])
        command_list.extend([f'-V {genotyped_gvcf_path}'])
        command_list.extend([f'-O {filtered_vcf_save_path}'])
        command_list.extend([f'--tranches-file {tranches_path}'])
        command_list.extend([f'--recal-file {recal_path}'])
        command_list.extend([f'-mode {mode}'])

        cmd_str = ' '.join(command_list)
        _ = self._run_command_if_file_not_exists(
            output_file_save_path=filtered_vcf_save_path,
            cmd=cmd_str
        )
        return filtered_vcf_save_path

    @property
    def _create_experiment_id(self) -> str:
        # Create a unique ID from the input GVCF paths and the annotations
        gvcf_filename_bases = [basename(i).split('.')[0] for i in self._gvcf_paths]
        return f"gvcf_sample_{'_'.join(gvcf_filename_bases)}__" \
               f"n_gvcfs_{len(gvcf_filename_bases)}__" \
               f"intervals_{'_'.join(self._intervals)}__" \
               f"annotations_{'_'.join(self._annotations)}"

    def get_experiment_filename(self, experiment_extension: str) -> str:
        if not experiment_extension.startswith('.'):
            experiment_extension = '.' + experiment_extension
        return self._create_experiment_id + experiment_extension

    def run_pipeline(self):
        """ Run the full pipeline

        - Combine GVCFs into a Genomics DB
        - Call genotype variants on the Genomics DB
        - Fit and apply a recalibration model to score variant calling
        - Filter variants based on variant quality
        """

        self.initialize()

        gendb_save_path = self.combine_gvcfs(
            self._gvcf_paths,
            gendb_save_path=join(self._curated_gvcf_dir, self.get_experiment_filename('.gendb')),
            intervals=self._intervals
        )

        genotyped_gvcf_path = self.genotype_gendb(
            reference_path=self._reference_path,
            gendb_path=f"gendb://{gendb_save_path}",
            genotyped_gvcf_save_path=join(self._curated_gvcf_dir, self.get_experiment_filename('.genotyped.g.vcf')),
        )

        recalibration_path, tranches_path, rscript_path = self.recalibrate_variants(
            reference_path=self._reference_path,
            genotyped_gvcf_path=genotyped_gvcf_path,
            resources=self._resources,
            annotations=self._annotations,
            mode=self._mode,
            recalibration_path=join(self._vqsr_dir, self.get_experiment_filename('.recal')),
            rscript_path=join(self._vqsr_dir, self.get_experiment_filename('.plots.r')),
            tranches_path=join(self._vqsr_dir, self.get_experiment_filename('.tranches'))
        )

        filtered_vcf_save_path = self.filter_based_on_vqsr(
                reference_path=self._reference_path,
                genotyped_gvcf_path=genotyped_gvcf_path,
                filtered_vcf_save_path=self._filtered_variants_output_path,
                tranches_path=tranches_path,
                recal_path=recalibration_path,
                mode="SNP"
        )

        return filtered_vcf_save_path
