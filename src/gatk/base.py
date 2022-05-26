import os
from os.path import join, basename
from typing import List, Optional
import shutil

from src.gatk.utils import DockerRunner, Container
from loguru import logger
import gzip

from src.gatk.utils import download_to_path


class GatkRunner:
    GATK_IMAGE = "broadinstitute/gatk:4.2.6.1"

    def __init__(
            self,
            # Data save directory on host machine
            volume_hostpath: str,
            dry_run: bool = True,
            debug: bool = True
    ):
        self._mount_path = '/data'
        self._volume_hostpath = volume_hostpath.rstrip('/')
        self._dry_run = dry_run
        self._debug = debug

        self.docker_runner = DockerRunner(
            image=GatkRunner.GATK_IMAGE,
            volume_hostpath=volume_hostpath,
            volume_mountpath=self._mount_path,
        )

    def container_path(self, path: str):
        if path.startswith(self._volume_hostpath):
            return path.replace(self._volume_hostpath, self._mount_path)
        return path

    def host_path(self, path: str):
        if not path.startswith(self._volume_hostpath):
            return path.replace(self._mount_path, self._volume_hostpath)
        return path

    def _run_command_if_file_not_exists(self, output_file_save_path: str, cmd: str) -> Optional[Container]:
        if self.path_exists_on_host(output_file_save_path):
            logger.info(f'File {output_file_save_path} already exists; skipping creation.')
        else:
            logger.info(f"Running command: {cmd}")

            if self._debug:
                cmd += " --java-options '-DGATK_STACKTRACE_ON_USER_EXCEPTION=true'"

            if not self._dry_run:
                container = self.docker_runner.run(
                    cmd,
                    stream_logs=True
                )
                return container

    @staticmethod
    def _log_operation_header(header: str) -> None:
        logger.info('-'*20)
        logger.info(header)
        logger.info('-'*20)

    @staticmethod
    def _add_suffix(path: str, suffix: str):
        """ Add suffix to a filepath, of form <filename>.<suffix>.<extension> """
        ext = path.split('.')[-1]
        return path.replace(ext, f'{suffix}.{ext}')

    @staticmethod
    def gunzipped_path(path: str):
        if path.endswith('.gz'):
            out_path = path.replace(".gz", "")
            return out_path
        return path

    def check_gunzip(self, path: str) -> str:
        if path.endswith('.gz'):
            out_path = path.replace(".gz", "")

            if os.path.isfile(out_path):
                logger.info(f"Decompressed file {out_path} already exists; skipping decompression.")
                return out_path
            else:
                logger.info(f"Decompressing {path} to {out_path}")
                if not self._dry_run:
                    with gzip.open(path, 'rb') as f_in:
                        with open(out_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                return out_path
        else:
            return path

    def path_exists_on_host(self, path: str) -> bool:
        path = self.host_path(path)
        if os.path.exists(path):
            return True
        elif os.path.exists(path.replace('.gz', "")):
            # File has already been compressed
            return True
        else:
            return False

    @staticmethod
    def _replace_extension(path: str, value: str):
        ext = path.split('.')[-1]
        return path.replace(ext, f'{value.lstrip(".")}')


class GatkCommon(GatkRunner):
    def __init__(
            self,
            volume_hostpath: str,
            dry_run: bool = True

    ):
        super().__init__(volume_hostpath=volume_hostpath, dry_run=dry_run)

    def create_index_features(self, inputs: List[str]) -> List[str]:
        """ Create an index for a VCF using IndexFeatureFile """

        self._log_operation_header('Stage: Creating IndexFeatureFile for Known Sites')

        indexed_feature_files = []
        for vcf_path in inputs:
            vcf_idx_path = vcf_path + '.idx'

            _ = self._run_command_if_file_not_exists(
                output_file_save_path=vcf_idx_path,
                cmd=f'gatk IndexFeatureFile --input {vcf_path}'
            )
            indexed_feature_files.append(vcf_idx_path)
        return indexed_feature_files

    # Create index file for reference fasta genome
    def create_reference_dict(self, ref_input: str) -> str:
        """ Create a sequence dictionary for a reference using CreateSequenceDictionary"""
        self._log_operation_header('Stage: Creating Reference SequenceDictionary')

        reference_dict_path = self._replace_extension(ref_input, '.dict')

        _ = self._run_command_if_file_not_exists(
            output_file_save_path=reference_dict_path,
            cmd=f'gatk CreateSequenceDictionary --REFERENCE {ref_input}'
        )

        return reference_dict_path

    def create_reference_index(self, ref_input: str):
        """ Create a Fasta index for a reference genome using samtools faidx """

        self._log_operation_header('Stage: Creating Reference Fasta Index')

        reference_fasta_index_path = ref_input + '.fai'

        _ = self._run_command_if_file_not_exists(
            output_file_save_path=reference_fasta_index_path,
            cmd=f'samtools faidx {ref_input}'
        )

    def create_reference_bwa_mem_img(self, ref_input: str) -> str:
        """ Create a BWA Mem Index for a reference genome using BwaMemIndexImageCreator """
        self._log_operation_header('Stage: Creating Reference BWA MEM Index')

        reference_fasta_bwa_img_path = ref_input + '.img'

        _ = self._run_command_if_file_not_exists(
            output_file_save_path=reference_fasta_bwa_img_path,
            cmd=f'gatk BwaMemIndexImageCreator -I {ref_input} -O {ref_input}.img'
        )

        return reference_fasta_bwa_img_path

    def download_uris(self, download_uris: List[str], download_dir: str, decompress_gz: bool = False) -> List[str]:
        """ Download URIs to directory using wget """
        os.makedirs(download_dir, exist_ok=True)
        save_paths = []
        for uri in download_uris:
            filename = basename(uri)
            save_path = join(download_dir, filename)
            if self.path_exists_on_host(save_path) or self.path_exists_on_host(self.gunzipped_path(save_path)):
                logger.info(f"Skipping download of URI {uri} as file {save_path} already exists.")
            else:
                logger.info(f"Downloading URI {uri} to {save_path}")
                if not self._dry_run:
                    download_to_path(url=uri, path=save_path)
                    if not os.path.isfile(save_path):
                        raise RuntimeError(f"File {uri} failed to download.")

            if decompress_gz:
                save_path = self.check_gunzip(save_path)
            save_paths.append(save_path)
        return save_paths