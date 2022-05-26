# Reads to Analysis Ready Variants


## Overview
This repository provides both an example and a scripted procedure for taking raw genomics reads to analysis ready variants following GATK's best practices.

In particular, we follow the GATK's steps for germline cohort data as outlined in [Germline-short-variant-discovery-SNPs-Indels](https://gatk.broadinstitute.org/hc/en-us/articles/360035535932-Germline-short-variant-discovery-SNPs-Indels-).

<img src="https://github.com/lajd/reads-to-analysis-ready-variants/blob/main/src/resources/germline_corhort_data.png?raw=true" width="700" />

### Methodology

#### Obtaining data

All raw data was obtained from 2 resources:
   - [1000 Genomes Project](https://www.internationalgenome.org/) and corresponding [FTP data server](https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/)
   - [Broad Institute](https://www.broadinstitute.org/) and corresponding FTP data server (ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/b37, only accessible via FTP client)

including unmapped read files for each sample (BAM), reference genome (Fasta) and  known sites (VCF) data.

#### Executing GATK steps in docker

In order to keep our experiment reproducible, we utilize the GATK docker image available from dockerhub at `broadinstitute/gatk:4.2.6.1`.
We use the latest version, which is currently `4.2.6.1`

### Scalability

We emphasize the use of tools which promote scalability, including the use of GATK's BETA Spark features (e.g. ReadsPipelineSpark) and [Hail](https://hail.is/) for scalable downstream analytics. 
Both of these tools leverage Spark to enable large scale data processing, which allows for scaling from a local workstation to a large HPC or cloud cluster.

## Example Pipeline as Markdown

To see an example of the pipeline as markdown with bash commands executed from within the GATK docker image, please see the `pipeline_mds` directory.

## Helper classes for automating the pipeline with Python

The main contribution of this repository is to script the pipeline using Python, running each of the GATK steps in docker and automatically handling the stages of the workflow.
For example, the following python code snippet will perform the following:
1. Download all data
   1. Sample reads (i.e., a cohort of reads)
   2. Known sites
   3. Reference
2. Perform data preprocessing
   1. Filtering, sorting and indexing cohort reads
   2. Creating indices for known sites
   3. Creating an index for the reference
3. Perform ReadsPipelineSpark for each sample, obtaining analysis-ready variants for each sample of the cohort
4. Combine cohort samples into a GenomicsDB
5. Perform joint genotyping on the cohort with HaplotypeCaller
6. Fit/applies a variant recalibration model to obtain variant quality scores
7. Applies VQSR to filter low quality variants


## Python package installation

### Prerequisites

- Docker (tested 20.10.14)
- ~32 GB RAM
- ~20GB SSD space
- Linux recommended (Ubuntu 20.04 tested)

### Setup environment

Create the conda environment
```commandline
CONDA_ENV_NAME=reads_to_variants
conda create -n $CONDA_ENV_NAME python=3.7 -y
```

Activate the conda environment
```commandline
conda activate $CONDA_ENV_NAME
```

Install the python package
```commandline
pip install .
```

Install Jupyter Notebook
```commandline
conda install ipykernel                                    
```

Add the created conda environment to the ipython kernel
```commandline
ipython kernel install --user --name=$CONDA_ENV_NAME
```

(Optional) Open the example notebook
Start the Jupyter server from the root of this repository and navigate to the localhost shown.

```commandline
jupyter notebook
```

Navigate to the example notebook located at `examples/trio_variant_analysis_pipeline.ipynb`


## Python Getting Started

Below we duplicate some python code from the example notebook found `examples/trio_variant_analysis_pipeline.ipynb`.
In particular, we download raw reads from a father/mother/child trio dataset obtained from the 1000 genomes project,
and use the Python helpers to obtain analysis ready variants that can be used by Hail.

```python
from os.path import join

PHASE_3_1KG_ROOT_FTP = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data"
BROAD_INSTITUTE_B37_ROOT_FTP = "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/b37"

# Reads for father/mother/child
READS_BAM_URIS=[
    # Father
    join(PHASE_3_1KG_ROOT_FTP, "HG00731/alignment/HG00731.chrom20.ILLUMINA.bwa.PUR.low_coverage.20130422.bam"),
    # Mother
    join(PHASE_3_1KG_ROOT_FTP, "HG00732/alignment/HG00732.chrom20.ILLUMINA.bwa.PUR.low_coverage.20130422.bam"),
    # Child
    join(PHASE_3_1KG_ROOT_FTP, "HG00733/alignment/HG00733.chrom20.ILLUMINA.bwa.PUR.low_coverage.20130415.bam"),
]

# Reference genome
REF_FA_GZ_URI = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/human_g1k_v37.fa"

# Known sites for variant extraction
KNOWN_EXTRACTION_VCF_GZ_URIS = [
    join(BROAD_INSTITUTE_B37_ROOT_FTP, "dbsnp_138.b37.vcf.gz")
]

# Known sites for VQSR model
KNOWN_SITES_VQSR_URIS = [
    join(BROAD_INSTITUTE_B37_ROOT_FTP, "hapmap_3.3.b37.vcf.gz"),
    join(BROAD_INSTITUTE_B37_ROOT_FTP, "1000G_omni2.5.b37.vcf.gz"),
    join(BROAD_INSTITUTE_B37_ROOT_FTP, "1000G_phase1.snps.high_confidence.b37.vcf.gz"),
    join(BROAD_INSTITUTE_B37_ROOT_FTP, "dbsnp_138.b37.vcf.gz")
]
```
### Perform variant discovery to obtain analysis-ready variants

```python
from src.gatk.extract_genomic_variants import ExtractGenomicVariants

variant_discovery = ExtractGenomicVariants(
    volume_hostpath='/data/Genomics',
    paired_reads_file_bam_uri=READS_BAM_URIS,
    ref_file_fa_gz_uri=REF_FA_GZ_URI,
    known_sites_vcf_gz_uri=KNOWN_EXTRACTION_VCF_GZ_URIS,
    dry_run=True,
    spark_driver_memory= '5g',
    spark_executor_memory= '8g',
    java_options= '-Xmx48g',
)

# Run the pipeline to obtain the GVCF path for each sample
sample_gvcf_paths = variant_discovery.run_pipeline()
print(sample_gvcf_paths)
# ['/data/sample_gvcfs/human/HG00731.g.vcf', '/data/sample_gvcfs/human/HG00732.g.vcf', '/data/sample_gvcfs/human/HG00733.g.vcf']
```

### Curate a genotype dataset and filter low quality variants 

```python
from src.gatk.curate_genomic_dataset import CurateGenotypeDataset

curated_genotyped_dataset = CurateGenotypeDataset(
    volume_hostpath='/data/Genomics',
    intervals=['20'],
    gvcf_paths=sample_gvcf_paths,
    annotations=[
        "QD",
        "MQ",
        "MQRankSum",
        "ReadPosRankSum",
        "FS",
        "SOR"
    ],
    known_sites_vqsr=KNOWN_SITES_VQSR_URIS,
    resources=[
        ('hapmap,known=false,training=true,truth=true,prior=15.0', join(variant_discovery.known_sites_dir, 'hapmap_3.3.b37.vcf')),
        ('omni,known=false,training=true,truth=false,prior=12.0', join(variant_discovery.known_sites_dir, '1000G_omni2.5.b37.vcf')),
        ('1000G,known=false,training=true,truth=false,prior=10.0', join(variant_discovery.known_sites_dir, '1000G_phase1.snps.high_confidence.b37.vcf')),
        ('dbsnp,known=true,training=false,truth=false,prior=2.0', join(variant_discovery.known_sites_dir, 'dbsnp_138.b37.vcf')),
    ],
    reference_path=variant_discovery.raw_data_container_paths_map[ExtractGenomicVariants.REFERENCE_DIR],
    dry_run=True
)

# Combine sample GVCFs and extract filtered variants for analysis
filtered_vcf_gz_path = curated_genotyped_dataset.run_pipeline()
print(filtered_vcf_gz_path)
# /data/curated_gvcfs/human/gvcf_sample_HG00731_HG00732_HG00733__n_gvcfs_3__intervals_20__annotations_QD_MQ_MQRankSum_ReadPosRankSum_FS_SOR.filtered_variants.g.vcf
```

### Load the cohort VCF file into Hail as a MatrixTable

```python
from src.hail.utils import vcf_path_to_mt, hl

mt = vcf_path_to_mt(filtered_vcf_gz_path)
```

### Perform analysis


```python
mt.GT.show(n_rows=100)
```

<table><thead><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00731'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00732'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00733'</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">locus</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">alleles</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">locus&lt;GRCh37&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">array&lt;str&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td></tr>
</thead><tbody><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:65900</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:66370</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:67500</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;TTGGTATCTAG&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:68749</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:69094</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:69481</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;CT&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:69506</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;GACACAC&quot;,&quot;GACAC&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/2</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:72104</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;TA&quot;,&quot;T&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:74347</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:74729</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;GT&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
</tbody></table><p style="background: #fdd; padding: 0.4em;">showing top 10 rows</p>


### Filter missing rows

```python

dataset = mt.filter_rows(hl.agg.any(hl.is_missing(mt.GT)) == False)
print(f"Num NA rows dropped: {mt.count()[0] - dataset.count()[0]}")
mt.filter_rows(hl.agg.any(hl.is_missing(mt.GT)) == True).GT.show()
# Num NA rows dropped: 3
```

<table><thead><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00731'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00732'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00733'</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">locus</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">alleles</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">locus&lt;GRCh37&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">array&lt;str&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td></tr>
</thead><tbody><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:37584126</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;AAAG&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">NA</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:44386269</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;GTGTATATATATGCATATGTATGTATATATATC&quot;,&quot;G&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">NA</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:50252339</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;A&quot;,&quot;AGGAG&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1|1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">NA</td></tr>
</tbody></table>


### Find known_sites where parents are both homozygous reference

```python
parents_hom_ref = mt.filter_rows((hl.literal([True, True]) == hl.agg.collect(mt.GT.is_hom_ref())[0:2]) == True)

parents_hom_ref.GT.show()
print(f"Percentage of total: {round(parents_hom_ref.count()[0]/mt.count()[0] * 100, 2)}%")
```

<table><thead><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00731'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00732'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00733'</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">locus</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">alleles</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">locus&lt;GRCh37&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">array&lt;str&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td></tr>
</thead><tbody><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:114916</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:142262</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;G&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1|1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:142339</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:221118</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;GAAA&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:248667</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:254517</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;CAAGAA&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0|1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:259563</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:259818</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:261059</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;T&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:285148</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;CA&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
</tbody></table><p style="background: #fdd; padding: 0.4em;">showing top 10 rows</p>

 Percentage of total: 1.29%


### Find known_sites where parents are both heterozygous

```python
parents_het = mt.filter_rows((hl.literal([True, True]) == hl.agg.collect(mt.GT.is_het())[0:2]) == True)

parents_het.GT.show()
print(f"Percentage of total: {round(parents_het.count()[0]/mt.count()[0] * 100, 2)}%")
```

<table><thead><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00731'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00732'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00733'</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">locus</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">alleles</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">locus&lt;GRCh37&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">array&lt;str&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td></tr>
</thead><tbody><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:68749</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:69094</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:69506</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;GACACAC&quot;,&quot;GACAC&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/2</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:76962</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:77965</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;GT&quot;,&quot;GTT&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/2</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:82012</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:87416</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;A&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:106703</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;CT&quot;,&quot;CTTT&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/2</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:126529</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:126600</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td></tr>
</tbody></table><p style="background: #fdd; padding: 0.4em;">showing top 10 rows</p>


 Percentage of total: 17.52%


### Sites where child is homozygous variant 


```python

child_hom_var = mt.filter_rows((hl.agg.collect(mt.GT.is_hom_var())[2]==True) == True)

child_hom_var.GT.show()
print(f"Percentage of total: {round(child_hom_var.count()[0]/mt.count()[0] * 100, 2)}%")
```


<table><thead><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;"></div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00731'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00732'</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">'HG00733'</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">locus</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">alleles</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; " colspan="1"><div style="text-align: left;border-bottom: solid 2px #000; padding-bottom: 5px">GT</div></td></tr><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">locus&lt;GRCh37&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">array&lt;str&gt;</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; text-align: left;">call</td></tr>
</thead><tbody><tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:65900</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:66370</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:67500</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;TTGGTATCTAG&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:68749</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;T&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:80655</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;A&quot;,&quot;G&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:106703</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;CT&quot;,&quot;CTTT&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/2</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:114916</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;A&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/0</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:127952</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;C&quot;,&quot;T&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:128863</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;G&quot;,&quot;C&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
<tr><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">20:129063</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">[&quot;A&quot;,&quot;G&quot;]</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">0/1</td><td style="white-space: nowrap; max-width: 500px; overflow: hidden; text-overflow: ellipsis; ">1/1</td></tr>
</tbody></table><p style="background: #fdd; padding: 0.4em;">showing top 10 rows</p>

Percentage of total: 27.36%
