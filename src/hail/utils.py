import os
import hail as hl

try:
    hc = hl.HailContext()
    hl.init()
except Exception:
    pass


def vcf_path_to_mt(vcf_path: str, extension: str = '.mt', reference_genome: str = 'GRCh37') -> hl.MatrixTable:
    vcf_dir = os.path.dirname(vcf_path)
    mt_path = os.path.join(vcf_dir, os.path.basename(vcf_path).split('.')[0] + extension)
    if not os.path.exists(mt_path):
        mt = hl.import_vcf(vcf_path, reference_genome=reference_genome)
        mt.write(mt_path)
    else:
        mt = hl.read_matrix_table(mt_path)
    return mt
